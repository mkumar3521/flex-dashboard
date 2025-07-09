import pandas as pd
import boto3
import pyodbc
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import json
import os
import numpy as np
import re
import yaml
from Monthly_Report_UI_Functions import RTOP1_ZONES, RTOP2_ZONES

# Load credentials
with open("Monthly_Report_AWS.yaml", "r") as file:
    cred = yaml.safe_load(file)

# Function to perform multiple inserts at once
def mydb_append_table(conn, table_name, df, chunksize=10000):
    df = df.copy()
    df = df.replace({np.nan: None})
    df = df.applymap(lambda x: str(x).replace("'", "\\'") if isinstance(x, str) else x)
    df = df.applymap(lambda x: f"'{x}'" if isinstance(x, str) else x)

    vals = df.apply(lambda row: f"({','.join(map(str, row))})", axis=1).tolist()
    vals_list = [vals[i:i + chunksize] for i in range(0, len(vals), chunksize)]

    query_prefix = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES "
    for chunk in vals_list:
        query = query_prefix + ', '.join(chunk).replace("None", "NULL")
        conn.execute(query)

# Database connection functions
def get_atspm_connection(conf_atspm):
    if os.name == "nt":  # Windows
        conn = pyodbc.connect(
            f"DSN={conf_atspm['odbc_dsn']};UID={os.getenv(conf_atspm['uid_env'])};PWD={os.getenv(conf_atspm['pwd_env'])}"
        )
    else:  # Linux
        conn = pyodbc.connect(
            f"DRIVER=FreeTDS;DSN={conf_atspm['odbc_dsn']};UID={os.getenv(conf_atspm['uid_env'])};PWD={os.getenv(conf_atspm['pwd_env'])}"
        )
    return conn

def get_maxview_connection(dsn="maxview"):
    conn = pyodbc.connect(
        f"DSN={dsn};UID={os.getenv('MAXV_USERNAME')};PWD={os.getenv('MAXV_PASSWORD')}"
    )
    return conn

def get_maxview_eventlog_connection():
    return get_maxview_connection(dsn="MaxView_EventLog")

def get_cel_connection():
    return get_maxview_eventlog_connection()

def get_aurora_connection(load_data_local_infile=False):
    engine = create_engine(
        f"mysql+pymysql://{cred['RDS_USERNAME']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:3306/{cred['RDS_DATABASE']}",
        connect_args={"local_infile": load_data_local_infile},
    )
    return engine.connect()

def get_athena_connection(conf_athena):
    engine = create_engine(
        f"awsathena+rest://@athena.{conf_athena['region']}.amazonaws.com:443/{conf_athena['database']}",
        connect_args={"s3_staging_dir": conf_athena["staging_dir"]},
    )
    return engine.connect()

# Athena partition management
def add_athena_partition(conf_athena, bucket, table_name, date_):
    try:
        conn = get_athena_connection(conf_athena)
        query = f"ALTER TABLE {conf_athena['database']}.{table_name} ADD PARTITION (date='{date_}')"
        conn.execute(query)
        print(f"Successfully created partition (date='{date_}') for {conf_athena['database']}.{table_name}")
    except Exception as e:
        print(f"Error: {str(e)} - Partition (date='{date_}') for {conf_athena['database']}.{table_name}")
    finally:
        conn.close()

# Function to query UDC hourly data
def query_udc_hourly(zone_group, month):
    conn = get_aurora_connection()  # Replace with your Aurora connection function
    query = "SELECT * FROM cor_mo_hourly_udc"
    df = pd.read_sql(query, conn)
    conn.close()

    df["Month"] = pd.to_datetime(df["Month"])
    df["month_hour"] = pd.to_datetime(df["month_hour"])
    filtered_df = df[(df["Zone"] == zone_group) & (df["Month"] <= pd.to_datetime(month))]
    return filtered_df

# Function to query health data
def query_health_data(health_metric, level, zone_group, corridor=None, month=None):
    conn = get_aurora_connection()  # Replace with your Aurora connection function

    per = "mo"
    mr_ = {"corridor": "sub", "subcorridor": "sub", "signal": "sig"}[level]
    table = f"{mr_}_{per}_{health_metric}"

    if level in ["corridor", "subcorridor"] and ("RTOP" in zone_group or zone_group == "Zone 7"):
        if zone_group == "All RTOP":
            zones = ["All RTOP", "RTOP1", "RTOP2"] + RTOP1_ZONES + RTOP2_ZONES
        elif zone_group == "RTOP1":
            zones = ["All RTOP", "RTOP1"] + RTOP1_ZONES
        elif zone_group == "RTOP2":
            zones = ["All RTOP", "RTOP2"] + RTOP2_ZONES
        elif zone_group == "Zone 7":
            zones = ["Zone 7m", "Zone 7d"]
        zones = ", ".join([f"'{zone}'" for zone in zones])
        where_clause = f"WHERE Zone_Group IN ({zones})"
    elif level in ["corridor", "subcorridor"] and corridor == "All Corridors":
        where_clause = f"WHERE Zone_Group = '{zone_group}'"
    else:
        where_clause = f"WHERE Corridor = '{corridor}'"
    where_clause += f" AND Month = '{month}'"

    query = f"SELECT * FROM {table} {where_clause}"
    try:
        df = pd.read_sql(query, conn)
        df["Month"] = pd.to_datetime(df["Month"])
    except Exception as e:
        print(e)
        df = pd.DataFrame()
    finally:
        conn.close()

    return df
# Aurora partition management
def create_aurora_partitioned_table(table_name, period_field="Timeperiod"):
    months = pd.date_range(datetime.now() - pd.DateOffset(months=10), datetime.now() + pd.DateOffset(months=1), freq="MS")
    new_partition_dates = months.strftime("%Y-%m-01")
    new_partition_names = months.strftime("p_%Y%m")
    partitions = ", ".join(
        [f"PARTITION {name} VALUES LESS THAN ('{date} 00:00:00')" for name, date in zip(new_partition_names, new_partition_dates)]
    )
    stmt = f"""
    CREATE TABLE `{table_name}_part` (
        `Zone_Group` varchar(128) DEFAULT NULL,
        `Corridor` varchar(128) DEFAULT NULL,
        `{period_field}` datetime NOT NULL,
        `var` double DEFAULT NULL,
        `ones` double DEFAULT NULL,
        `delta` double DEFAULT NULL,
        `Description` varchar(128) DEFAULT NULL,
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        PRIMARY KEY (`id`, `{period_field}`),
        UNIQUE KEY `idx_{table_name}_unique` (`{period_field}`, `Zone_Group`, `Corridor`),
        KEY `idx_{table_name}_zone_period` (`Zone_Group`, `{period_field}`),
        KEY `idx_{table_name}_corridor_period` (`Corridor`, `{period_field}`)
    )
    PARTITION BY RANGE COLUMNS (`{period_field}`) (
        {partitions},
        PARTITION future VALUES LESS THAN (MAXVALUE)
    )
    """
    conn = get_aurora_connection()
    conn.execute(stmt)
    conn.close()

def add_aurora_partition(table_name):
    mo = datetime.now() + pd.DateOffset(months=1)
    new_partition_date = mo.strftime("%Y-%m-01")
    new_partition_name = mo.strftime("p_%Y%m")
    existing_partitions = get_aurora_partitions(table_name)
    if new_partition_name not in existing_partitions:
        stmt = f"""
        ALTER TABLE {table_name} REORGANIZE PARTITION future INTO (
            PARTITION {new_partition_name} VALUES LESS THAN ('{new_partition_date} 00:00:00'),
            PARTITION future VALUES LESS THAN MAXVALUE
        )
        """
        conn = get_aurora_connection()
        conn.execute(stmt)
        conn.close()

def drop_aurora_partitions(table_name, months_to_keep=8):
    existing_partitions = get_aurora_partitions(table_name)
    drop_partition_name = (datetime.now() - pd.DateOffset(months=months_to_keep)).strftime("p_%Y%m")
    drop_partition_names = [p for p in existing_partitions if p <= drop_partition_name]
    conn = get_aurora_connection()
    for partition_name in drop_partition_names:
        stmt = f"ALTER TABLE {table_name} DROP PARTITION {partition_name};"
        conn.execute(stmt)
    conn.close()

def get_aurora_partitions(table_name):
    query = f"SELECT PARTITION_NAME FROM information_schema.partitions WHERE TABLE_NAME = '{table_name}'"
    conn = get_aurora_connection()
    partitions = pd.read_sql(query, conn)
    conn.close()
    return partitions["PARTITION_NAME"].tolist()

# Query data function
def query_data(metric, level="corridor", resolution="monthly", hourly=False, zone_group=None, corridor=None, month=None, quarter=None, upto=True):
    per = {"quarterly": "qu", "monthly": "mo", "weekly": "wk", "daily": "dy"}[resolution]
    mr_ = {"corridor": "cor", "subcorridor": "sub", "signal": "sig"}[level]
    tab = metric["hourly_table"] if hourly and "hourly_table" in metric else metric["table"]
    table = f"{mr_}_{per}_{tab}"

    where_clause = f"WHERE Zone_Group = '{zone_group}'"
    if level == "signal" and zone_group == "All":
        where_clause = "WHERE True"

    query = f"SELECT * FROM {table} {where_clause}"
    comparison = "<=" if upto else "="

    if isinstance(month, str):
        month = pd.to_datetime(month)

    if hourly and resolution == "monthly":
        query += f" AND Hour <= '{month + pd.DateOffset(months=1) - pd.DateOffset(hours=1)}'"
        if not upto:
            query += f" AND Hour >= '{month}'"
    elif resolution == "monthly":
        query += f" AND Month {comparison} '{month}'"
    elif resolution == "quarterly":
        query += f" AND Quarter {comparison} {quarter}"
    elif resolution in ["weekly", "daily"]:
        query += f" AND Date {comparison} '{month + pd.DateOffset(months=1) - pd.DateOffset(days=1)}'"

    conn = get_aurora_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    if corridor:
        df = df[df["Corridor"] == corridor]

    if "Month" in df.columns:
        df["Month"] = pd.to_datetime(df["Month"])
    if "Hour" in df.columns:
        df["Hour"] = pd.to_datetime(df["Hour"])

    return df