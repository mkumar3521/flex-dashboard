# s3_parquet_io.py

import os
import re
import time
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import s3fs
from concurrent.futures import ProcessPoolExecutor
import database_functions as dbf

fs = s3fs.S3FileSystem()

def s3_upload_parquet(df, date_, fn, bucket, table_name, conf_athena):
    df = df.copy()
    df = df.drop(columns=["Date"], errors="ignore")

    for col in ["Detector", "CallPhase", "SignalID"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    s3_path = f"s3://{bucket}/mark/{table_name}/date={date_}/{fn}.parquet"

    def write():
        table = pa.Table.from_pandas(df, preserve_index=False)
        with fs.open(s3_path, 'wb') as f:
            pq.write_table(table, f, use_deprecated_int96_timestamps=True)

    # Local import to avoid circular dependency
    import utilities as utils
    utils.keep_trying(write, n_tries=5)

    dbf.add_athena_partition(conf_athena, bucket, table_name, date_)


def s3_upload_parquet_date_split(df, prefix, bucket, table_name, conf_athena, parallel=False, usable_cores=4):
    df = df.copy()

    if "Date" not in df.columns:
        if "Timeperiod" in df.columns:
            df["Date"] = pd.to_datetime(df["Timeperiod"]).dt.date
        elif "Hour" in df.columns:
            df["Date"] = pd.to_datetime(df["Hour"]).dt.date

    unique_dates = df["Date"].unique()

    def process_date(date_):
        date_str = str(date_)
        sub_df = df[df["Date"] == date_]
        fn = f"{prefix}_{date_str}"
        s3_upload_parquet(sub_df, date_str, fn, bucket, table_name, conf_athena)
        time.sleep(1)

    if len(unique_dates) == 1:
        process_date(unique_dates[0])
    else:
        if parallel and os.name != 'nt':  # not Windows
            with ProcessPoolExecutor(max_workers=usable_cores) as executor:
                executor.map(process_date, unique_dates)
        else:
            for d in unique_dates:
                process_date(d)


def s3_read_parquet(bucket, object_key, date_=None):
    if date_ is None:
        match = re.search(r"\d{4}-\d{2}-\d{2}", object_key)
        date_ = match.group(0) if match else None

    try:
        path = f"s3://{bucket}/{object_key}"
        with fs.open(path, 'rb') as f:
            df = pq.read_table(f).to_pandas()
        df = df[df.columns.difference([col for col in df.columns if col.startswith("__")])]
        if date_:
            df["Date"] = pd.to_datetime(date_).date()
        return df
    except Exception as e:
        print(f"Failed to read parquet: {e}")
        return pd.DataFrame()


def s3_read_parquet_parallel(table_name, start_date, end_date, signals_list=None,
                             bucket=None, callback=lambda x: x,
                             parallel=False, s3root="mark", usable_cores=4):

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    date_range = pd.date_range(start=start, end=end)

    s3 = boto3.client("s3")

    def read_date(date_):
        prefix = f"{s3root}/{table_name}/date={date_.date()}"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        rows = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                df = s3_read_parquet(bucket, key, date_)
                # Local import to avoid circular dependency
                import utilities as utils
                df = utils.convert_to_utc(df)
                rows.append(callback(df))
        return pd.concat(rows) if rows else pd.DataFrame()

    if parallel and os.name != 'nt':
        with ProcessPoolExecutor(max_workers=usable_cores) as executor:
            dfs = list(executor.map(read_date, date_range))
    else:
        dfs = [read_date(d) for d in date_range]

    return pd.concat([df for df in dfs if not df.empty], ignore_index=True)
