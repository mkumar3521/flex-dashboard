import os
import re
import yaml
import sys
import boto3
import s3fs
import psutil
import pandas as pd
from botocore.exceptions import NoCredentialsError
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from functools import reduce
import datetime
from pathlib import Path
import joblib
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

fs = s3fs.S3FileSystem()


def get_cor(bucket, object_key):
    """Reads a .qs file from S3 bucket."""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=object_key)
        return pd.read_qs(response['Body'])
    except NoCredentialsError:
        logging.error("Credentials not available.")
        return None


def sizeof(obj):
    """Returns the size of an object in MB."""
    return f"{obj.memory_usage(deep=True).sum() / (1024 ** 2):.2f} MB"


def apply_style(filename):
    """Applies style to a file (placeholder for Python equivalent)."""
    # Python equivalent for styling files can be implemented using linters like `black` or `autopep8`.
    os.system(f"black {filename}")


def get_most_recent_monday(date_):
    """Gets the most recent Monday from a given date."""
    date_ = pd.to_datetime(date_)
    return date_ - pd.Timedelta(days=date_.weekday())


def get_date_from_string(x, s3bucket=None, s3prefix=None, table_include_regex_pattern="_dy_",
                         table_exclude_regex_pattern="_outstand|_report|_resolv|_task|_tpri|_tsou|_tsub|_ttyp|_kabco|_maint|_ops|_safety|_alert|_udc|_summ",
                         exceptions=5):
    """Gets a date from a string or determines the first missing date."""
    if x == "yesterday":
        return (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    elif re.search(r"\d+(?= days ago)", x):
        days_ago = int(re.search(r"\d+(?= days ago)", x).group())
        return (datetime.date.today() - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d")
    elif x == "first_missing":
        if s3bucket and s3prefix:
            # Fetch objects from S3 bucket
            s3 = boto3.client('s3')
            objects = s3.list_objects_v2(Bucket=s3bucket, Prefix=s3prefix)
            all_dates = [re.search(r"(?<=date=)(\d+-\d+-\d+)", obj['Key']).group() for obj in objects.get('Contents', [])]
            first_missing = pd.to_datetime(max(all_dates)) + pd.Timedelta(days=1)
            return min(first_missing, pd.to_datetime(datetime.date.today() - datetime.timedelta(days=1)))
        else:
            # Placeholder for database logic
            logging.info("Start date determined from database.")
            # Implement database logic here
            return None
    else:
        return pd.to_datetime(x) + pd.Timedelta(days=1)


def get_signalids_from_s3(date_, s3bucket, s3prefix="atspm"):
    """Gets signal IDs from S3 bucket."""
    if isinstance(date_, datetime.date):
        date_ = date_.strftime("%Y-%m-%d")
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=s3bucket, Prefix=f"{s3prefix}/date={date_}")
    keys = [obj['Key'] for obj in objects.get('Contents', [])]
    signalids = sorted([int(re.search(rf"(?<={s3prefix}_)\d+", key).group()) for key in keys if re.search(rf"(?<={s3prefix}_)\d+", key)])
    return signalids


def get_last_modified_s3(bucket, object_key):
    """Gets the last modified date of an S3 object."""
    s3 = boto3.client('s3')
    try:
        response = s3.head_object(Bucket=bucket, Key=object_key)
        return response['LastModified']
    except NoCredentialsError:
        logging.error("Credentials not available.")
        return None

# TODO
def get_usable_cores(GB=8):
    """Gets the number of usable cores based on available memory."""
    if os.name == 'nt':  # Windows

        mem = psutil.virtual_memory().total
        cores = max(int(mem / (GB * 1e9)), 1)
        return min(cores, os.cpu_count() - 1)
    elif os.name == 'posix':  # Linux
        with open('/proc/meminfo') as f:
            meminfo = f.read()
        mem = int(re.search(r"MemAvailable:\s+(\d+)", meminfo).group(1)) * 1024
        cores = max(int(mem / (GB * 1e9)), 1)
        return min(cores, os.cpu_count() - 1)
    else:
        raise OSError("Unknown operating system.")


def convert_to_utc(df):
    """
    Converts all datetime columns in a DataFrame to UTC timezone.
    """
    # Identify datetime columns
    datetime_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

    # Convert each datetime column to UTC
    for col in datetime_columns:
        df[col] = df[col].dt.tz_localize('UTC') if df[col].dt.tz is None else df[col].dt.tz_convert('UTC')

    return df




def keep_trying(func, n_tries, *args, sleep=1, timeout=None, **kwargs):
    """
    Retry a function call up to `n_tries` times with exponential backoff and optional timeout.

    Parameters:
        func: Callable to retry.
        n_tries: Maximum number of attempts.
        *args, **kwargs: Arguments to pass to the function.
        sleep: Initial sleep time between retries (doubles each retry).
        timeout: Optional timeout (in seconds) for each function execution.
    Returns:
        The function's return value if successful, or None if all attempts fail.
    """
    attempt = 1
    result = None

    while attempt <= n_tries:
        try:
            if timeout is not None:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, *args, **kwargs)
                    result = future.result(timeout=timeout)
            else:
                result = func(*args, **kwargs)

            if attempt > 1:
                print(f"[keep_trying] Attempt {attempt} succeeded.")
            return result

        except Exception as e:
            print(f"[keep_trying] Attempt {attempt} failed: {str(e).strip()}")
            # Uncomment to print traceback:
            # traceback.print_exc()

        attempt += 1
        time.sleep(sleep)
        sleep *= 2

    return None


def write_signal_details(plot_date, conf, signals_list=None):
    print(f"Writing signal details for {plot_date}")
    try:
        # Local import to avoid circular dependency
        from s3_parquet_io import s3_read_parquet
        
        # Read raw counts
        rc = s3_read_parquet(
            bucket=conf["bucket"],
            object_key=f"mark/counts_1hr/date={plot_date}/counts_1hr_{plot_date}.parquet",
            date_=plot_date
        )
        if rc.empty:
            return

        rc = convert_to_utc(rc)[["SignalID", "Date", "Timeperiod", "Detector", "CallPhase", "vol"]]

        # Read filtered counts
        fc = s3_read_parquet(
            bucket=conf["bucket"],
            object_key=f"mark/filtered_counts_1hr/date={plot_date}/filtered_counts_1hr_{plot_date}.parquet",
            date_=plot_date
        )
        if fc.empty:
            return

        fc = convert_to_utc(fc)[["SignalID", "Date", "Timeperiod", "Detector", "CallPhase", "Good_Day"]]

        # Read adjusted counts
        ac = s3_read_parquet(
            bucket=conf["bucket"],
            object_key=f"mark/adjusted_counts_1hr/date={plot_date}/adjusted_counts_1hr_{plot_date}.parquet",
            date_=plot_date
        )
        if ac.empty:
            return

        ac = convert_to_utc(ac)[["SignalID", "Date", "Timeperiod", "Detector", "CallPhase", "vol"]]

        # Filter if signals list is provided
        if signals_list is not None:
            rc = rc[rc["SignalID"].astype(str).isin(signals_list)]
            fc = fc[fc["SignalID"].astype(str).isin(signals_list)]
            ac = ac[ac["SignalID"].astype(str).isin(signals_list)]

        # Merge datasets
        rc = rc.rename(columns={"vol": "vol_rc"})
        ac = ac.rename(columns={"vol": "vol_ac"})

        df = reduce(lambda left, right: pd.merge(
            left, right, on=["SignalID", "Date", "Timeperiod", "Detector", "CallPhase"], how='outer'),
            [rc, fc, ac])

        df["bad_day"] = df["Good_Day"].apply(lambda x: x == 0)
        df["SignalID"] = df["SignalID"].astype("Int64")
        df["Detector"] = df["Detector"].astype("Int64")
        df["CallPhase"] = df["CallPhase"].astype("Int64")
        df["vol_rc"] = df["vol_rc"].astype("Int64")
        df["vol_ac"] = df.apply(lambda row: row["vol_ac"] if row["bad_day"] else pd.NA, axis=1).astype("Int64")

        df = df.sort_values(by=["SignalID", "Detector", "Timeperiod"])

        # Add hour, nest by SignalID
        df["Hour"] = pd.to_datetime(df["Timeperiod"]).dt.hour
        df = df.drop(columns=["Timeperiod"])

        grouped = df.groupby("SignalID")
        nested_df = pd.DataFrame({
            "SignalID": grouped.groups.keys(),
            "data": [group.drop(columns=["SignalID"]).reset_index(drop=True) for _, group in grouped]
        })

        # Save to S3
        table_name = "signal_details"
        prefix = "sg"
        fn = f"{prefix}_{plot_date}"
        s3_path = f"s3://{conf['bucket']}/mark/{table_name}/date={plot_date}/{fn}.parquet"

        def write_parquet():
            table = pa.Table.from_pandas(nested_df, preserve_index=False)
            with fs.open(s3_path, 'wb') as f:
                pq.write_table(table, f, use_deprecated_int96_timestamps=True)

        keep_trying(write_parquet, n_tries=5)

    except Exception as e:
        print(f"Can't write signal details for {plot_date}: {e}")


def cleanup_cycle_data(date_):
    """Removes local cycles and detections files for a given date."""
    print(f"Removing local cycles and detections files for {date_}")
    os.system(f"rm -r -f ../cycles/date={date_}")
    os.system(f"rm -r -f ../detections/date={date_}")


def get_signals_chunks(df, rows=1e6):
    """Splits a dataframe into chunks of size 'rows'."""
    signals_list = df['SignalID'].drop_duplicates().sort_values().tolist()
    records = len(df)
    chunk_length = round(rows / (records / len(signals_list)))
    return [signals_list[i:i + chunk_length] for i in range(0, len(signals_list), chunk_length)]


def get_signals_chunks_arrow(df, rows=1e6):
    """Splits an Arrow dataframe into chunks of size 'rows'."""
    signals_list = df['SignalID'].unique().tolist()
    records = len(df)
    chunk_length = round(rows / (records / len(signals_list)))
    return [signals_list[i:i + chunk_length] for i in range(0, len(signals_list), chunk_length)]



def show_largest_objects(n=20):
    """Displays the largest objects in memory."""
    objects = {name: sys.getsizeof(obj) for name, obj in globals().items()}
    largest_objects = sorted(objects.items(), key=lambda x: x[1], reverse=True)[:n]
    for name, size in largest_objects:
        print(f"{name}: {size / (1024 ** 2):.2f} MB")

# Write a function to read configuration from a YAML file Monthly_Report.yaml
def read_config(file_path="Monthly_Report.yaml"):
    """Reads configuration from a YAML file."""

    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def s3write_using_qsave(data, bucket, object_key):
    # Save the data locally as a .qs file
    local_filename = Path(object_key).name
    joblib.dump(data, local_filename, compress=('zlib', 3))

    # Upload the file to S3
    s3 = boto3.client('s3')
    s3.upload_file(Filename=local_filename, Bucket=bucket, Key=object_key)


def get_month_abbrs(start_date, end_date):
    """
    Generates a list of month abbreviations in the format YYYY-MM
    """
    # Convert input strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Set the day of the start_date to the first day of the month
    start_date = start_date.replace(day=1)

    # Generate a sequence of months and format them as YYYY-MM
    return [d.strftime("%Y-%m") for d in pd.date_range(start=start_date, end=end_date, freq="MS")]
