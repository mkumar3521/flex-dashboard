"""
Init script for Monthly Report Calculations, This will be called at the beginning of any script, for setting up
initial configurations, fetching data, and preparing the environment.
"""
import datetime
import joblib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import pyarrow.feather as feather
from dateutil.rrule import rrule, DAILY
from dateutil.parser import parse as date_parse
import pandas as pd
import utilities as util
import configs
import Monthly_Report_Functions as mrf
import database_functions as dbf
from sqlalchemy import text


def parallel_signal_fetch(date_):
    """
    Fetch signal IDs from S3 for a given date.
    """
    signal = util.get_signalids_from_s3(date_.date(), mrf.conf["bucket"])
    return signal


# ----- LOG START TIME -----
print(f"\n\n{datetime.datetime.now()} Starting Calcs Script")

# ----- PARALLEL SETUP -----
usable_cores = util.get_usable_cores()  # Assume this is a utility function
# Or hardcode if necessary: usable_cores = os.cpu_count()

# ----- DEFINE DATE RANGE FOR CALCULATIONS -----
start_date = util.get_date_from_string(mrf.conf["start_date"], s3bucket=mrf.conf["bucket"],
                                       s3prefix="mark/split_failures")
end_date = util.get_date_from_string(mrf.conf["end_date"])

# Generate month abbreviations like ["2024-04", "2024-05"]
month_abbrs = util.get_month_abbrs(start_date, end_date)

# ----- GET CORRIDORS -----
corridors = configs.get_corridors(mrf.conf["corridors_filename_s3"], filter_signals=True)

feather_filename = str(Path(mrf.conf["corridors_filename_s3"]).with_suffix(".feather"))
feather.write_feather(corridors, feather_filename)

qs_filename = Path(mrf.conf["corridors_filename_s3"]).with_suffix(".qs")
# Save the corridors object to the .qs file
joblib.dump(corridors, qs_filename, compress=('zlib', 3))

# All corridors
all_corridors = configs.get_corridors(mrf.conf["corridors_filename_s3"], filter_signals=False)

feather_filename_all = str(Path("all_" + mrf.conf["corridors_filename_s3"]).with_suffix(".feather"))
feather.write_feather(all_corridors, feather_filename_all)

qs_filename_all = Path("all_" + mrf.conf["corridors_filename_s3"]).with_suffix(".qs")
# Save the corridors object to the .qs file
joblib.dump(corridors, qs_filename, compress=('zlib', 3))

# ----- SIGNAL LIST FOR GIVEN DATE RANGE -----
# Handle potential NaT values in date parsing - Convert to string first
try:
    start_date_str = str(start_date) if start_date is not None else ""
    end_date_str = str(end_date) if end_date is not None else ""

    # Only parse if we have valid string dates
    if start_date_str and start_date_str != "nan" and start_date_str != "NaT":
        start_dt = date_parse(start_date_str)
    else:
        start_dt = datetime.datetime.now() - datetime.timedelta(days=7)

    if end_date_str and end_date_str != "nan" and end_date_str != "NaT":
        end_dt = date_parse(end_date_str)
    else:
        end_dt = datetime.datetime.now()

    date_range = list(rrule(DAILY, dtstart=start_dt, until=end_dt))
except (ValueError, TypeError) as e:
    print(f"Error parsing dates: {e}. Using default date range.")
    start_dt = datetime.datetime.now() - datetime.timedelta(days=7)
    end_dt = datetime.datetime.now()
    date_range = list(rrule(DAILY, dtstart=start_dt, until=end_dt))

# Use ThreadPoolExecutor for better macOS compatibility
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=usable_cores) as executor:
    signals_flat = list(executor.map(parallel_signal_fetch, date_range))

signals_list = list(set([signal for sublist in signals_flat for signal in sublist]))

# ----- SAVE LATEST DETECTOR CONFIG TO S3 -----
latest_config = configs.get_latest_det_config(mrf.conf)
util.s3write_using_qsave(latest_config, bucket=mrf.conf["bucket"], object_key="ATSPM_Det_Config_Good_Latest.qs")

# # ----- HANDLE ATHENA PARTITIONS -----
# athena = dbf.get_athena_connection(mrf.conf["athena"])
# #partitions_df = athena.execute(f"SHOW PARTITIONS {mrf.conf['athena']['atspm_table']}").fetchall()
# partitions_df = athena.execute(text(f"SHOW PARTITIONS {mrf.conf['athena']['atspm_table']}")).fetchall()
# existing_partitions = [row[0].split('=')[-1] for row in partitions_df]
#
# full_date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
# missing_partitions = list(set(full_date_range) - set(existing_partitions))
#
# if len(missing_partitions) > 10:
#     print(f"Adding missing partition: date={missing_partitions}")
#     athena.execute(text(f"MSCK REPAIR TABLE {mrf.conf['athena']['atspm_table']}"))
# elif missing_partitions:
#     print("Adding missing partitions:")
#     for date_ in missing_partitions:
#         dbf.add_athena_partition(mrf.conf["athena"], mrf.conf["bucket"], mrf.conf["athena"]["atspm_table"], date_)
#
# athena.close()

try:
    print("Connecting to Athena and checking partitions...")
    athena = dbf.get_athena_connection(mrf.conf["athena"])

    # Get database and table names
    database_name = mrf.conf["athena"]["database"]
    table_name = mrf.conf["athena"]["atspm_table"]
    full_table_name = f"{database_name}.{table_name}"

    print(f"Checking partitions for table: {full_table_name}")

    # Try to show partitions with error handling
    try:
        partitions_result = athena.execute(text(f"SHOW PARTITIONS {full_table_name}")).fetchall()
        print(f"Found {len(partitions_result)} existing partitions")

        # Parse existing partitions - handle different formats
        existing_partitions = []
        for row in partitions_result:
            partition_str = str(row[0]) if row else ""
            if "date=" in partition_str:
                # Extract date from partition string like "date=2024-01-15"
                date_part = partition_str.split('date=')[-1].strip()
                # Remove any additional partition info after the date
                date_part = date_part.split('/')[0].split(' ')[0]
                existing_partitions.append(date_part)

        print(f"Parsed {len(existing_partitions)} partition dates")

    except Exception as e:
        print(f"Error showing partitions: {e}")
        print("Table might not exist or have no partitions")
        existing_partitions = []

    # Generate expected date range
    try:
        # Convert dates to string first, then parse
        start_date_str = str(start_date) if start_date is not None else ""
        end_date_str = str(end_date) if end_date is not None else ""

        # Parse dates safely
        if start_date_str and start_date_str not in ["nan", "NaT", "None"]:
            start_date_dt = pd.to_datetime(start_date_str)
            start_date_parsed = start_date_dt.date() if hasattr(start_date_dt, 'date') else start_date_dt
        else:
            start_date_parsed = (datetime.datetime.now() - datetime.timedelta(days=30)).date()

        if end_date_str and end_date_str not in ["nan", "NaT", "None"]:
            end_date_dt = pd.to_datetime(end_date_str)
            end_date_parsed = end_date_dt.date() if hasattr(end_date_dt, 'date') else end_date_dt
        else:
            end_date_parsed = datetime.datetime.now().date()

        full_date_range = pd.date_range(start=start_date_parsed, end=end_date_parsed).strftime('%Y-%m-%d').tolist()
        print(f"Expected date range: {len(full_date_range)} dates from {full_date_range[0]} to {full_date_range[-1]}")

    except Exception as e:
        print(f"Error creating date range: {e}")
        # Fallback to last 30 days
        end_date_fallback = datetime.datetime.now().date()
        start_date_fallback = end_date_fallback - datetime.timedelta(days=30)
        full_date_range = pd.date_range(start=start_date_fallback, end=end_date_fallback).strftime('%Y-%m-%d').tolist()
        print(f"Using fallback date range: {len(full_date_range)} dates")

    # Find missing partitions
    missing_partitions = list(set(full_date_range) - set(existing_partitions))
    print(f"Missing partitions: {len(missing_partitions)}")

    if len(missing_partitions) > 10:
        print(f"Too many missing partitions ({len(missing_partitions)}), running MSCK REPAIR TABLE")
        try:
            athena.execute(text(f"MSCK REPAIR TABLE {full_table_name}"))
            print("MSCK REPAIR TABLE completed successfully")
        except Exception as e:
            print(f"Error running MSCK REPAIR TABLE: {e}")

    elif missing_partitions:
        print(f"Adding {len(missing_partitions)} missing partitions...")
        for i, date_ in enumerate(missing_partitions[:5]):  # Limit to first 5 to avoid timeout
            try:
                print(f"Adding partition {i + 1}/5: date={date_}")
                dbf.add_athena_partition(mrf.conf["athena"], mrf.conf["bucket"], table_name, date_)
            except Exception as e:
                print(f"Error adding partition for {date_}: {e}")

        if len(missing_partitions) > 5:
            print(f"Note: Only added first 5 partitions. {len(missing_partitions) - 5} remaining.")
    else:
        print("All required partitions exist")

    athena.close()
    print("Athena partition check completed")

except Exception as e:
    print(f"Error in Athena partition handling: {e}")
    print("Continuing without partition management...")
    try:
        if 'athena' in locals():
            athena.close()
    except:
        pass