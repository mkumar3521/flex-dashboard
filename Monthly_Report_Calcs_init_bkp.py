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


def parallel_signal_fetch(date_):
    """
    Fetch signal IDs from S3 for a given date.
    """
    return util.get_signalids_from_s3(date_.date(), mrf.conf["bucket"])

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
corridors = configs.get_corridors("/Users/achyuthpothuganti/Downloads/flex_v2/Corridors_v5_Latest.xlsx", filter_signals=True)

feather_filename = str(Path("/Users/achyuthpothuganti/Downloads/flex_v2/Corridors_v5_Latest.xlsx").with_suffix(".feather"))
qs_filename = Path("/Users/achyuthpothuganti/Downloads/flex_v2/Corridors_v5_Latest.xlsx").with_suffix(".qs")
# Save the corridors object to the .qs file
joblib.dump(corridors, qs_filename, compress=('zlib', 3))

# All corridors
all_corridors = configs.get_corridors("/Users/achyuthpothuganti/Downloads/flex_v2/Corridors_v5_Latest.xlsx", filter_signals=False)

feather_filename_all = str(Path("all_" + mrf.conf["corridors_filename_s3"]).with_suffix(".feather"))
feather.write_feather(all_corridors, feather_filename_all)

qs_filename_all = Path("all_" + mrf.conf["corridors_filename_s3"]).with_suffix(".qs")
# Save the corridors object to the .qs file
joblib.dump(corridors, qs_filename, compress=('zlib', 3))

# ----- SIGNAL LIST FOR GIVEN DATE RANGE -----
date_range = list(rrule(DAILY, dtstart=start_date, until=date_parse(end_date)))


with ProcessPoolExecutor(max_workers=usable_cores) as executor:
    signals_flat = list(executor.map(parallel_signal_fetch, date_range))

signals_list = list(set([signal for sublist in signals_flat for signal in sublist]))

# ----- SAVE LATEST DETECTOR CONFIG TO S3 -----
latest_config = configs.get_latest_det_config(mrf.conf)
util.s3write_using_qsave(latest_config, bucket=mrf.conf["bucket"], object_key="ATSPM_Det_Config_Good_Latest.qs")

# ----- HANDLE ATHENA PARTITIONS -----
athena = dbf.get_athena_connection(mrf.conf["athena"])
partitions_df = athena.execute(f"SHOW PARTITIONS {mrf.conf['athena']['atspm_table']}").fetchall()
existing_partitions = [row[0].split('=')[-1] for row in partitions_df]

full_date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
missing_partitions = list(set(full_date_range) - set(existing_partitions))

if len(missing_partitions) > 10:
    print(f"Adding missing partition: date={missing_partitions}")
    athena.execute(f"MSCK REPAIR TABLE {mrf.conf['athena']['atspm_table']}")
elif missing_partitions:
    print("Adding missing partitions:")
    for date_ in missing_partitions:
        dbf.add_athena_partition(mrf.conf["athena"], mrf.conf["bucket"], mrf.conf["athena"]["atspm_table"], date_)

athena.close()
