# monthly_report_calcs_1.py

import subprocess
import shutil
from datetime import datetime, timedelta
import pandas as pd

from dateutil.relativedelta import relativedelta
from pyarrow.dataset import dataset as arrow_dataset
from concurrent.futures import ProcessPoolExecutor
import Monthly_Report_Functions as mrf
import Monthly_Report_Calcs_init as mr_init
import counts
import s3_parquet_io as s3_io
import utilities as utils
import metrics
import aggregations as agg


def run_python_script(script, args="", wait=False):
    cmd = f"~/miniconda3/bin/conda run -n sigops python {script} {args}"
    subprocess.Popen(cmd, shell=True) if not wait else subprocess.run(cmd, shell=True)


def run_uptime_tasks():
    print(f"{datetime.now()} parse cctv logs [1 of 11]")
    if mrf.conf['run'].get('cctv', True):
        run_python_script("parse_cctvlog.py")
        run_python_script("parse_cctvlog_encoders.py")

    print(f"{datetime.now()} parse rsu logs [3 of 11]")
    if mrf.conf['run'].get('rsus', False):
        run_python_script("parse_rsus.py")


def run_travel_times():
    print(f"{datetime.now()} travel times [4 of 11]")
    if mrf.conf['run'].get('travel_times', True):
        run_python_script("get_travel_times_v2.py", "mark travel_times_1hr.yaml")
        run_python_script("get_travel_times_v2.py", "mark travel_times_15min.yaml")
        run_python_script("get_travel_times_1min_v2.py", "mark")


def run_counts():
    print(f"{datetime.now()} counts [4 of 11]")
    if mrf.conf["run"].get("counts", True):
        date_range = pd.date_range(start=mr_init.start_date, end=mr_init.end_date, freq="D")

        if len(date_range) == 1:
            counts.get_counts2(date_range[0], bucket=mrf.conf["bucket"], conf_athena=mrf.conf["athena"], uptime=True,
                         counts=True)
        else:
            for date_ in date_range:
                utils.keep_trying(counts.get_counts2, 2, date_, bucket=mrf.conf["bucket"], conf_athena=mrf.conf[
                    "athena"],
                            uptime=True, counts=True)

        print("\n---------------------- Finished counts ---------------------------\n")
        print(f"{datetime.now()} monthly cu [5 of 11]")


def process_month(yyyy_mm):
    """
    Process counts and adjusted counts for a given month.
    """
    sd = pd.to_datetime(f"{yyyy_mm}-01")
    ed = min(sd + relativedelta(months=1) - timedelta(days=1), pd.to_datetime(mr_init.end_date))
    date_range = pd.date_range(start=sd, end=ed, freq="D")

    print("1-hour adjusted counts")
    counts.prep_db_for_adjusted_counts_arrow("filtered_counts_1hr", mrf.conf, date_range)
    counts.get_adjusted_counts_arrow("filtered_counts_1hr", "adjusted_counts_1hr", mrf.conf)

    fc_ds = utils.keep_trying(lambda: arrow_dataset("filtered_counts_1hr/"), 3, 60)
    ac_ds = utils.keep_trying(lambda: arrow_dataset("adjusted_counts_1hr/"), 3, 60)

    for date_ in date_range:
        try:
            ac_df = ac_ds.to_table(filter=(ac_ds.schema["Date"] == str(date_.date()))).to_pandas()
        except Exception:
            ac_df = pd.DataFrame(columns=["SignalID", "CallPhase", "Detector", "Timeperiod", "vol"])

        s3_io.s3_upload_parquet_date_split(ac_df, mrf.conf["bucket"], "adjusted_counts_1hr", "adjusted_counts_1hr",
                                      mrf.conf["athena"])
        utils.write_signal_details(date_.strftime("%Y-%m-%d"), mrf.conf, mr_init.signals_list)

    def process_day(date_):
        try:
            ac_df = ac_ds.to_table(filter=(ac_ds.schema["Date"] == str(date_.date()))).to_pandas()
        except Exception:
            return

        if not ac_df.empty:
            ac_df["Date"] = pd.to_datetime(ac_df["Timeperiod"]).dt.date
            vpd = metrics.get_vpd(ac_df)
            s3_io.s3_upload_parquet_date_split(vpd, mrf.conf["bucket"], "vpd", "vehicles_pd", mrf.conf["athena"])

            vph = agg.get_vph(ac_df, interval="1 hour")
            s3_io.s3_upload_parquet_date_split(vph, mrf.conf["bucket"], "vph", "vehicles_ph", mrf.conf["athena"])

    with ProcessPoolExecutor(max_workers=mrf.usable_cores) as executor:
        executor.map(process_day, date_range)

    shutil.rmtree("filtered_counts_1hr", ignore_errors=True)
    shutil.rmtree("adjusted_counts_1hr", ignore_errors=True)

    print("15-minute adjusted counts")
    counts.prep_db_for_adjusted_counts_arrow("filtered_counts_15min", mrf.conf, date_range)
    counts.get_adjusted_counts_arrow("filtered_counts_15min", "adjusted_counts_15min", mrf.conf)

    fc_ds = utils.keep_trying(lambda: arrow_dataset("filtered_counts_15min/"), 3, 60)
    ac_ds = utils.keep_trying(lambda: arrow_dataset("adjusted_counts_15min/"), 3, 60)

    for date_ in date_range:
        try:
            ac_df = ac_ds.to_table(filter=(ac_ds.schema["Date"] == str(date_.date()))).to_pandas()
        except Exception:
            ac_df = pd.DataFrame(columns=["SignalID", "CallPhase", "Detector", "Timeperiod", "vol"])

        s3_io.s3_upload_parquet_date_split(ac_df, mrf.conf["bucket"], "adjusted_counts_15min", "adjusted_counts_15min",
                                      mrf.conf["athena"])

        throughput = metrics.get_thruput(ac_df)
        s3_io.s3_upload_parquet_date_split(throughput, mrf.conf["bucket"], "tp", "throughput", mrf.conf["athena"])

        vp15 = agg.get_vph(ac_df, interval="15 min")
        s3_io.s3_upload_parquet_date_split(vp15, mrf.conf["bucket"], "vp15", "vehicles_15min", mrf.conf["athena"])

    shutil.rmtree("filtered_counts_15min", ignore_errors=True)
    shutil.rmtree("adjusted_counts_15min", ignore_errors=True)


if __name__ == "__main__":
    if mrf.conf["run"].get("counts_based_measures", True):
        for yyyy_mm in mrf.conf.get("month_abbrs", []):
            process_month(yyyy_mm)

    print("--- Finished counts-based measures ---")
