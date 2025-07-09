import pandas as pd
import pyarrow as pa
from s3_parquet_io import s3_upload_parquet
from configs import get_det_config


def get_counts(df, det_config, units="hours", date_=None, event_code=82, TWR_only=False):
    if date_.weekday() in [1, 2, 3] or not TWR_only:  # Tue, Wed, Thu
        df = df[df["eventcode"] == event_code]

        if units == "hours":
            df["timeperiod"] = df["timestamp"].dt.floor("H")
        elif units == "15min":
            df["timeperiod"] = df["timestamp"].dt.floor("15T")
        else:
            raise ValueError("Invalid units. Use 'hours' or '15min'.")

        df = df.groupby(["timeperiod", "signalid", "eventparam"]).size().reset_index(name="vol")
        df = df.merge(det_config, on=["signalid", "eventparam"], how="left")
        df = df[["signalid", "timeperiod", "eventparam", "CallPhase", "vol"]]
        df["signalid"] = df["signalid"].astype("category")
        df["eventparam"] = df["eventparam"].astype("category")
        return df
    else:
        return pd.DataFrame()


def get_counts2(date_, bucket, conf_athena, uptime=True, counts=True):
    print(f"-- Get Counts for: {date_} -----------")

    if uptime:
        print(f"Communications uptime {date_}")
        # Placeholder for uptime logic
        comm_uptime = pd.DataFrame()  # Replace with actual logic
        s3_upload_parquet(comm_uptime, date_, "cu", bucket, "comm_uptime", conf_athena)

    if counts:
        det_config = get_det_config(date_)
        counts_1hr = get_counts(df, det_config, "hours", date_, event_code=82, TWR_only=False)
        s3_upload_parquet(counts_1hr, date_, "counts_1hr", bucket, "counts_1hr", conf_athena)

        filtered_counts_1hr = get_filtered_counts_3stream(date_, counts_1hr, interval="1 hour")
        s3_upload_parquet(filtered_counts_1hr, date_, "filtered_counts_1hr", bucket, "filtered_counts_1hr", conf_athena)

def get_filtered_counts_3stream(date_, counts, interval="1 hour"):
    thresholds = {
        "1 hour": {"max_volume": 1200, "max_abs_delta": 200, "max_flat": 5},
        "15 min": {"max_volume": 300, "max_abs_delta": 50, "max_flat": 20}
    }
    if interval not in thresholds:
        raise ValueError("Invalid interval. Use '1 hour' or '15 min'.")

    counts["delta_vol"] = counts["vol"].diff()
    counts["flatlined"] = counts.groupby(["signalid", "CallPhase", "eventparam"])["vol"].transform(lambda x: (x == x.shift()).cumsum())
    counts["flat_flag"] = counts["flatlined"] > thresholds[interval]["max_flat"]
    counts["maxvol_flag"] = counts["vol"] > thresholds[interval]["max_volume"]
    counts["mad_flag"] = counts["delta_vol"].abs() > thresholds[interval]["max_abs_delta"]

    counts["Good_Day"] = ~(counts["flat_flag"] | counts["maxvol_flag"] | counts["mad_flag"])
    return counts

def get_adjusted_counts(df):
    df["Ph_Contr"] = df.groupby(["signalid", "CallPhase", "eventparam"])["vol"].transform(lambda x: x / x.sum())
    df["mvol"] = df.groupby(["signalid", "CallPhase", "timeperiod"])["vol"].transform("mean")
    df["vol"] = df["vol"].fillna(df["mvol"] * df["Ph_Contr"])
    return df


def prep_db_for_adjusted_counts_arrow(table, conf, date_range):
    for date_ in date_range:
        fc = pd.DataFrame()  # Replace with actual logic to read data
        fc.to_parquet(f"{table}/date={date_}.parquet")

def get_adjusted_counts_arrow(fc_table, ac_table, conf):
    fc_ds = pa.dataset(fc_table)
    for group in fc_ds.to_table().to_pandas()["group"].unique():
        ac = fc_ds.filter(pa.compute.equal(fc_ds["group"], group)).to_table().to_pandas()
        ac = get_adjusted_counts(ac)
        ac.to_parquet(f"{ac_table}/group={group}.parquet")