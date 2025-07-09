import yaml
import pandas as pd
with open("Monthly_Report.yaml", "r") as file:
    config = yaml.safe_load(file)

def weighted_mean_by_corridor_(df, per_, corridors, var_, wt_=None):
    df = df.merge(corridors, how="left", on="SignalID")
    df = df[df["Corridor"].notna()]
    df["Corridor"] = pd.Categorical(df["Corridor"])

    grouped = df.groupby(["Zone_Group", "Zone", "Corridor", per_])

    if wt_ is None:
        result = grouped[var_].mean().reset_index()
        result["lag_"] = result[var_].shift()
        result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
        return result[["Zone_Group", "Zone", "Corridor", per_, var_, "delta"]]
    else:
        result = grouped.apply(lambda x: pd.Series({
            var_: (x[var_] * x[wt_]).sum() / x[wt_].sum(),
            wt_: x[wt_].sum()
        })).reset_index()
        result["lag_"] = result[var_].shift()
        result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
        return result[["Zone_Group", "Zone", "Corridor", per_, var_, wt_, "delta"]]

def group_corridor_by_(df, per_, var_, wt_, corr_grp):
    grouped = df.groupby(per_)
    result = grouped.apply(lambda x: pd.Series({
        var_: (x[var_] * x[wt_]).sum() / x[wt_].sum(),
        wt_: x[wt_].sum()
    })).reset_index()
    result["Corridor"] = corr_grp
    result["lag_"] = result[var_].shift()
    result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
    result["Zone_Group"] = corr_grp
    return result[["Zone_Group", "Corridor", per_, var_, wt_, "delta"]]

def group_corridors_(df, per_, var_, wt_, gr_=group_corridor_by_):
    zones = df.groupby("Zone")
    all_zones = [gr_(zones.get_group(zone), per_, var_, wt_, zone) for zone in zones.groups]

    all_rtop = gr_(df[df["Zone_Group"].isin(["RTOP1", "RTOP2"])], per_, var_, wt_, "All RTOP")
    all_rtop1 = gr_(df[df["Zone_Group"] == "RTOP1"], per_, var_, wt_, "RTOP1")
    all_rtop2 = gr_(df[df["Zone_Group"] == "RTOP2"], per_, var_, wt_, "RTOP2")
    all_zone7 = gr_(df[df["Zone"].isin(["Zone 7m", "Zone 7d"])], per_, var_, wt_, "Zone 7")

    return pd.concat([df[["Corridor", "Zone", per_, var_, wt_, "delta"]],
                      *all_zones, all_rtop, all_rtop1, all_rtop2, all_zone7]).drop_duplicates()

def get_hourly(df, var_, corridors):
    df = df.merge(corridors, how="left", on="SignalID")
    df = df[df["Corridor"].notna()]
    df["lag_"] = df[var_].shift()
    df["delta"] = (df[var_] - df["lag_"]) / df["lag_"]
    return df[["SignalID", "Hour", var_, "delta", "Zone_Group", "Zone", "Corridor", "Subcorridor"]]

def get_period_avg(df, var_, per_, wt_="ones"):
    if wt_ == "ones":
        df[wt_] = 1

    grouped = df.groupby(["SignalID", per_])
    result = grouped.apply(lambda x: pd.Series({
        var_: (x[var_] * x[wt_]).sum() / x[wt_].sum(),
        wt_: x[wt_].sum()
    })).reset_index()
    result["lag_"] = result[var_].shift()
    result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
    return result[["SignalID", per_, var_, wt_, "delta"]]

def get_period_sum(df, var_, per_):
    grouped = df.groupby(["SignalID", per_])
    result = grouped[var_].sum().reset_index()
    result["lag_"] = result[var_].shift()
    result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
    return result[["SignalID", per_, var_, "delta"]]

def get_daily_avg(df, var_, wt_="ones", peak_only=False):
    if wt_ == "ones":
        df[wt_] = 1

    if peak_only:
        df = df[df["Date_Hour"].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]

    grouped = df.groupby(["SignalID", "Date"])
    result = grouped.apply(lambda x: pd.Series({
        var_: (x[var_] * x[wt_]).sum() / x[wt_].sum(),
        wt_: x[wt_].sum()
    })).reset_index()
    result["lag_"] = result[var_].shift()
    result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
    return result[["SignalID", "Date", var_, wt_, "delta"]]

def get_daily_avg_cctv(df, var_="uptime", wt_="num", peak_only=False):
    grouped = df.groupby(["CameraID", "Date"])
    result = grouped.apply(lambda x: pd.Series({
        var_: (x[var_] * x[wt_]).sum() / x[wt_].sum(),
        wt_: x[wt_].sum()
    })).reset_index()
    result["lag_"] = result[var_].shift()
    result["delta"] = (result[var_] - result["lag_"]) / result["lag_"]
    return result[["CameraID", "Date", var_, wt_, "delta"]]

def get_vph(counts, interval="1 hour", mainline_only=True):
    if mainline_only:
        counts = counts[counts["CallPhase"].isin([2, 6])]  # Filter rows with CallPhase 2 or 6

    # Identify the column with datetime type and rename it to Timeperiod if necessary
    datetime_cols = [col for col in counts.columns if pd.api.types.is_datetime64_any_dtype(counts[col])]
    if "Timeperiod" not in counts.columns and datetime_cols:
        counts = counts.rename(columns={datetime_cols[0]: "Timeperiod"})

    # Group by SignalID, Week, DOW, and Timeperiod, and calculate the sum of vol
    counts["Week"] = counts["Timeperiod"].dt.isocalendar().week
    counts["DOW"] = counts["Timeperiod"].dt.dayofweek + 1  # Day of week (1 = Monday, ..., 7 = Sunday)
    grouped = counts.groupby(["SignalID", "Week", "DOW", "Timeperiod"], as_index=False)["vol"].sum()
    grouped = grouped.rename(columns={"vol": "vph"})

    # Rename Timeperiod to Hour if interval is "1 hour"
    if interval == "1 hour":
        grouped = grouped.rename(columns={"Timeperiod": "Hour"})

    return grouped

