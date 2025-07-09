import pandas as pd
from datetime import datetime, timedelta
import boto3
import pyarrow.feather as feather


def get_corridors(corr_fn, filter_signals=True, mark_only=False):
    cols = {
        "SignalID": "float64",
        "Zone_Group": "string",
        "Zone": "string",
        "Corridor": "string",
        "Subcorridor": "string",
        "Agency": "string",
        "Main Street Name": "string",
        "Side Street Name": "string",
        "Milepost": "float64",
        "Asof": "datetime64[ns]",
        "Duplicate": "float64",
        "Include": "bool",
        "Modified": "datetime64[ns]",
        "Note": "string",
        "Latitude": "float64",
        "Longitude": "float64",
        "County": "string",
        "City": "string"
    }

    df = pd.read_excel(corr_fn, dtype=cols)
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    if filter_signals:
        df = df[(df["SignalID"] > 0) & (df["Include"] == True)]

    if mark_only:
        df = df[(df["Zone_Group"] == df["Zone"]) | (df["Zone_Group"].isin(["RTOP1", "RTOP2"]))]

    df["Modified"] = df["Modified"].fillna(pd.Timestamp("1900-01-01"))
    df = df.sort_values("Modified").drop_duplicates(subset=["SignalID", "Zone", "Corridor"], keep="last")
    df = df.dropna(subset=["Corridor"])

    df["Name"] = df["Main Street Name"] + " @ " + df["Side Street Name"]
    df["Description"] = df["SignalID"].astype(str) + ": " + df["Name"]

    return df[[
        "SignalID", "Zone", "Zone_Group", "Corridor", "Subcorridor", "Milepost",
        "Agency", "Name", "Asof", "Latitude", "Longitude", "Description"
    ]]

def check_corridors(corridors):
    distinct_corridors = corridors[["Zone", "Zone_Group", "Corridor"]].drop_duplicates()

    # Check 1: Same Corridor in multiple Zones
    corridors_in_multiple_zones = distinct_corridors.groupby("Corridor").size()
    check1 = distinct_corridors[distinct_corridors["Corridor"].isin(corridors_in_multiple_zones[corridors_in_multiple_zones > 1].index)]

    # Check 2: Corridors with different cases
    corridors_with_case_mismatches = distinct_corridors["Corridor"].str.lower().value_counts()
    check2 = distinct_corridors[distinct_corridors["Corridor"].str.lower().isin(corridors_with_case_mismatches[corridors_with_case_mismatches > 1].index)]

    if not check1.empty:
        print("Corridors in multiple zones:")
        print(check1)
    if not check2.empty:
        print("Same corridor, different cases:")
        print(check2)

    return check1.empty and check2.empty

def get_cam_config(object, bucket, corridors):
    s3 = boto3.client("s3")
    cam_config0 = pd.read_excel(f"s3://{bucket}/{object}")
    cam_config0 = cam_config0[cam_config0["Include"] == True]
    cam_config0 = cam_config0[["CameraID", "Location", "MaxView ID", "As_of_Date"]].drop_duplicates()

    corrs = corridors[["SignalID", "Zone_Group", "Zone", "Corridor", "Subcorridor"]]
    cam_config = corrs.merge(cam_config0, left_on="SignalID", right_on="MaxView ID", how="left")
    cam_config = cam_config.dropna(subset=["CameraID"])
    cam_config["Description"] = cam_config["CameraID"] + ": " + cam_config["Location"]

    return cam_config.sort_values(["Zone_Group", "Zone", "Corridor", "CameraID"])

def get_ped_config(bucket, date_):
    date_ = max(pd.Timestamp(date_), pd.Timestamp("2019-01-01"))
    s3key = f"config/maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv"
    s3 = boto3.client("s3")

    try:
        obj = s3.get_object(Bucket=bucket, Key=s3key)
        ped_config = pd.read_csv(obj["Body"])
        ped_config = ped_config.groupby(["SignalID", "Detector"]).first().reset_index()
        ped_config = ped_config[["SignalID", "Detector", "CallPhase"]].drop_duplicates()
        return ped_config
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()

def get_det_config(bucket, folder, date_):
    s3 = boto3.client("s3")
    s3prefix = f"config/{folder}/date={date_}"
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=s3prefix)

    if "Contents" in objects:
        det_config = pd.concat([
            pd.read_feather(f"s3://{bucket}/{obj['Key']}")
            for obj in objects["Contents"]
        ])
        det_config = det_config.groupby(["SignalID", "Detector"]).first().reset_index()
        det_config = det_config[["SignalID", "Detector", "CallPhase"]]
        return det_config
    else:
        raise ValueError(f"No detector config file for {date_}")


def get_latest_det_config(conf):
    # Start with today's date
    date_ = datetime.now()

    s3 = boto3.client("s3")

    while True:
        # Format the date as YYYY-MM-DD
        date_str = date_.strftime("%Y-%m-%d")
        prefix = f"config/atspm_det_config_good/date={date_str}"

        # List objects in the S3 bucket with the given prefix
        objects = s3.list_objects_v2(Bucket=conf["bucket"], Prefix=prefix)

        if "Contents" in objects:
            # Get the first object key
            object_key = objects["Contents"][0]["Key"]

            # Read the Feather file from S3
            obj = s3.get_object(Bucket=conf["bucket"], Key=object_key)
            det_config = feather.read_feather(obj["Body"])
            return det_config
        else:
            # Move to the previous day
            date_ -= timedelta(days=1)