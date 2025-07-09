import boto3
import s3fs
import re
import pandas as pd
import geopandas as gpd
from io import StringIO
from shapely.geometry import LineString

def points_to_line(data, long, lat, id_field=None, sort_field=None):
    if sort_field:
        if id_field:
            data = data.sort_values(by=[id_field, sort_field])
        else:
            data = data.sort_values(by=sort_field)

    if id_field is None:
        line = LineString(data[[long, lat]].values)
        return gpd.GeoSeries([line])
    else:
        lines = []
        ids = []
        for gid, group in data.groupby(id_field):
            line = LineString(group[[long, lat]].values)
            lines.append(line)
            ids.append(gid)
        return gpd.GeoDataFrame({id_field: ids, 'geometry': lines}, geometry='geometry')


def get_tmc_coords(coords_string):
    match = re.search(r"'(.*?)'", coords_string)
    if not match:
        return pd.DataFrame(columns=["longitude", "latitude"])

    coord_str = match.group(1)
    coords = [c.strip().split() for c in coord_str.split(',')]
    df = pd.DataFrame(coords, columns=["longitude", "latitude"]).astype(float)
    return df

def get_geom_coords(coords_string):
    if pd.notna(coords_string):
        coord_list = list(set(re.split(',|:', coords_string)))
        coords = [c.strip().split() for c in coord_list]
        df = pd.DataFrame(coords, columns=["longitude", "latitude"]).astype(float)
        return df
    return pd.DataFrame(columns=["longitude", "latitude"])


def get_signals_sp(bucket, corridors_df):
    s3 = s3fs.S3FileSystem(anon=False)
    BLACK = "#000000"
    WHITE = "#FFFFFF"
    GRAY = "#D0D0D0"

    # Simulate `get_bucket_df`
    client = boto3.client('s3')
    objects = client.list_objects_v2(Bucket=bucket, Prefix='maxv_atspm_intersections')
    keys = [obj['Key'] for obj in objects['Contents']]
    most_recent_key = max(keys)

    # Read CSV from S3
    with s3.open(f"{bucket}/{most_recent_key}") as f:
        df = pd.read_csv(f)

    df = df[df["Latitude"] != 0]
    df = df[df["Longitude"] != 0]
    df["SignalID"] = df["SignalID"].astype(str)

    merged = df.merge(corridors_df, on="SignalID", how="left")
    merged["Corridor"].fillna("None", inplace=True)

    merged["Description"] = merged.apply(
        lambda row: f"{row['SignalID']}: {row['PrimaryName']} @ {row['SecondaryName']}"
        if pd.isna(row.get("Description")) else row["Description"],
        axis=1
    )

    merged["fill_color"] = merged["Zone"].apply(lambda z: BLACK if str(z).startswith("Z") else WHITE)
    merged["stroke_color"] = merged.apply(
        lambda row: row["color"] if str(row["Zone"]).startswith("Z") else (GRAY if row["Corridor"] == "None" else BLACK),
        axis=1
    )

    return merged


def get_map_data(conf):
    BLACK = "#000000"
    WHITE = "#FFFFFF"
    GRAY = "#D0D0D0"
    DARK_DARK_GRAY = "#494949"

    palette = ["#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
               "#ff7f00", "#ffff33", "#a65628", "#f781bf"]

    # Load corridors
    corridors_df = pd.read_feather(f"s3://{conf['bucket']}/all_Corridors_Latest.feather")
    corridors_df["Corridor"].fillna("None", inplace=True)

    rtop_corridors = corridors_df[corridors_df["Zone"].str.startswith("Zone", na=False)][["Corridor"]].drop_duplicates()
    num_corridors = len(rtop_corridors)

    colors = (palette * (num_corridors // len(palette) + 1))[:num_corridors]
    rtop_corridors["color"] = colors

    corridor_colors = pd.concat([
        rtop_corridors,
        pd.DataFrame([{"Corridor": "None", "color": GRAY}])
    ])

    # Load TMCs
    tmcs = pd.read_excel(f"s3://{conf['bucket']}/Corridor_TMCs_Latest.xlsx")
    tmcs["Corridor"].fillna("None", inplace=True)
    tmcs = tmcs.merge(corridor_colors, on="Corridor", how="left")
    tmcs["color"] = tmcs.apply(
        lambda row: DARK_DARK_GRAY if row["Corridor"] != "None" and pd.isna(row["color"]) else row["color"], axis=1)

    tmcs["tmc_coords"] = tmcs["coordinates"].apply(get_tmc_coords)
    tmcs["geometry"] = tmcs["tmc_coords"].apply(
        lambda df: LineString(df[["longitude", "latitude"]].values) if not df.empty else None)

    corridors_sp = gpd.GeoDataFrame(tmcs.dropna(subset=["geometry"]), geometry="geometry")

    subcor_tmcs = tmcs[tmcs["Subcorridor"].notna()]
    subcorridors_sp = gpd.GeoDataFrame(subcor_tmcs.dropna(subset=["geometry"]), geometry="geometry")

    signals_sp = get_signals_sp(conf['bucket'], corridors_df)

    return {
        "corridors_sp": corridors_sp,
        "subcorridors_sp": subcorridors_sp,
        "signals_sp": signals_sp
    }
