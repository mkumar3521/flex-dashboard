import pandas as pd
import geopandas as gpd
import boto3
import s3fs
from shapely.geometry import Point
from geopy.distance import geodesic


def read_corridors(param):
    # throw not implemented error
    raise NotImplementedError("This function is not implemented yet. Please provide the implementation details.")



def get_teams_locations(locs_df, conf):
    s3 = s3fs.S3FileSystem(anon=False)

    # Get the latest key from S3
    client = boto3.client('s3')
    objects = client.list_objects_v2(Bucket=conf['bucket'], Prefix='config/maxv_atspm_intersections')
    keys = [obj['Key'] for obj in objects['Contents']]
    last_key = max(keys)

    # Read signal CSV from S3
    with s3.open(f"{conf['bucket']}/{last_key}", 'r') as f:
        sigs = pd.read_csv(f)

    sigs = sigs[sigs['Latitude'] != 0]
    sigs['SignalID'] = sigs['SignalID'].astype(str)
    sigs['Latitude'] = pd.to_numeric(sigs['Latitude'])
    sigs['Longitude'] = pd.to_numeric(sigs['Longitude'])

    # Read corridors
    corridors = read_corridors(conf['corridors_filename_s3'])

    # Create GeoDataFrames
    locs_gdf = gpd.GeoDataFrame(
        locs_df,
        geometry=gpd.points_from_xy(locs_df.Longitude, locs_df.Latitude),
        crs="EPSG:4326"
    )

    sigs_gdf = gpd.GeoDataFrame(
        sigs,
        geometry=gpd.points_from_xy(sigs.Longitude, sigs.Latitude),
        crs="EPSG:4326"
    )

    # Match each signal to the closest location (within 100m)
    match_rows = []
    for i, sig_row in sigs_gdf.iterrows():
        distances = locs_gdf.geometry.distance(sig_row.geometry)
        min_idx = distances.idxmin()
        dist_meters = sig_row.geometry.distance(locs_gdf.loc[min_idx].geometry) * 111139  # approx meters per degree
        if dist_meters < 100:
            matched_row = pd.concat([sigs_gdf.loc[i], locs_gdf.loc[min_idx]], axis=0)
            matched_row['m'] = int(dist_meters)
            match_rows.append(matched_row)

    if not match_rows:
        return pd.DataFrame()

    matches = pd.DataFrame(match_rows).T
    matches.columns = matches.columns.astype(str)
    matches = matches.T

    # Simplify
    matches['guessID'] = matches['Custom Identifier'].str.split().str[0]
    matches['good_guess'] = (matches['guessID'] == matches['SignalID']).astype(int)

    # Filter to best match per LocationId
    matches = (
        matches.sort_values('good_guess', ascending=False)
        .groupby('DB Id')
        .first()
        .reset_index()
    )

    return matches[[
        'SignalID', 'PrimaryName', 'SecondaryName', 'm', 'DB Id',
        'Maintained By', 'Custom Identifier', 'City', 'County'
    ]].rename(columns={'DB Id': 'LocationId'})
