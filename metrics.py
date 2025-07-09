import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from database_functions import get_atspm_connection, get_athena_connection


def get_uptime(df, start_date, end_time):
    """
    Calculate uptime for signals based on their timestamps.
    """
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.floor('min')
    ts_sig = df[['signalid', 'timestamp']].drop_duplicates()

    signals = ts_sig['signalid'].unique()
    bookend1 = pd.DataFrame({'SignalID': signals, 'Timestamp': pd.to_datetime(f"{start_date} 00:00:00")})
    bookend2 = pd.DataFrame({'SignalID': signals, 'Timestamp': pd.to_datetime(end_time)})

    ts_sig = pd.concat([ts_sig.rename(columns={'signalid': 'SignalID', 'timestamp': 'Timestamp'}), bookend1, bookend2]).drop_duplicates()
    ts_sig = ts_sig.sort_values(by=['SignalID', 'Timestamp'])

    ts_all = ts_sig[['Timestamp']].drop_duplicates()
    ts_all['SignalID'] = 0
    ts_all = ts_all.sort_values(by=['Timestamp'])

    def calculate_uptime(data):
        """
        Calculate uptime for each signal based on the timestamps.
        """
        data['Date'] = data['Timestamp'].dt.date
        data['lag_Timestamp'] = data.groupby(['SignalID', 'Date'])['Timestamp'].shift()
        data['span'] = (data['Timestamp'] - data['lag_Timestamp']).dt.total_seconds() / 60
        data['span'] = np.where(data['span'] > 15, data['span'], 0)
        uptime = data.groupby(['SignalID', 'Date']).agg(uptime=lambda x: 1 - x.sum() / (60 * 24)).reset_index()
        return uptime

    uptime_sig = calculate_uptime(ts_sig)
    uptime_all = calculate_uptime(ts_all)
    uptime_all = uptime_all.drop(columns=['SignalID']).rename(columns={'uptime': 'uptime_all'})

    return {'sig': uptime_sig, 'all': uptime_all}

def get_spm_data_atspm(start_date, end_date, conf_atspm, table, signals_list=None, eventcodes=None, TWR_only=False):
    conn = get_atspm_connection(conf_atspm)
    end_date1 = pd.to_datetime(end_date) + timedelta(days=1)
    df = pd.read_sql_table(table, conn)

    if signals_list is not None:
        df = df[df['SignalID'].isin(signals_list)]

    df = df[(df['Timestamp'] >= start_date) & (df['Timestamp'] < end_date1)]

    if eventcodes is not None:
        df = df[df['EventCode'].isin(eventcodes)]

    if TWR_only:
        df = df[df['Timestamp'].dt.weekday.isin([1, 2, 3])]

    return df

def get_spm_data_aws(start_date, end_date, signals_list=None, conf_athena=None, table=None, TWR_only=True):
    conn = get_athena_connection(conf_athena)
    df = pd.read_sql_table(table, conn)
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

    if TWR_only:
        df = df[df['date'].dt.weekday.isin([1, 2, 3])]

    if signals_list is not None:
        df = df[df['signalid'].isin(signals_list)]

    return df

def get_cycle_data(start_date, end_date, conf_athena, signals_list=None):
    return get_spm_data_aws(start_date, end_date, signals_list, conf_athena, table="CycleData", TWR_only=False)

def get_detection_events(start_date, end_date, conf_athena, signals_list=None):
    return get_spm_data_aws(start_date, end_date, signals_list, conf_athena, table="DetectionEvents", TWR_only=False)

def get_detector_uptime(filtered_counts_1hr):
    return filtered_counts_1hr.drop_duplicates(subset=['Date', 'SignalID', 'Detector', 'Good_Day']).set_index(['SignalID', 'Detector']).reindex(pd.date_range(filtered_counts_1hr['Date'].min(), filtered_counts_1hr['Date'].max(), freq='D'), fill_value=0).reset_index()

def get_bad_detectors(filtered_counts_1hr):
    return get_detector_uptime(filtered_counts_1hr)[filtered_counts_1hr['Good_Day'] == 0]

def get_vpd(counts, mainline_only=True):
    if mainline_only:
        counts = counts[counts['CallPhase'].isin([2, 6])]
    counts['Date'] = pd.to_datetime(counts['Timeperiod']).dt.date
    return counts.groupby(['SignalID', 'CallPhase', 'Date']).agg(vpd=('vol', 'sum')).reset_index()

def get_thruput(counts):
    counts['Date'] = pd.to_datetime(counts['Timeperiod']).dt.date
    counts['vph'] = counts.groupby(['SignalID', 'Date', 'Timeperiod'])['vol'].transform('sum')
    counts['vph'] = counts.groupby(['SignalID', 'Date'])['vph'].transform(lambda x: np.percentile(x, 95) * 4)
    return counts[['SignalID', 'Date', 'vph']].drop_duplicates()

# Additional functions can be ported similarly based on the logic provided in the R script.