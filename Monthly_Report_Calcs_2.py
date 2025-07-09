# Monthly_Report_Calcs_2.py

import subprocess
import gc
from datetime import datetime
import pandas as pd

import Monthly_Report_Functions as mrf
import Monthly_Report_Calcs_init as mr_init
import s3_parquet_io as s3_io
import metrics
import utilities as utils


def run_python_script(script, args="", wait=True):
    """Run a Python script with conda environment"""
    cmd = f"~/miniconda3/bin/conda run -n sigops python {script} {args}"
    if wait:
        subprocess.run(cmd, shell=True)
    else:
        subprocess.Popen(cmd, shell=True)


def get_qs(detection_events, intervals=["hour", "15min"]):
    """
    Calculate queue spillback metrics from detection events
    This is a placeholder - implement queue spillback logic based on your requirements
    """
    # Placeholder implementation - replace with actual queue spillback algorithm
    result = {}
    
    for interval in intervals:
        # Group by appropriate time interval
        if interval == "hour":
            detection_events['timeperiod'] = detection_events['timestamp'].dt.floor('H')
        elif interval == "15min":
            detection_events['timeperiod'] = detection_events['timestamp'].dt.floor('15min')
        
        # Calculate queue spillback metrics (placeholder logic)
        qs_data = detection_events.groupby(['signalid', 'timeperiod']).agg({
            'timestamp': 'count'  # Replace with actual queue spillback calculation
        }).reset_index()
        
        qs_data.columns = ['SignalID', 'Timeperiod', 'queue_spillback_metric']
        result[interval] = qs_data
    
    return result


def get_ped_delay(date_, conf, signals_list):
    """
    Calculate pedestrian delay using ATSPM method
    Based on push button-start of walk durations
    """
    # Placeholder implementation - replace with actual pedestrian delay algorithm
    # This would typically involve:
    # 1. Getting pedestrian button press events
    # 2. Getting walk signal start events  
    # 3. Calculating delay between button press and walk start
    
    # For now, return empty DataFrame - implement based on your specific requirements
    pd_data = pd.DataFrame(columns=['SignalID', 'Date', 'ped_delay'])
    
    return pd_data


def get_sf_utah(date_, conf, signals_list, intervals=["hour", "15min"]):
    """
    Calculate split failures using Utah method
    Based on green, start-of-red occupancies
    """
    # Placeholder implementation - replace with actual split failure algorithm
    result = {}
    
    for interval in intervals:
        # This would typically involve:
        # 1. Getting cycle data for the date
        # 2. Analyzing green and red occupancy patterns
        # 3. Identifying split failures based on Utah methodology
        
        # For now, return empty DataFrame - implement based on your specific requirements
        sf_data = pd.DataFrame(columns=['SignalID', 'Date', 'split_failures'])
        result[interval] = sf_data
    
    return result


def get_queue_spillback_date_range(start_date, end_date):
    """Process queue spillback for a date range"""
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    for date_ in date_range:
        print(f"Processing queue spillback for {date_.date()}")
        
        detection_events = metrics.get_detection_events(
            date_.date(), 
            date_.date(), 
            mrf.conf['athena'], 
            mr_init.signals_list
        )
        
        if not detection_events.empty:
            qs = get_qs(detection_events, intervals=["hour", "15min"])
            
            if not qs['hour'].empty:
                s3_io.s3_upload_parquet_date_split(
                    qs['hour'],
                    mrf.conf['bucket'],
                    "qs",
                    "queue_spillback",
                    mrf.conf['athena']
                )
            
            if not qs['15min'].empty:
                s3_io.s3_upload_parquet_date_split(
                    qs['15min'],
                    mrf.conf['bucket'],
                    "qs", 
                    "queue_spillback_15min",
                    mrf.conf['athena']
                )


def get_pd_date_range(start_date, end_date):
    """Process pedestrian delay for a date range"""
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    for date_ in date_range:
        print(f"Processing pedestrian delay for {date_.date()}")
        
        pd_data = get_ped_delay(date_.date(), mrf.conf, mr_init.signals_list)
        
        if not pd_data.empty:
            s3_io.s3_upload_parquet_date_split(
                pd_data,
                mrf.conf['bucket'],
                "pd",
                "ped_delay", 
                mrf.conf['athena']
            )
    
    gc.collect()


def get_sf_date_range(start_date, end_date):
    """Process split failures for a date range"""
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    for date_ in date_range:
        print(f"Processing split failures for {date_.date()}")
        
        sf = get_sf_utah(date_.date(), mrf.conf, mr_init.signals_list, intervals=["hour", "15min"])
        
        if not sf['hour'].empty:
            s3_io.s3_upload_parquet_date_split(
                sf['hour'],
                mrf.conf['bucket'],
                "sf",
                "split_failures",
                mrf.conf['athena']
            )
        
        if not sf['15min'].empty:
            s3_io.s3_upload_parquet_date_split(
                sf['15min'],
                mrf.conf['bucket'],
                "sf",
                "split_failures_15min",
                mrf.conf['athena']
            )


def main():
    """Main execution function"""
    start_date = mr_init.start_date
    end_date = mr_init.end_date
    
    # ETL Dashboard
    print(f"{datetime.now()} etl [7 of 11]")
    if mrf.conf['run'].get('etl', True):
        run_python_script("etl_dashboard.py", f"{start_date} {end_date}")
    
    # Arrivals on Green
    print(f"{datetime.now()} aog [8 of 11]")
    if mrf.conf['run'].get('arrivals_on_green', True):
        run_python_script("get_aog.py", f"{start_date} {end_date}")
    
    gc.collect()
    
    # Queue Spillback
    print(f"{datetime.now()} queue spillback [9 of 11]")
    if mrf.conf['run'].get('queue_spillback', True):
        get_queue_spillback_date_range(start_date, end_date)
    
    # Pedestrian Delay
    print(f"{datetime.now()} ped delay [10 of 11]")
    if mrf.conf['run'].get('ped_delay', True):
        get_pd_date_range(start_date, end_date)
    
    # Split Failures
    print(f"{datetime.now()} split failures [11 of 11]")
    if mrf.conf['run'].get('split_failures', True):
        # Utah method, based on green, start-of-red occupancies
        get_sf_date_range(start_date, end_date)
    
    # Flash Events
    print(f"{datetime.now()} flash events [12 of 12]")
    if mrf.conf['run'].get('flash_events', True):
        run_python_script("get_flash_events.py")
    
    gc.collect()
    
    print("\n--------------------- End Monthly Report calcs -----------------------\n")


if __name__ == "__main__":
    main() 