# Monthly_Report_Package_1.py
# Python port of Monthly_Report_Package_1.R

import pandas as pd
import numpy as np
import gc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import Monthly_Report_Functions as mrf
import Monthly_Report_Calcs_init_bkp as mr_init
import s3_parquet_io as s3_io
import aggregations as agg
import metrics
import utilities as utils


def save_to_rds(df, filename, metric_name, report_start_date, calcs_start_date):
    """
    Save DataFrame to pickle file (Python equivalent of R's RDS)
    """
    # Filter data based on date range
    if 'Date' in df.columns:
        df = df[df['Date'] >= pd.to_datetime(calcs_start_date)]
    
    df.to_pickle(filename.replace('.rds', '.pkl'))
    print(f"Saved {filename}")


def get_avg_daily_detector_uptime(data):
    """
    Calculate average daily detector uptime
    """
    return data.groupby(['SignalID', 'Date']).agg({
        'uptime': 'mean'
    }).reset_index()


def get_pau_gamma(dates, papd, paph, corridors, wk_calcs_start_date, pau_start_date):
    """
    Calculate pedestrian activation uptime using gamma distribution method
    Placeholder implementation - needs actual gamma distribution logic
    """
    # Placeholder - implement gamma distribution uptime calculation
    result = papd.copy()
    result['uptime'] = np.random.uniform(0.7, 1.0, len(result))  # Placeholder
    result['all'] = 1
    return result


def get_bad_ped_detectors(pau):
    """
    Identify bad pedestrian detectors based on uptime
    """
    return pau[pau['uptime'] < 0.5]  # Placeholder threshold


def process_detector_uptime():
    """Process vehicle detector uptime - Section 1 of 29"""
    print(f"{datetime.now()} Vehicle Detector Uptime [1 of 29 (mark1)]")
    
    try:
        # Read detector uptime data
        avg_daily_detector_uptime = s3_io.s3_read_parquet_parallel(
            bucket=mrf.conf['bucket'],
            table_name="detector_uptime_pd",
            start_date=mr_init.wk_calcs_start_date,
            end_date=mr_init.report_end_date,
            signals_list=mr_init.signals_list,
            callback=get_avg_daily_detector_uptime
        )
        
        avg_daily_detector_uptime['Date'] = pd.to_datetime(avg_daily_detector_uptime['Date'])
        avg_daily_detector_uptime['SignalID'] = avg_daily_detector_uptime['SignalID'].astype('category')
        
        # Calculate corridor averages
        cor_avg_daily_detector_uptime = agg.get_cor_avg_daily_detector_uptime(
            avg_daily_detector_uptime, mr_init.corridors
        )
        
        sub_avg_daily_detector_uptime = agg.get_cor_avg_daily_detector_uptime(
            avg_daily_detector_uptime, mr_init.subcorridors
        ).dropna(subset=['Corridor'])
        
        # Calculate weekly averages
        weekly_detector_uptime = agg.get_weekly_detector_uptime(avg_daily_detector_uptime)
        cor_weekly_detector_uptime = agg.get_cor_weekly_detector_uptime(
            weekly_detector_uptime, mr_init.corridors
        )
        sub_weekly_detector_uptime = agg.get_cor_weekly_detector_uptime(
            weekly_detector_uptime, mr_init.subcorridors
        ).dropna(subset=['Corridor'])
        
        # Calculate monthly averages
        monthly_detector_uptime = agg.get_monthly_detector_uptime(avg_daily_detector_uptime)
        cor_monthly_detector_uptime = agg.get_cor_monthly_detector_uptime(
            avg_daily_detector_uptime, mr_init.corridors
        )
        sub_monthly_detector_uptime = agg.get_cor_monthly_detector_uptime(
            avg_daily_detector_uptime, mr_init.subcorridors
        ).dropna(subset=['Corridor'])
        
        # Save to files
        save_to_rds(avg_daily_detector_uptime, "avg_daily_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(weekly_detector_uptime, "weekly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.wk_calcs_start_date)
        save_to_rds(monthly_detector_uptime, "monthly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        
        # Corridor data
        save_to_rds(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(cor_weekly_detector_uptime, "cor_weekly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.wk_calcs_start_date)
        save_to_rds(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        
        # Subcorridor data
        save_to_rds(sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(sub_weekly_detector_uptime, "sub_weekly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.wk_calcs_start_date)
        save_to_rds(sub_monthly_detector_uptime, "sub_monthly_detector_uptime.pkl", 
                   "uptime", mr_init.report_start_date, mr_init.calcs_start_date)
        
        # Clean up memory
        del avg_daily_detector_uptime, weekly_detector_uptime, monthly_detector_uptime
        del cor_avg_daily_detector_uptime, cor_weekly_detector_uptime, cor_monthly_detector_uptime
        del sub_avg_daily_detector_uptime, sub_weekly_detector_uptime, sub_monthly_detector_uptime
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)


def process_ped_pushbutton_uptime():
    """Process pedestrian pushbutton uptime - Section 2 of 29"""
    print(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (mark1)]")
    
    try:
        # Calculate start date for pedestrian analysis (needs longer history)
        pau_start_date = min(
            pd.to_datetime(mr_init.calcs_start_date),
            pd.to_datetime(mr_init.report_end_date).replace(day=1) - relativedelta(months=6)
        ).strftime('%Y-%m-%d')
        
        # Read pedestrian counts
        counts_ped_hourly = s3_io.s3_read_parquet_parallel(
            bucket=mrf.conf['bucket'],
            table_name="counts_ped_1hr",
            start_date=pau_start_date,
            end_date=mr_init.report_end_date,
            signals_list=mr_init.signals_list,
            parallel=False
        )
        
        # Clean and process data
        counts_ped_hourly = counts_ped_hourly.dropna(subset=['CallPhase'])
        counts_ped_hourly['SignalID'] = counts_ped_hourly['SignalID'].astype('category')
        counts_ped_hourly['Detector'] = counts_ped_hourly['Detector'].astype('category')
        counts_ped_hourly['CallPhase'] = counts_ped_hourly['CallPhase'].astype('category')
        counts_ped_hourly['Date'] = pd.to_datetime(counts_ped_hourly['Date']).dt.date
        counts_ped_hourly['DOW'] = pd.to_datetime(counts_ped_hourly['Date']).dt.dayofweek
        counts_ped_hourly['Week'] = pd.to_datetime(counts_ped_hourly['Date']).dt.isocalendar().week
        counts_ped_hourly['vol'] = pd.to_numeric(counts_ped_hourly['vol'])
        
        # Calculate daily pedestrian activations
        counts_ped_daily = counts_ped_hourly.groupby([
            'SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase'
        ]).agg({'vol': 'sum'}).reset_index()
        counts_ped_daily.rename(columns={'vol': 'papd'}, inplace=True)
        
        papd = counts_ped_daily
        paph = counts_ped_hourly.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'})
        
        # Calculate pedestrian uptime using gamma method
        dates = pd.date_range(start=pau_start_date, end=mr_init.report_end_date, freq='D')
        pau = get_pau_gamma(dates, papd, paph, mr_init.corridors, 
                           mr_init.wk_calcs_start_date, pau_start_date)
        
        # Filter and replace bad data
        pau['papd'] = np.where(pau['uptime'] == 1, pau['papd'], np.nan)
        pau_grouped = pau.groupby(['SignalID', 'Detector', 'CallPhase', 
                                  pau['Date'].dt.year, pau['Date'].dt.month])
        pau['papd'] = pau_grouped['papd'].transform(
            lambda x: np.where(pau['uptime'] == 1, x, np.floor(x.mean()))
        )
        
        # Identify bad detectors
        bad_detectors = get_bad_ped_detectors(pau)
        bad_detectors = bad_detectors[bad_detectors['Date'] >= mr_init.calcs_start_date]
        
        if not bad_detectors.empty:
            s3_io.s3_upload_parquet_date_split(
                bad_detectors,
                bucket=mrf.conf['bucket'],
                prefix="bad_ped_detectors",
                table_name="bad_ped_detectors",
                conf_athena=mrf.conf['athena'],
                parallel=False
            )
        
        # Save pedestrian uptime data
        save_to_rds(pau, "pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        
        # Calculate daily, weekly, monthly aggregations
        pau['CallPhase'] = pau['Detector']  # Hack for aggregation functions
        
        daily_pa_uptime = agg.get_daily_avg(pau, "uptime", peak_only=False)
        weekly_pa_uptime = agg.get_weekly_avg_by_day(pau, "uptime", peak_only=False)
        monthly_pa_uptime = agg.get_monthly_avg_by_day(pau, "uptime", "all", peak_only=False)
        
        # Calculate corridor aggregations
        cor_daily_pa_uptime = agg.get_cor_weekly_avg_by_day(daily_pa_uptime, mr_init.corridors, "uptime")
        sub_daily_pa_uptime = agg.get_cor_weekly_avg_by_day(daily_pa_uptime, mr_init.subcorridors, "uptime")
        sub_daily_pa_uptime = sub_daily_pa_uptime.dropna(subset=['Corridor'])
        
        cor_weekly_pa_uptime = agg.get_cor_weekly_avg_by_day(weekly_pa_uptime, mr_init.corridors, "uptime")
        sub_weekly_pa_uptime = agg.get_cor_weekly_avg_by_day(weekly_pa_uptime, mr_init.subcorridors, "uptime")
        sub_weekly_pa_uptime = sub_weekly_pa_uptime.dropna(subset=['Corridor'])
        
        cor_monthly_pa_uptime = agg.get_cor_monthly_avg_by_day(monthly_pa_uptime, mr_init.corridors, "uptime")
        sub_monthly_pa_uptime = agg.get_cor_monthly_avg_by_day(monthly_pa_uptime, mr_init.subcorridors, "uptime")
        sub_monthly_pa_uptime = sub_monthly_pa_uptime.dropna(subset=['Corridor'])
        
        # Save all data
        save_to_rds(daily_pa_uptime, "daily_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(cor_daily_pa_uptime, "cor_daily_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(sub_daily_pa_uptime, "sub_daily_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        
        save_to_rds(weekly_pa_uptime, "weekly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.wk_calcs_start_date)
        save_to_rds(cor_weekly_pa_uptime, "cor_weekly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.wk_calcs_start_date)
        save_to_rds(sub_weekly_pa_uptime, "sub_weekly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.wk_calcs_start_date)
        
        save_to_rds(monthly_pa_uptime, "monthly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(cor_monthly_pa_uptime, "cor_monthly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        save_to_rds(sub_monthly_pa_uptime, "sub_monthly_pa_uptime.pkl", "uptime", 
                   mr_init.report_start_date, mr_init.calcs_start_date)
        
        # Clean up memory
        del pau, daily_pa_uptime, weekly_pa_uptime, monthly_pa_uptime
        del cor_daily_pa_uptime, cor_weekly_pa_uptime, cor_monthly_pa_uptime
        del sub_daily_pa_uptime, sub_weekly_pa_uptime, sub_monthly_pa_uptime
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)


def process_watchdog_alerts():
    """Process watchdog alerts - Section 3 of 29"""
    print(f"{datetime.now()} watchdog alerts [3 of 29 (mark1)]")
    
    try:
        # Process vehicle detector alerts
        bad_det = s3_io.s3_read_parquet_parallel(
            "bad_detectors",
            start_date=(datetime.now() - timedelta(days=90)).date(),
            end_date=(datetime.now() - timedelta(days=1)).date(),
            bucket=mrf.conf['bucket']
        )
        
        bad_det['SignalID'] = bad_det['SignalID'].astype('category')
        bad_det['Detector'] = bad_det['Detector'].astype('category')
        
        # Get detector configuration for each date
        unique_dates = sorted(bad_det['Date'].unique())
        det_configs = []
        
        for date_ in unique_dates:
            det_config = utils.get_det_config(date_)
            det_config = det_config[['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber']]
            det_config['Date'] = date_
            det_configs.append(det_config)
        
        if det_configs:
            det_config = pd.concat(det_configs, ignore_index=True)
            det_config['SignalID'] = det_config['SignalID'].astype('category')
            det_config['CallPhase'] = det_config['CallPhase'].astype('category')
            det_config['Detector'] = det_config['Detector'].astype('category')
            
            # Join configurations with bad detectors
            bad_det = bad_det.merge(det_config, on=['SignalID', 'Detector', 'Date'], how='left')
            
            # Join with corridor information
            corridors_subset = mr_init.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']]
            bad_det = bad_det.merge(corridors_subset, on='SignalID', how='left')
            
            # Filter and format
            bad_det = bad_det.dropna(subset=['Corridor'])
            bad_det['Alert'] = 'Bad Vehicle Detection'
            bad_det['Name'] = bad_det['Name'].str.replace('@', '-').where(
                bad_det['Corridor'] == 'Ramp Meter', bad_det['Name']
            )
            bad_det['ApproachDesc'] = bad_det['ApproachDesc'].fillna('').astype(str) + \
                                    ' Lane ' + bad_det['LaneNumber'].astype(str)
            bad_det['ApproachDesc'] = bad_det['ApproachDesc'].str.strip()
            
            # Select final columns
            bad_det = bad_det[[
                'Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 
                'Detector', 'Date', 'Alert', 'Name', 'ApproachDesc'
            ]]
            
            # Save to S3
            s3_io.s3write_using(
                bad_det,
                bucket=mrf.conf['bucket'],
                object="mark/watchdog/bad_detectors.parquet",
                write_func='parquet'
            )
        
        # Process pedestrian detector alerts
        bad_ped = s3_io.s3_read_parquet_parallel(
            "bad_ped_detectors",
            start_date=(datetime.now() - timedelta(days=90)).date(),
            end_date=(datetime.now() - timedelta(days=1)).date(),
            bucket=mrf.conf['bucket']
        )
        
        if not bad_ped.empty:
            bad_ped['SignalID'] = bad_ped['SignalID'].astype('category')
            bad_ped['Detector'] = bad_ped['Detector'].astype('category')
            
            # Join with corridor information
            bad_ped = bad_ped.merge(corridors_subset, on='SignalID', how='left')
            bad_ped['Alert'] = 'Bad Ped Detection'
            
            bad_ped = bad_ped[[
                'Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Detector', 
                'Date', 'Alert', 'Name'
            ]]
            
            s3_io.s3write_using(
                bad_ped,
                bucket=mrf.conf['bucket'],
                object="mark/watchdog/bad_ped_pushbuttons.parquet",
                write_func='parquet'
            )
        
        # Process CCTV alerts (placeholder)
        # Implementation would depend on specific CCTV data structure
        
        print("Watchdog alerts processed successfully")
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)


def process_placeholder_section(section_name, section_number):
    """
    Placeholder for sections not yet implemented
    """
    print(f"{datetime.now()} {section_name} [{section_number} of 29 (mark1)]")
    
    try:
        print(f"Processing {section_name}...")
        # Placeholder - implement specific logic for each section
        print(f"{section_name} completed (placeholder)")
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)


def main():
    """Main execution function"""
    print(f"{datetime.now()} Starting Monthly Report Package 1")
    
    # Section 1: Vehicle Detector Uptime
    process_detector_uptime()
    gc.collect()
    
    # Section 2: Pedestrian Pushbutton Uptime
    process_ped_pushbutton_uptime()
    gc.collect()
    
    # Section 3: Watchdog Alerts
    process_watchdog_alerts()
    gc.collect()
    
    # Placeholder sections (implement as needed)
    sections = [
        ("Daily Pedestrian Activations", 4),
        ("Hourly Pedestrian Activations", 5),
        ("Pedestrian Delay", 6),
        ("Communication Uptime", 7),
        ("Daily Volumes", 8),
        ("Hourly Volumes", 9),
        ("Daily Throughput", 10),
        ("Daily Arrivals on Green", 11),
        ("Hourly Arrivals on Green", 12),
        ("Daily Progression Ratio", 13),
        ("Hourly Progression Ratio", 14),
        ("Daily Split Failures", 15),
        ("Hourly Split Failures", 16),
        ("Daily Queue Spillback", 17),
        ("Hourly Queue Spillback", 18),
        ("Travel Time and Buffer Time Indexes", 19),
        ("Activities", 20),
        ("User Delay Costs", 21),
        ("Flash Events", 22),
        ("Bike/Ped Safety Index", 23),
        ("Relative Speed Index", 24),
        ("Crash Indices", 25)
    ]
    
    for section_name, section_number in sections:
        process_placeholder_section(section_name, section_number)
        gc.collect()
    
    print(f"{datetime.now()} Monthly Report Package 1 completed")


if __name__ == "__main__":
    main() 