import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
import numpy as np
import os
from datetime import datetime
import gcsfs
import json

import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

ce_bucket_name = 'rdr-prod-ce-digital-health'
ce_bucket_id = f'gs://{ce_bucket_name}'

storage_client = storage.Client()
bucket = storage_client.get_bucket(ce_bucket_name)

# ACTIVITIES: daily_activity_summary and steps intraday

daily_activities_uris = {
'ce_activity_summary':{
    
 'activities-tracker-activityCalories':
    {'activity_calories': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-activityCalories/*'},
 
'activities-caloriesBMR':
    {'calories_bmr': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-caloriesBMR/*'},
 
'activities-tracker-calories':
    {'calories_out': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-calories/*'},

 'activities-tracker-elevation':
    {'elevation':'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-elevation/*'},
 
'activities-tracker-floors':
    {'floors': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-floors/*'},
    
 'activities-tracker-minutesFairlyActive':
    {'fairly_active_minutes': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-minutesFairlyActive/*'},
 
'activities-tracker-minutesLightlyActive':
    {'lightly_active_minutes': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-minutesLightlyActive/*'},
 
'activities-tracker-minutesSedentary':
    {'sedentary_minutes': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-minutesSedentary/*'},
 
'activities-tracker-minutesVeryActive':
    {'very_active_minutes':'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-minutesVeryActive/*'}, 
 
'activities-tracker-steps':
    {'steps': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-tracker-steps/*'},
    }
}

steps_intraday_uris = {
'ce_steps_intraday':{
    
 'activities-steps-intraday':
    {'steps': 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-steps-intraday/*'},
    }
}

# HEART RATE: daily_heart_rate_summary & min_level heart rate intraday 
heartrate_uris = {
'ce_heart_rate_summary':{    
'activities-heart':   
    {"activities-heart": 'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/activities-heart-intraday/*'},
    }
}

# SLEEP # Final Table: ce_sleep_daily_summary and ce_sleep_level
sleep_uris = {
'ce_sleep_daily_summary':{
    'sleep':
    {'sleep':'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/sleep/*'} 
    }
}

# DEVICE # Final Table: ce_device
device_uris = {
'ce_device':{
    'devices':
    {'devices':'gs://rdr-prod-ce-digital-health/raw/health/{upload_date}/FITBIT/devices/*'} 
    }
}


##

def get_upload_date_range(start_date_YYYYMMDD, end_date_YYYYMMDD):

    if start_date_YYYYMMDD.lower() == 'yesterday':
        start_date = (datetime.now() - timedelta(days =1)).date()
        end_date = start_date
    else:
        start_date = datetime.date(pd.to_datetime(start_date_YYYYMMDD.strip(' ')))
        end_date = datetime.date(pd.to_datetime(end_date_YYYYMMDD.strip(' ')))

    # Format date range based on user input
    upload_date_range = pd.date_range(start_date, end_date)
    upload_date_range = [str(datetime.date(d)).replace('-','/') for d in upload_date_range]
    print('Upload date range: '+ str(start_date)+ ' to '+ str(end_date))
    
    return upload_date_range

def check_fitbit_data(check_what, table_list = ["ce_heart_rate_second_level", 'ce_heart_rate_summary', 'ce_sleep_daily_summary'
                                                , 'ce_sleep_level', 'ce_sleep_daily_summary_counts', 'ce_sleep_daily_summary_30dayavg'
                                                , 'ce_sleep_daily_summary_ext', 'ce_sleep_level_short', 'ce_steps_intraday', 'ce_device']
                      , add_condition = '', project_id = cur_project_id, dataset_name = ce_dataset_name):
    
    #construct query
    query = f'''
        SELECT {check_what}, '{table_list[0]}' as table 
        FROM `{project_id}.{dataset_name}.{table_list[0]}`
        
        '''
    for table in table_list[1:]:
        query +=f'''
        UNION ALL
        SELECT {check_what}, '{table}' as table  
        FROM `{project_id}.{dataset_name}.{table}`
        '''
    QUERY = '''
    WITH data as ({query})
    SELECT * 
    FROM data
    add_condition
    '''
   
    #run query
    check_df = pd_gbq.read_gbq(query, project_id=project_id, progress_bar_type = None).sort_values(['table'])
    return check_df


def create_federated_table(federated_table_name, uris, maxBadRecords = 0, dataset_name = ce_dataset_name, project_id=cur_project_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_name).table(federated_table_name)
    table = bigquery.Table(table_ref)
    extconfig = bigquery.ExternalConfig('CSV')
    extconfig.schema = [bigquery.SchemaField('data', 'STRING')]
    extconfig.options.autodetect = False
    extconfig.options.field_delimiter = '|' #u'\u00ff'
    extconfig.options.allow_jagged_rows = True
    extconfig.options.ignore_unknown_values = True
    extconfig.options.allow_quoted_newlines = True
    extconfig.max_bad_records = maxBadRecords
    extconfig.source_uris = uris
    table.external_data_configuration = extconfig
    bq_client.delete_table(table, not_found_ok=True) 
    bq_client.create_table(table)


def delete_table(table, dataset_name = ce_dataset_name, project_id=cur_project_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_name).table(table)
    table = bigquery.Table(table_ref)
    bq_client.delete_table(table, not_found_ok=True) 


def load_federated_table_to_bigquery(federated_table_name, staging_table_name
                                     , dataset_name = ce_dataset_name, project_id=cur_project_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    federated_table_id = f'{project_id}.{dataset_name}.{federated_table_name}'
    staging_table_id = f'{project_id}.{dataset_name}.{staging_table_name}'
    
    job_config = bigquery.QueryJobConfig(destination=staging_table_id, write_disposition = 'WRITE_TRUNCATE') #Was WRITE_APPEND
    sql = f"""
        SELECT
            *, _FILE_NAME as filename
        FROM `{federated_table_id}`
    """
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)# Make an API request.
    
    return query_job.result()  # Wait for the job to complete.


queries = {
    
'ce_activity_summary': """
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, '{dtype}/P([0-9]+)/') AS INT64) AS careevolution_id,
        SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date,
        SAFE_CAST(REPLACE(SPLIT(SPLIT(SPLIT(filename, '_date_')[OFFSET(1)], '.json')[OFFSET(0)], '_')[OFFSET(1)], '/', '-') AS DATE) as max_date,
        SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateTime") AS DATE) AS date,
        SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.value") AS STRING) AS {column},

    FROM `{staging_table_id}`
    , UNNEST(JSON_EXTRACT_ARRAY(data, "$.{dtype}")) AS unnested_data """,
    
'ce_steps_intraday':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'activities-steps-intraday/P([0-9]+)/') AS INT64) AS careevolution_id,
        SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date,
        SAFE_CAST(CONCAT(JSON_EXTRACT_SCALAR(unnested_data, "$.dateTime"), 'T' 
                        , REPLACE(JSON_EXTRACT_SCALAR(unnested_intraday_data, "$.time"), '-',':')
                        ) AS DATETIME) AS datetime, 
        SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_intraday_data, "$.value") AS NUMERIC) AS steps,
  
        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.activities-steps")) AS unnested_data
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.activities-steps-intraday['dataset']")) as unnested_intraday_data """,

    
#######################
'ce_heart_rate_summary':"""
    SELECT DISTINCT
         CAST(REGEXP_EXTRACT(filename, 'activities-heart-intraday/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateTime") AS DATE) AS date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_hr_data, "$.name") AS STRING) AS zone_name
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_hr_data, "$.min") AS INTEGER) AS min_heart_rate
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_hr_data, "$.max") AS INTEGER) AS max_heart_rate
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_hr_data, "$.minutes") AS INTEGER) AS minutes_in_zone
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_hr_data, "$.caloriesOut") AS FLOAT64) AS calorie_count

        FROM `{staging_table_id}`
        , UNNEST(JSON_QUERY_ARRAY(data, "$.activities-heart")) AS unnested_data
        , UNNEST(JSON_QUERY_ARRAY(unnested_data, "$.value.heartRateZones")) as unnested_hr_data""",
    
'ce_heart_rate_second_level':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'activities-heart-intraday/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(CONCAT(JSON_EXTRACT_SCALAR(unnested_data, "$.dateTime"), 'T' 
                        , REPLACE(JSON_EXTRACT_SCALAR(unnested_intraday_data, "$.time"), '-',':')
                        ) AS DATETIME) AS datetime
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_intraday_data, "$.value") AS INT64) AS heart_rate_value
        , SPLIT(REGEXP_EXTRACT(filename, "activities_heart_date_(.+?).json"), '_')[OFFSET(2)] AS intraday_interval
        
        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.activities-heart")) AS unnested_data
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.activities-heart-intraday['dataset']")) as unnested_intraday_data """,
    
    
    
#######################
'ce_sleep_daily_summary':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$['isMainSleep']") AS STRING) as is_main_sleep
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.timeInBed") AS INTEGER) as minute_in_bed #minute_in_bed_total
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAfterWakeup") AS INTEGER) as minute_after_wakeup
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAsleep") AS INTEGER) as minute_asleep #minute_asleep_total
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAwake") AS INTEGER) as minute_awake #minute_awake_total
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesToFallAsleep") AS INTEGER) as minute_to_fall_asleep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$['efficiency']") AS INTEGER) as efficiency
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['restless']['minutes']") AS INTEGER) as minute_restless
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['deep']['minutes']") AS INTEGER) as minute_deep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['light']['minutes']") AS INTEGER) as minute_light
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['rem']['minutes']") AS INTEGER) as minute_rem
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['wake']['minutes']") AS INTEGER) as minute_wake
       
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.logType") AS STRING) as log_type
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.type") AS STRING) as type

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        """ ,      
    
'ce_sleep_level':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$['isMainSleep']") AS STRING) as is_main_sleep
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.logId") AS NUMERIC) AS sleep_log_id
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_sleep_intraday, "$.level") AS STRING) as level
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_sleep_intraday, "$.dateTime") AS DATETIME) as start_datetime
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_sleep_intraday, "$.seconds") AS FLOAT64)/60 as duration_in_min
     
     FROM `{staging_table_id}`
     , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
     , UNNEST(JSON_EXTRACT_ARRAY(unnested_data, "$.levels['data']")) as unnested_sleep_intraday
        """,
    
'ce_sleep_daily_summary':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$['isMainSleep']") AS STRING) as is_main_sleep
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.logId") AS NUMERIC) AS sleep_log_id
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.timeInBed") AS INTEGER) as minute_in_bed #minute_in_bed_total
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesToFallAsleep") AS INTEGER) as minute_to_fall_asleep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAsleep") AS INTEGER) as minute_asleep #minute_asleep_total
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAfterWakeup") AS INTEGER) as minute_after_wakeup
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.minutesAwake") AS INTEGER) as minute_awake #minute_awake_total
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['restless']['minutes']") AS INTEGER) as minute_restless
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['deep']['minutes']") AS INTEGER) as minute_deep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['light']['minutes']") AS INTEGER) as minute_light
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['rem']['minutes']") AS INTEGER) as minute_rem
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['wake']['minutes']") AS INTEGER) as minute_wake

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        """ , 

'ce_sleep_daily_summary_counts':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.logId") AS NUMERIC) AS sleep_log_id
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['restless']['count']") AS INTEGER) as count_restless
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['asleep']['count']") AS INTEGER) as count_asleep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['awake']['count']") AS INTEGER) as count_awake
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['deep']['count']") AS INTEGER) as count_deep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['light']['count']") AS INTEGER) as count_light
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['rem']['count']") AS INTEGER) as count_rem
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['wake']['count']") AS INTEGER) as count_wake

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        """,

'ce_sleep_daily_summary_30dayavg':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.logId") AS NUMERIC) AS sleep_log_id
        
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['restless']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_restless
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['asleep']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_asleep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['awake']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_awake
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['deep']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_deep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['light']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_light
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['rem']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_rem
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.levels['summary']['wake']['thirtyDayAvgMinutes']") AS INTEGER) as thirty_day_avg_minutes_wake

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        """,

'ce_sleep_daily_summary_ext':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['dateOfSleep']") AS DATE) AS sleep_date
        , JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['isMainSleep']") AS is_main_sleep
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['logId']") AS NUMERIC) AS sleep_log_id
        , JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['logType']") AS sleep_log_type
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.summary['totalSleepRecords']") AS INTEGER) AS total_sleep_records
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.summary['totalMinutesAsleep']") AS INTEGER) AS total_minute_asleep
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.summary['totalTimeInBed']") AS INTEGER) AS total_time_in_bed
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['duration']") AS INTEGER) AS sleep_duration
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['efficiency']") AS INTEGER) AS sleep_efficiency
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['startTime']") AS DATETIME) AS sleep_start_time
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['endTime']") AS DATETIME) AS sleep_end_time
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['infoCode']") AS INTEGER) AS sleep_info_code
        , JSON_EXTRACT_SCALAR(unnested_data,"$.sleep['type']") AS sleep_type

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        """,

'ce_sleep_level_short':"""
    SELECT DISTINCT
        CAST(REGEXP_EXTRACT(filename, 'sleep/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.dateOfSleep") AS DATE) AS sleep_date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$['isMainSleep']") AS STRING) as is_main_sleep
        , CAST(JSON_EXTRACT_SCALAR(unnested_data,"$.logId") AS NUMERIC) AS sleep_log_id

        , SAFE_CAST(JSON_EXTRACT_SCALAR(short_data, "$.level") AS STRING) as level
        , SAFE_CAST(JSON_EXTRACT_SCALAR(short_data, "$.dateTime") AS DATETIME) as start_datetime
        , SAFE_CAST(JSON_EXTRACT_SCALAR(short_data, "$.seconds") AS FLOAT64)/60 as duration_in_min

        FROM `{staging_table_id}`
        , UNNEST(JSON_EXTRACT_ARRAY(data, "$.sleep")) as unnested_data
        , UNNEST(JSON_EXTRACT_ARRAY(unnested_data, "$.levels['shortData']")) as short_data
        """,

###############################
'ce_device':"""
    
    SELECT DISTINCT
        SAFE_CAST(REGEXP_EXTRACT(filename, 'FITBIT/devices/P([0-9]+)/') AS INT64) AS careevolution_id
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.id") AS STRING) as device_id
        , SAFE_CAST(REPLACE(SPLIT(SPLIT(filename, 'raw/health/')[OFFSET(1)], '/FITBIT')[OFFSET(0)], '/', '-') AS DATE) as upload_date
        
        , SAFE_CAST(SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.lastSyncTime") AS DATETIME) AS DATE) as date
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.battery") AS STRING) as battery
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.batteryLevel") AS FLOAT64) as battery_level
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.deviceVersion") AS STRING) as device_version
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.lastSyncTime") AS DATETIME) as last_sync_time
        , SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.type") AS STRING) as device_type
        
        #SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.features") AS STRING) as features,
        #SAFE_CAST(JSON_EXTRACT_SCALAR(unnested_data, "$.mac") AS STRING) as mac,
    
    FROM `{staging_table_id}` 
    , UNNEST(JSON_EXTRACT_ARRAY(data, "$.")) AS unnested_data 

 """,    
  
}

def parse_table_to_bigquery(query, dtype, column#, field_dtype
                            , staging_table_name, staging_by_dtype_table_name
                            , dataset_name = ce_dataset_name, project_id = cur_project_id
                           , disposition = 'WRITE_APPEND'):
    
    # Construct a BigQuery client object.
    
    staging_by_dtype_table_id = f'{project_id}.{dataset_name}.{staging_by_dtype_table_name}'
    staging_table_id = f'{project_id}.{dataset_name}.{staging_table_name}'
    
    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(destination = staging_by_dtype_table_id, write_disposition = disposition)

    # Start the query, passing in the extra configuration.
    query_job = client.query(query.format(staging_table_id = staging_table_id
                                          , dtype =dtype, column =column#, field_dtype =field_dtype
                                          , dataset_name = dataset_name), job_config=job_config)  # Make an API request.
    return query_job.result()  # Wait for the job to complete.


def load_table_to_bigquery_flex(query, destination_table_name
                            , dataset_name = ce_dataset_name, project_id = cur_project_id
                           , disposition = 'WRITE_APPEND'):
    
    # Construct a BigQuery client object.    
    destination_table_id = f'{project_id}.{dataset_name}.{destination_table_name}'    
    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(destination = destination_table_id, write_disposition = disposition
                                        , schema_update_options='ALLOW_FIELD_ADDITION'
                                        )
                                    
    # Start the query, passing in the extra configuration.
    query_job = client.query(query.format(dataset_name = dataset_name), job_config=job_config)  # Make an API request.
    
    return query_job.result()  # Wait for the job to complete.


def create_final_activity_table(final_ce_daily_activity_summary_table, join = 'FULL JOIN',dataset_name = ce_dataset_name):

#HH: re-wrote each of the queries to have a partition ordered by max date on the individual staging tables
    ce_query1 = """ 
        SELECT DISTINCT careevolution_id, date, activity_calories, calories_bmr, calories_out, elevation

        FROM (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date, 
                    CAST(activity_calories AS FLOAT64) as activity_calories,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_activityCalories`
            WHERE activity_calories is not null
            ORDER BY upload_date DESC) a1
            WHERE rn = 1
        )

        {join}                      
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(calories_bmr AS FLOAT64) as calories_bmr,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_caloriesBMR`
            WHERE calories_bmr is not null
            ORDER BY upload_date DESC) a2
            WHERE rn = 1)
            USING(careevolution_id, date)

        {join}                      
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(calories_out AS FLOAT64) as calories_out,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_calories`
            WHERE calories_out is not null
            ORDER BY upload_date DESC) a3
            WHERE rn = 1)
            USING(careevolution_id, date)
            
        {join} 
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(elevation AS FLOAT64) as elevation,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_elevation`
            WHERE elevation is not null
            ORDER BY upload_date DESC) a4
            WHERE rn = 1)
            USING(careevolution_id, date)


    """.format(dataset_name = dataset_name, join = join)

    start = datetime.now()
    print(start)
    load_table_to_bigquery_flex(query = ce_query1, destination_table_name=f'{final_ce_daily_activity_summary_table}_1')

    end = datetime.now()
    total_time = end-start
    print(f'Total time for first merges: {total_time}\n')  

    ce_query2 = """ 
        SELECT DISTINCT careevolution_id, date, floors, fairly_active_minutes, lightly_active_minutes

        FROM          
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(floors AS INTEGER) as floors,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_floors`
            WHERE floors is not null
            ORDER BY upload_date DESC) a5
            WHERE rn = 1 
            )
        {join}                      
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(fairly_active_minutes AS FLOAT64) as fairly_active_minutes,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_minutesFairlyActive`
            WHERE fairly_active_minutes is not null
            ORDER BY upload_date DESC) a6
            WHERE rn = 1)
            USING(careevolution_id, date)

        {join}                      
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(lightly_active_minutes AS FLOAT64) as lightly_active_minutes,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_minutesLightlyActive`
            WHERE lightly_active_minutes is not null
            ORDER BY upload_date DESC) a7
            WHERE rn = 1)
            USING(careevolution_id, date)


    """.format(dataset_name = dataset_name, join = join)

    start = datetime.now()
    print(start)
    load_table_to_bigquery_flex(query = ce_query2, destination_table_name=f'{final_ce_daily_activity_summary_table}_2')

    end = datetime.now()
    total_time = end-start
    print(f'Total time for second merges: {total_time}\n')

    ce_query3 = """ 
        SELECT DISTINCT careevolution_id, date, sedentary_minutes, very_active_minutes, steps

        FROM 
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(sedentary_minutes AS FLOAT64) as sedentary_minutes,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_minutesSedentary`
            WHERE sedentary_minutes is not null
            ORDER BY upload_date DESC) a
            WHERE rn = 1
            )
            
        {join} 
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(very_active_minutes AS FLOAT64) as very_active_minutes,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_minutesVeryActive`
            WHERE very_active_minutes is not null
            ORDER BY upload_date DESC) a
            WHERE rn = 1)
            USING(careevolution_id, date)

        {join} 
            (
            SELECT * EXCEPT (rn, max_date)
            FROM (
                SELECT DISTINCT careevolution_id, upload_date, max_date, date
                    , CAST(steps AS INTEGER) as steps,
                    ROW_NUMBER() OVER(PARTITION BY careevolution_id, date ORDER BY upload_date DESC, max_date DESC) AS rn
            FROM `{dataset_name}.ce_staging_steps`
            WHERE steps is not null
            ORDER BY upload_date DESC) a
            WHERE rn = 1)
            USING(careevolution_id, date)

    """.format(dataset_name = dataset_name, join = join)

    start = datetime.now()
    print(start)
    load_table_to_bigquery_flex(query = ce_query3, destination_table_name=f'{final_ce_daily_activity_summary_table}_3')

    end = datetime.now()
    total_time = end-start
    print(f'Total time for 3rd merges: {total_time}\n')

    print(f'Truncating previous {final_ce_daily_activity_summary_table} and replacing with new one, with new `coalesced` data')
    delete_table(f'{final_ce_daily_activity_summary_table}')

    ce_query = f""" 
        SELECT DISTINCT *, SAFE_CAST(NULL AS FLOAT64) as marginal_calories        
        FROM `{dataset_name}.{final_ce_daily_activity_summary_table}_1`
        {join} `{dataset_name}.{final_ce_daily_activity_summary_table}_2` USING(careevolution_id, date)
        {join} `{dataset_name}.{final_ce_daily_activity_summary_table}_3` USING(careevolution_id, date)

    """.format(dataset_name = dataset_name, join = join)

    start = datetime.now()
    print(start)
    load_table_to_bigquery_flex(query = ce_query, destination_table_name=f'{final_ce_daily_activity_summary_table}')

    end = datetime.now()
    total_time = end-start
    print(f'Total time for final table: {total_time}\n')

    print(f'Deleting intermediate tables: {final_ce_daily_activity_summary_table}_1, {final_ce_daily_activity_summary_table}_2, and {final_ce_daily_activity_summary_table}_3\n')

    delete_table(f'{final_ce_daily_activity_summary_table}_1')
    delete_table(f'{final_ce_daily_activity_summary_table}_2')
    delete_table(f'{final_ce_daily_activity_summary_table}_3')

    print(f'Final Table {final_ce_daily_activity_summary_table} Ready.\n')


def parse_all_to_final_tables(main_table, query, dtype, column, staging_table_name, dataset_name = ce_dataset_name, project_id = cur_project_id):
    
    # PARSING
    print(f'     in {project_id}.{dataset_name}')
    if main_table == 'ce_activity_summary':
        staging_by_dtype_table_name = dtype.split('-')[-1]
        staging_by_dtype_table_name = f'ce_staging_{staging_by_dtype_table_name}'
                    
        print(f'      --> {staging_by_dtype_table_name}')
        parse_table_to_bigquery(query = query, dtype = dtype, column = column, staging_table_name = staging_table_name
                                , staging_by_dtype_table_name = staging_by_dtype_table_name)               
                    
    else:              
        if main_table in ('ce_heart_rate_summary'):
            staging_by_dtype_table_name = main_table
            print(f'      --> {staging_by_dtype_table_name}')
            parse_table_to_bigquery(query = queries[main_table], dtype = dtype, column = column, staging_table_name = staging_table_name
                                    , staging_by_dtype_table_name = staging_by_dtype_table_name, dataset_name = dataset_name, project_id = project_id)

            print(f'      --> ce_heart_rate_second_level')
            parse_table_to_bigquery(query = queries['ce_heart_rate_second_level'], dtype = dtype, column = column, staging_table_name = staging_table_name
                                    , staging_by_dtype_table_name = 'ce_heart_rate_second_level', dataset_name = dataset_name, project_id = project_id)
        else:
            if main_table in ('ce_sleep_daily_summary'):
                staging_by_dtype_table_name = main_table
                print(f'      --> {staging_by_dtype_table_name}')
                parse_table_to_bigquery(query = queries[main_table], dtype = dtype, column = column, staging_table_name = staging_table_name
                                        , staging_by_dtype_table_name = staging_by_dtype_table_name, dataset_name = dataset_name, project_id = project_id)

                print(f'      --> ce_sleep_level')
                parse_table_to_bigquery(query = queries['ce_sleep_level'], dtype = dtype, column = column, staging_table_name = staging_table_name
                                        , staging_by_dtype_table_name = 'ce_sleep_level', dataset_name = dataset_name, project_id = project_id)

                print(f'      --> ce_sleep_daily_summary_counts')
                parse_table_to_bigquery(query = queries['ce_sleep_daily_summary_counts'], dtype = dtype, column = column, staging_table_name = staging_table_name, staging_by_dtype_table_name = 'ce_sleep_daily_summary_counts', dataset_name = dataset_name, project_id = project_id)

                print(f'      --> ce_sleep_daily_summary_30dayavg')
                parse_table_to_bigquery(query = queries['ce_sleep_daily_summary_30dayavg'], dtype = dtype, column = column, staging_table_name = staging_table_name, staging_by_dtype_table_name = 'ce_sleep_daily_summary_30dayavg', dataset_name = dataset_name, project_id = project_id)

                print(f'      --> ce_sleep_daily_summary_ext')
                parse_table_to_bigquery(query = queries['ce_sleep_daily_summary_ext'], dtype = dtype, column = column, staging_table_name = staging_table_name, staging_by_dtype_table_name = 'ce_sleep_daily_summary_ext', dataset_name = dataset_name, project_id = project_id)

                print(f'      --> ce_sleep_level_short')
                parse_table_to_bigquery(query = queries['ce_sleep_level_short'], dtype = dtype, column = column, staging_table_name = staging_table_name
                                        , staging_by_dtype_table_name = 'ce_sleep_level_short', dataset_name = dataset_name, project_id = project_id)

            else:
                staging_by_dtype_table_name = main_table
                print(f'      --> {staging_by_dtype_table_name}')
                parse_table_to_bigquery(query = queries[main_table], dtype = dtype, column = column#, field_dtype = field_dtype, staging_table_name = staging_table_name, staging_by_dtype_table_name = staging_by_dtype_table_name, dataset_name = dataset_name, project_id = project_id)



def run_etl(upload_date, datatype_uris, queries = queries):
    
    print(f'\nUpload date: {upload_date}')
    print(datetime.now())
    for main_table in datatype_uris:
        
        query = queries[main_table]
        federated_table_name = f'dev_{main_table}'.replace('ce_dev_ce','ce_dev')
        staging_table_name = f'ce_staging_{main_table}'.replace('ce_staging_ce','ce_staging')
        print(f' {federated_table_name} --> {staging_table_name}')

        for dtype in datatype_uris[main_table]:
            for column in datatype_uris[main_table][dtype]:
                uri = datatype_uris[main_table][dtype][column]
                uri = uri.format(upload_date = upload_date.replace('-','/'))
                
                # STAGING               
                create_federated_table(federated_table_name = federated_table_name, uris = uri)                
                
                load_federated_table_to_bigquery(federated_table_name = federated_table_name
                                                 , staging_table_name = staging_table_name)

                # PARSING
                parse_all_to_final_tables(main_table, query, dtype, column, staging_table_name)




