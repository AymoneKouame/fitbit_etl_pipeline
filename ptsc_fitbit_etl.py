from google.cloud import bigquery, storage
import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from datetime import datetime
import gcsfs
import json
import orjson
#import ijson
import os
import dask.dataframe as dd

def check_fitbit_data(check_what, add_condition = '', project_id = cur_project_id, dataset_name = ptsc_dataset_name):
    check_df = pd.read_gbq(f''' 

    WITH data as (
        SELECT {check_what}, 'activity_summary' as cat 
        FROM `{project_id}.{dataset_name}.activity_summary`
        UNION ALL
        SELECT {check_what}, 'heart_rate_summary' as cat  
        FROM `{project_id}.{dataset_name}.heart_rate_summary`
        UNION ALL
        SELECT {check_what}, 'heart_rate_minute_level' as cat  
        FROM `{project_id}.{dataset_name}.heart_rate_minute_level`
        UNION ALL
        SELECT {check_what}, 'sleep_level' as cat  
        FROM `{project_id}.{dataset_name}.sleep_level`
        UNION ALL
        SELECT {check_what}, 'steps_intraday' as cat  
        FROM `{project_id}.{dataset_name}.steps_intraday`
        UNION ALL
        SELECT {check_what}, 'sleep_level_short' as cat  
        FROM `{project_id}.{dataset_name}.sleep_level_short`
        UNION ALL
        SELECT {check_what}, 'sleep_daily_summary' as cat  
        FROM `{project_id}.{dataset_name}.sleep_daily_summary`
        UNION ALL
        SELECT {check_what}, 'sleep_daily_summary_30dayavg' as cat  
        FROM `{project_id}.{dataset_name}.sleep_daily_summary_30dayavg`
        UNION ALL
        SELECT {check_what}, 'sleep_daily_summary_counts' as cat  
        FROM `{project_id}.{dataset_name}.sleep_daily_summary_counts`
        UNION ALL
        SELECT {check_what}, 'sleep_daily_summary_ext' as cat  
        FROM `{project_id}.{dataset_name}.sleep_daily_summary_ext`

        )
    SELECT * FROM data {add_condition}
        ''').sort_values(['cat'])
    return check_df


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

def create_federated_table(federated_table_name, uris, maxBadRecords = 0
                           , dataset_name = ptsc_dataset_name, project_id=cur_project_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_name).table(federated_table_name)
    table = bigquery.Table(table_ref)
    extconfig = bigquery.ExternalConfig('CSV')
    extconfig.schema = [bigquery.SchemaField('data', 'STRING')]
    extconfig.options.autodetect = False
    extconfig.options.field_delimiter = '|' #u'\u00ff'
#     extconfig.options.quote_character = ''
    # extconfig.compression = 'GZIP'
    extconfig.options.allow_jagged_rows = True
    extconfig.options.ignore_unknown_values = True
    extconfig.options.allow_quoted_newlines = True
    extconfig.max_bad_records = maxBadRecords
    extconfig.source_uris = uris
    table.external_data_configuration = extconfig
    bq_client.delete_table(table, not_found_ok=True) 
    bq_client.create_table(table)


def delete_table(table, dataset_name = ptsc_dataset_name, project_id=cur_project_id):
    bq_client = bigquery.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_name).table(table)
    table = bigquery.Table(table_ref)
    bq_client.delete_table(table, not_found_ok=True) 


def load_federated_table_to_bigquery(federated_table_id, destination_table_id, project_id = cur_project_id):

    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(destination=destination_table_id, write_disposition = 'WRITE_APPEND')
    
    sql = """
        SELECT
            *,
            _FILE_NAME as filename
        FROM `{federated_table_id}`
    """
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql.format(federated_table_id=federated_table_id), 
                             job_config=job_config)  # Make an API request.
    return query_job.result()  # Wait for the job to complete.


def parse_table_to_bigquery(sql_query, destination_table_id, project_id = cur_project_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(destination= destination_table_id, 
                                         write_disposition = 'WRITE_APPEND',
                                         schema_update_options = 'ALLOW_FIELD_ADDITION')
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql_query, job_config=job_config)  # Make an API request.
    return query_job.result()  # Wait for the job to complete.


src_table_id = {
    'activity_summary': 'staging_daily_activity_summary',
    'heart_rate_minute_level': 'staging_heartrate',
    'heart_rate_summary': 'staging_heartrate',
    'steps_intraday': 'staging_intraday_steps',
    'sleep_level': 'staging_daily_sleep_summary',
    'sleep_daily_summary': 'staging_daily_sleep_summary',
    'sleep_daily_summary_counts': 'staging_daily_sleep_summary',
    'sleep_daily_summary_30dayavg': 'staging_daily_sleep_summary',
    'sleep_daily_summary_ext': 'staging_daily_sleep_summary',
    'sleep_level_short': 'staging_daily_sleep_summary'
}

queries = {
'activity_summary': """
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) AS vibrent_id,
    SAFE_CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    SAFE_CAST(REGEXP_EXTRACT(filename, '/([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})[T|-]?') AS DATE) as date,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, "$.summary['activityCalories']") AS FLOAT64) as activity_calories,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['caloriesBMR']") AS FLOAT64) as calories_bmr,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['caloriesOut']") AS FLOAT64) as calories_out,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['elevation']") AS FLOAT64) as elevation,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['fairlyActiveMinutes']") AS FLOAT64) as fairly_active_minutes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['floors']") AS INT64) as floors,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['lightlyActiveMinutes']") AS FLOAT64) as lightly_active_minutes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['marginalCalories']") AS FLOAT64) as marginal_calories,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['sedentaryMinutes']") AS FLOAT64) as sedentary_minutes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['steps']") AS INT64) as steps,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['veryActiveMinutes']") AS FLOAT64) as very_active_minutes
FROM `{dataset}.staging_daily_activity_summary`""", 

'heart_rate_minute_level': """
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'VITAL_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    SAFE_CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    DATETIME(CAST(JSON_EXTRACT_SCALAR(data,"$.activities-heart[0]['dateTime']") as DATE)
    , CAST(JSON_EXTRACT_SCALAR(params ,"$.time") AS TIME)) as datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(params,"$.value") AS INT64) as heart_rate_value
FROM `{dataset}.staging_heartrate`, unnest(JSON_EXTRACT_ARRAY(data,"$.activities-heart-intraday['dataset']")) as params""", 


'heart_rate_summary': """
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'VITAL_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.activities-heart[0]['dateTime']") AS DATE) AS date,
    JSON_EXTRACT_SCALAR(zone,"$.name") as zone_name,
    SAFE_CAST(JSON_EXTRACT_SCALAR(zone,"$.min") AS INT64) as min_heart_rate,
    SAFE_CAST(JSON_EXTRACT_SCALAR(zone,"$.max") AS INT64) as max_heart_rate,
    SAFE_CAST(JSON_EXTRACT_SCALAR(zone,"$.minutes") AS INT64) as minute_in_zone,
    SAFE_CAST(JSON_EXTRACT_SCALAR(zone,"$.caloriesOut") AS FLOAT64) as calorie_count
FROM `{dataset}.staging_heartrate`, 
unnest(JSON_EXTRACT_ARRAY(data, "$.activities-heart[0]['value']['heartRateZones']")) as zone""",


'steps_intraday': """
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) AS vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    DATETIME(CAST(JSON_EXTRACT_SCALAR(data,"$.activities-steps[0]['dateTime']") as DATE), CAST(JSON_EXTRACT_SCALAR(params ,"$.time") AS TIME)) as datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(params,"$.value") AS NUMERIC) as steps
FROM `{dataset}.staging_intraday_steps`, 
unnest(JSON_EXTRACT_ARRAY(data,"$.activities-steps-intraday['dataset']")) as params""",


'sleep_level': """
-- first sleep data
with first_long_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS minute_in_bed,
    JSON_EXTRACT_SCALAR(level,"$.level") as level,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.dateTime") AS DATETIME) as start_datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.seconds") AS FLOAT64)/60 as duration_in_min
FROM `{dataset}.staging_daily_sleep_summary`,
unnest(JSON_EXTRACT_ARRAY(data, "$.sleep[0]['levels']['data']")) as level
),

-- second sleep data
second_long_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS minute_in_bed,
    JSON_EXTRACT_SCALAR(level,"$.level") as level,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.dateTime") AS DATETIME) as start_datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.seconds") AS FLOAT64)/60 as duration_in_min
FROM `{dataset}.staging_daily_sleep_summary`,
unnest(JSON_EXTRACT_ARRAY(data, "$.sleep[1]['levels']['data']")) as level
),

long_data AS (
SELECT *
FROM first_long_data
UNION ALL
SELECT *
FROM second_long_data
)

-- remove duplicates
SELECT * EXCEPT(rank, minute_in_bed)
FROM (
    SELECT * , rank() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id ORDER BY minute_in_bed DESC)  AS rank
    FROM long_data
    )
WHERE rank = 1
AND start_datetime IS NOT NULL
AND sleep_date IS NOT NULL
""",
    
    
'sleep_daily_summary': """
WITH first_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['timeInBed']") AS INTEGER) AS minute_in_bed,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['minutesToFallAsleep']") AS INTEGER) AS minute_to_fall_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['minutesAsleep']") AS INTEGER) AS minute_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['minutesAfterWakeup']") AS INTEGER) AS minute_after_wakeup,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['minutesAwake']") AS INTEGER) AS minute_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['restless']['minutes']") AS INTEGER) AS minute_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['deep']['minutes']") AS INTEGER) AS minute_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['light']['minutes']") AS INTEGER) AS minute_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['rem']['minutes']") AS INTEGER) AS minute_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['wake']['minutes']") AS INTEGER) AS minute_wake

FROM `{dataset}.staging_daily_sleep_summary`
),

second_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['timeInBed']") AS INTEGER) AS minute_in_bed,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['minutesToFallAsleep']") AS INTEGER) AS minute_to_fall_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['minutesAsleep']") AS INTEGER) AS minute_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['minutesAfterWakeup']") AS INTEGER) AS minute_after_wakeup,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['minutesAwake']") AS INTEGER) AS minute_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['restless']['minutes']") AS INTEGER) AS minute_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['deep']['minutes']") AS INTEGER) AS minute_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['light']['minutes']") AS INTEGER) AS minute_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['rem']['minutes']") AS INTEGER) AS minute_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['wake']['minutes']") AS INTEGER) AS minute_wake
FROM `{dataset}.staging_daily_sleep_summary`
),

long_data AS (
    SELECT * FROM first_sleep_data
    UNION ALL 
    SELECT * FROM second_sleep_data
)

SELECT * EXCEPT(rank)
FROM (
    SELECT * , RANK() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id, is_main_sleep ORDER BY minute_in_bed DESC) AS rank
    FROM long_data
    )
WHERE rank = 1
AND sleep_date IS NOT NULL
""",


'sleep_daily_summary_counts': """
WITH first_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['restless']['count']") AS INTEGER) AS count_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['asleep']['count']") AS INTEGER) AS count_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['awake']['count']") AS INTEGER) AS count_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['deep']['count']") AS INTEGER) AS count_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['light']['count']") AS INTEGER) AS count_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['rem']['count']") AS INTEGER) AS count_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['wake']['count']") AS INTEGER) AS count_wake

FROM `{dataset}.staging_daily_sleep_summary`
),

second_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['restless']['count']") AS INTEGER) AS count_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['asleep']['count']") AS INTEGER) AS count_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['awake']['count']") AS INTEGER) AS count_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['deep']['count']") AS INTEGER) AS count_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['light']['count']") AS INTEGER) AS count_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['rem']['count']") AS INTEGER) AS count_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['wake']['count']") AS INTEGER) AS count_wake
    
FROM `{dataset}.staging_daily_sleep_summary`
),

long_data AS (
    SELECT * FROM first_sleep_data
    UNION ALL 
    SELECT * FROM second_sleep_data
)

SELECT * EXCEPT(rank)
FROM (
    SELECT * , RANK() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id ORDER BY count_deep DESC, count_light DESC) AS rank
    FROM long_data
    )
WHERE rank = 1
AND sleep_date IS NOT NULL
""",


'sleep_daily_summary_30dayavg': """
WITH first_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['restless']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['asleep']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['awake']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['deep']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['light']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['rem']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['levels']['summary']['wake']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_wake

FROM `{dataset}.staging_daily_sleep_summary`
),

second_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['restless']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_restless,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['asleep']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['awake']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_awake,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['deep']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_deep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['light']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_light,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['rem']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_rem,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['levels']['summary']['wake']['thirtyDayAvgMinutes']") AS INTEGER) AS thirty_day_avg_minutes_wake
    
FROM `{dataset}.staging_daily_sleep_summary`
),

long_data AS (
    SELECT * FROM first_sleep_data
    UNION ALL 
    SELECT * FROM second_sleep_data
)

SELECT * EXCEPT(rank)
FROM (
    SELECT * , RANK() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id ORDER BY thirty_day_avg_minutes_deep DESC, thirty_day_avg_minutes_light DESC) AS rank
    FROM long_data
    )
WHERE rank = 1
AND sleep_date IS NOT NULL
""",


'sleep_daily_summary_ext': """
WITH first_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logType']") AS sleep_log_type,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalSleepRecords']") AS INTEGER) AS total_sleep_records,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalMinutesAsleep']") AS INTEGER) AS total_minute_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS total_time_in_bed,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['duration']") AS INTEGER) AS sleep_duration,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['efficiency']") AS INTEGER) AS sleep_efficiency,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['startTime']") AS DATETIME) AS sleep_start_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['endTime']") AS DATETIME) AS sleep_end_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['infoCode']") AS INTEGER) AS sleep_info_code,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['type']") AS sleep_type

FROM `{dataset}.staging_daily_sleep_summary`
),

second_sleep_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logType']") AS sleep_log_type,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalSleepRecords']") AS INTEGER) AS total_sleep_records,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalMinutesAsleep']") AS INTEGER) AS total_minute_asleep,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS total_time_in_bed,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['duration']") AS INTEGER) AS sleep_duration,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['efficiency']") AS INTEGER) AS sleep_efficiency,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['startTime']") AS DATETIME) AS sleep_start_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['endTime']") AS DATETIME) AS sleep_end_time,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['infoCode']") AS INTEGER) AS sleep_info_code,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['type']") AS sleep_type
    
FROM `{dataset}.staging_daily_sleep_summary`
),

long_data AS (
    SELECT * FROM first_sleep_data
    UNION ALL 
    SELECT * FROM second_sleep_data
)

SELECT * EXCEPT(rank)
FROM (
    SELECT * , RANK() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id, is_main_sleep ORDER BY sleep_duration DESC) AS rank
    FROM long_data
    )
WHERE rank = 1
AND sleep_date IS NOT NULL
""",


'sleep_level_short': """
-- first sleep data
with first_long_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[0]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[0]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS minute_in_bed,
    JSON_EXTRACT_SCALAR(level,"$.level") AS level,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.dateTime") AS DATETIME) as start_datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.seconds") AS FLOAT64)/60 as duration_in_min
FROM `{dataset}.staging_daily_sleep_summary`,
unnest(JSON_EXTRACT_ARRAY(data, "$.sleep[0]['levels']['shortData']")) as level
),

-- second sleep data
second_long_data AS (
SELECT DISTINCT
    CAST(REGEXP_EXTRACT(filename, 'ACTIVITY_MEASUREMENTS/([0-9]+)/') AS INT64) as vibrent_id,
    CAST(REPLACE(REGEXP_EXTRACT(filename, 'health/([0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}})/'), '/', '-') AS DATE) as upload_date,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['dateOfSleep']") AS DATE) AS sleep_date,
    JSON_EXTRACT_SCALAR(data,"$.sleep[1]['isMainSleep']") AS is_main_sleep,
    CAST(JSON_EXTRACT_SCALAR(data,"$.sleep[1]['logId']") AS NUMERIC) AS sleep_log_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data,"$.summary['totalTimeInBed']") AS INTEGER) AS minute_in_bed,
    JSON_EXTRACT_SCALAR(level,"$.level") AS level,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.dateTime") AS DATETIME) as start_datetime,
    SAFE_CAST(JSON_EXTRACT_SCALAR(level,"$.seconds") AS FLOAT64)/60 as duration_in_min
FROM `{dataset}.staging_daily_sleep_summary`,
unnest(JSON_EXTRACT_ARRAY(data, "$.sleep[1]['levels']['shortData']")) as level
),

long_data AS (
SELECT *
FROM first_long_data
UNION ALL
SELECT *
FROM second_long_data
)

-- remove duplicates
SELECT * EXCEPT(rank, minute_in_bed)
FROM (
    SELECT * , rank() OVER(PARTITION BY vibrent_id, upload_date, sleep_date, sleep_log_id ORDER BY minute_in_bed DESC)  AS rank
    FROM long_data
    )
WHERE rank = 1
AND start_datetime IS NOT NULL
AND sleep_date IS NOT NULL
"""

}

URIS = {
    'DAILY_ACTIVITY_SUMMARY': "gs://ptsc-health-data-prod/raw/health/{yyyy}/{mm}/{dd}/FITBIT/DAILY_ACTIVITY_SUMMARY/NA/ACTIVITY_MEASUREMENTS/*",
    'HEARTRATE': "gs://ptsc-health-data-prod/raw/health/{yyyy}/{mm}/{dd}/FITBIT/HEARTRATE/NA/VITAL_MEASUREMENTS/*",
    'DAILY_SLEEP_SUMMARY': "gs://ptsc-health-data-prod/raw/health/{yyyy}/{mm}/{dd}/FITBIT/DAILY_SLEEP_SUMMARY/NA/ACTIVITY_MEASUREMENTS/*",
    'INTRADAY_STEPS': "gs://ptsc-health-data-prod/raw/health/{yyyy}/{mm}/{dd}/FITBIT/INTRADAY_STEPS/NA/ACTIVITY_MEASUREMENTS/*",
}

def cat_etl_variables(cat, project_id, dataset_name, URIS = URIS):
    #print("Variables for ", cat)
    cat_lower_case = cat.lower()
    federated_table_name = 'dev_' + cat_lower_case
    staging_table_name = 'staging_' + cat_lower_case
    federated_table_id = f"{project_id}.{dataset_name}.{federated_table_name}"
    staging_table_id = f"{project_id}.{dataset_name}.{staging_table_name}"

    return federated_table_name, federated_table_id, staging_table_name, staging_table_id


def upload_cat_year_month(cat:str, yyyy:str, MM:str, start_day:str, end_day="31", URIS = URIS
                          , maxBadRecordsAllowed = 0
                          , dataset_name= ptsc_dataset_name, project_id = cur_project_id): 
    #start_day = previous max upload date
    
    print("\nUploading ", cat)
    if maxBadRecordsAllowed!=0: print(f"(Allowing {maxBadRecordsAllowed} max bad records)")
        
    federated_table_name, federated_table_id, staging_table_name, staging_table_id \
        = cat_etl_variables(cat, project_id = project_id, dataset_name = dataset_name)
    print(federated_table_name,"-->", staging_table_name, f' (in {project_id}.{dataset_name})\n')
    delete_table(project_id = project_id, dataset_name = dataset_name, table = staging_table_name)
    
    add_str_mm = ''
    if end_day!='31': add_str_mm = add_str_mm+f' (from day {int(start_day)+1} to {int(end_day)})'
    print(' Uploading year', yyyy)
    for mm in [MM]:
        print("   month", mm, add_str_mm)
        for day in range(1, 32):
            dd = str(day).zfill(2)
            if dd <= start_day:
                continue
            if dd > end_day:
                break
                
            uris = URIS[cat].format(yyyy=yyyy, mm=mm, dd=dd)
            #print(uris)
            print("    day", dd)
            create_federated_table(dataset_name, federated_table_name, uris
                                   , maxBadRecords = maxBadRecordsAllowed)
            try:
                load_federated_table_to_bigquery(federated_table_id = federated_table_id
                                             , destination_table_id = staging_table_id)
            except exceptions.BadRequest as BadRequest:
                print(f'      BadRequest: {BadRequest.message}')
                #Initiate Alternative ETL.
                load_JSON_from_gcp_to_staging(cat = cat, upload_YYYY = yyyy, upload_MM = MM, upload_DD = dd)

    print(f'staging {cat} done.')


def parse_to_table(queries = queries, dataset_name = ptsc_dataset_name, src_table_id= src_table_id, project_id = cur_project_id
                  , staging_table_name = None, fix = False):
    
    def parse_to_table_v0(queries = queries, dataset_name = dataset_name, src_table_id= src_table_id, project_id = project_id):
        
        for table_name, query in queries.items():
            source_table_id = src_table_id[table_name]
            sql_query = query.format(dataset=dataset_name)
            destination_table_id = project_id + '.' + dataset_name + '.' + table_name
            print('\nDESTINATION:',destination_table_id)
            #print(f"""\nQUERY: {sql_query} """)
            print(f"Loading data: {source_table_id} --> {table_name}")
            result = parse_table_to_bigquery(sql_query, destination_table_id, project_id = project_id)
            print(result)


    if fix == False: parse_to_table_v0()

    elif fix == True:
        '''Some of the JSON are not being parsed because there are a bunch of 
         -1 empty spaces between the key/values that the SQL does not like (one reason for sure - tested)
         -2 the double quotations are changes to single quotation (possibly a reason, not sure)
         -3 a value was changed from 'true' to 'True'(one reason for sure - tested)'''

        print('  Fixing staging table before parsing...')
        staging_table_id = project_id + '.' + dataset_name + '.' + staging_table_name
        client = bigquery.Client(project=project_id)
        fix_staging_sql = f"""SELECT replace(replace(replace(replace(data, ' ',''), "'",'''"'''), 'True','true'), 'False','false') as data
                            , filename
                            FROM {staging_table_id}"""
        query_job = client.query(fix_staging_sql, job_config=bigquery.QueryJobConfig(destination=staging_table_id, write_disposition = 'WRITE_TRUNCATE'))
        query_job.result()
        
        print('  Done. parsing...')
        parse_to_table_v0()


