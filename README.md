# ETL Pipeline - Data Flow
This ETL process assumes inputs are JSON Files loacated in a Google Cloud Bucket. Files are delivered via a Google Bucket in the RDR. One file = data for one data type for one day for one participant in .json format. 

## ETL Process Summary
 - 1 Import all necessary libraries
 - 2 Create a Federated table with 'CSV' external config based on the JSON file uris (location path in GC) (the federated table will be deleted/overwritten for each new batch of files)
 - 3 Batch load the federated table to a staging table, ensuring usage of pseudo column _FILE_NAME in GBQ SQL (the staging table will be deleted/overwritten for each new batch of files)
 - 4 Using GBQ parse the JSON file into tables following the project requirements
 - 5 Load the final tables to Google Big Query (the final tables will be appended with each new batch of parsed data)
   
## Running the ET pipeline example.
```URIS = {
    'DAILY_ACTIVITY_SUMMARY': "gs://fake_bucket_id/{yyyy}/{mm}/{dd}/FITBIT/DAILY_ACTIVITY_SUMMARY/NA/ACTIVITY_MEASUREMENTS/*",
    'HEARTRATE': "gs://fake_bucket_id/{yyyy}/{mm}/{dd}/FITBIT/HEARTRATE/NA/VITAL_MEASUREMENTS/*",
    'DAILY_SLEEP_SUMMARY': "gs://fake_bucket_id/{yyyy}/{mm}/{dd}/FITBIT/DAILY_SLEEP_SUMMARY/NA/ACTIVITY_MEASUREMENTS/*",
    'INTRADAY_STEPS': "gs://fake_bucket_id/{yyyy}/{mm}/{dd}/FITBIT/INTRADAY_STEPS/NA/ACTIVITY_MEASUREMENTS/*",
}

for MM in [str(m).zfill(2) for m in list(range(1,13))]:
    for cat in URIS: 
        upload_cat_year_month(cat = cat, yyyy = '2025', MM = MM, start_day= '00')
    parse_to_table()```


## Results
The following JSON file (**synthetic**) located in the Google bucket fake_bucket_id', will return two tables in Google Big Query:

####Input
(filename = gs://fake_bucket_id/2025/09/02/FITBIT/DAILY_SLEEP_SUMMARY/NA/ACTIVITY_MEASUREMENTS/522222/2025-09-01-00.00.00_2025-09-01-23.59.59-cd9ec7a8af7f97byy3jy52a4d796cu83.JSON)

{'sleep': [{'dateOfSleep': '2025-09-01',
   'duration': 29340056,
   'efficiency': 93,
   'endTime': '2025-09-01T09:29:00.000',
   'infoCode': 0,
   'isMainSleep': True,
   'levels': {'data': [{'dateTime': '2025-09-01T01:20:00.000','level': 'light','seconds': 9451},
     {'dateTime': '2025-09-01T02:01:30.000', 'level': 'deep', 'seconds': 948},
     {'dateTime': '2025-09-01T02:09:30.000', 'level': 'light', 'seconds': 654},
     {'dateTime': '2025-09-01T02:21:30.000', 'level': 'deep', 'seconds': 1200},
     {'dateTime': '2025-09-01T02:39:30.000', 'level': 'light', 'seconds': 841},
     {'dateTime': '2025-09-01T02:52:30.000', 'level': 'rem', 'seconds': 324},
     {'dateTime': '2025-09-01T02:59:30.000',
      'level': 'light',
      'seconds': 4110},
     {'dateTime': '2025-09-01T04:08:00.000', 'level': 'wake', 'seconds': 1100},
     {'dateTime': '2025-09-01T04:27:30.000',
      'level': 'light',
      'seconds': 4110},
     {'dateTime': '2025-09-01T05:36:00.000', 'level': 'wake', 'seconds': 1364},
     {'dateTime': '2025-09-01T05:58:00.000',
      'level': 'light',
      'seconds': 2790},
     {'dateTime': '2025-09-01T06:44:30.000', 'level': 'rem', 'seconds': 200},
     {'dateTime': '2025-09-01T06:49:30.000', 'level': 'light', 'seconds': 879},
     {'dateTime': '2025-09-01T06:52:30.000', 'level': 'deep', 'seconds': 279},
     {'dateTime': '2025-09-01T06:57:00.000',
      'level': 'light',
      'seconds': 5790},
     {'dateTime': '2025-09-01T08:33:30.000', 'level': 'wake', 'seconds': 954},
     {'dateTime': '2025-09-01T08:44:30.000',
      'level': 'light',
      'seconds': 2670}],
    'shortData': [{'dateTime': '2025-09-01T01:23:00.000',
      'level': 'wake',
      'seconds': 120},
     {'dateTime': '2025-09-01T01:27:00.000', 'level': 'wake', 'seconds': 50},
     {'dateTime': '2025-09-01T02:12:00.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T02:39:00.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T03:19:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T04:57:30.000', 'level': 'wake', 'seconds': 89},
     {'dateTime': '2025-09-01T05:22:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T06:00:30.000', 'level': 'wake', 'seconds': 55},
     {'dateTime': '2025-09-01T06:04:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T06:07:00.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T06:09:00.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T06:19:00.000', 'level': 'wake', 'seconds': 89},
     {'dateTime': '2025-09-01T06:27:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T07:09:30.000', 'level': 'wake', 'seconds': 110},
     {'dateTime': '2025-09-01T07:51:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T08:50:30.000', 'level': 'wake', 'seconds': 175},
     {'dateTime': '2025-09-01T09:00:30.000', 'level': 'wake', 'seconds': 25},
     {'dateTime': '2025-09-01T09:06:00.000', 'level': 'wake', 'seconds': 110},
     {'dateTime': '2025-09-01T09:25:30.000', 'level': 'wake', 'seconds': 20}],
    'summary': {'deep': {'count': 2, 'minutes': 25, 'thirtyDayAvgMinutes': 16},
     'light': {'count': 27, 'minutes': 375, 'thirtyDayAvgMinutes': 400},
     'rem': {'count': 4, 'minutes': 12, 'thirtyDayAvgMinutes': 30},
     'wake': {'count': 21, 'minutes': 70, 'thirtyDayAvgMinutes': 50}}},
   'logId': 46565657789,
   'logType': 'auto_detected',
   'minutesAfterWakeup': 0,
   'minutesAsleep': 500,
   'minutesAwake': 71,
   'minutesToFallAsleep': 0,
   'startTime': '2025-09-01T01:20:00.000',
   'timeInBed': 369,
   'type': 'stages'}],
 'summary': {'stages': {'deep': 25, 'light': 286, 'rem': 15, 'wake': 69},
  'totalMinutesAsleep': 286,
  'totalSleepRecords': 1,
  'totalTimeInBed': 500}}

#### Output
 - Table 1
  <img width="975" height="79" alt="image" src="https://github.com/user-attachments/assets/bf794e86-c539-48e8-a72a-0e8c7047d5cb" /> <img width="492" height="67" alt="image" src="https://github.com/user-attachments/assets/55643156-d4bd-4214-b74f-10da0c387aae" />


 - Table 2
<img width="762" height="60" alt="image" src="https://github.com/user-attachments/assets/2bfce0d9-5126-439c-b4b4-05e06b056aac" />

