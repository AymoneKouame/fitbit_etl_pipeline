\# ETL Pipeline - Data Flow

This ETL process assumes inputs are JSON Files loacated in a Google Cloud Bucket. Files are delivered via a Google Bucket in the RDR. One file = data for one data type for one day for one participant in .json format. 



\## ETL Process Summary

&nbsp;- 1 Import all necessary libraries

&nbsp;- 2 Create a Federated table with 'CSV' external config based on the JSON file uris (location path in GC) (the federated table will be deleted/overwritten for each new batch of files)

&nbsp;- 3 Batch load the federated table to a staging table, ensuring usage of pseudo column \_FILE\_NAME in GBQ SQL (the staging table will be deleted/overwritten for each new batch of files)

&nbsp;- 4 Using GBQ parse the JSON file into tables following the project requirements

&nbsp;- 5 Load the final tables to Google Big Query (the final tables will be appended with each new batch of parsed data)

&nbsp;  

\## Running the ET pipeline example.



\#Code example

```URIS = {

&nbsp;   'DAILY\_ACTIVITY\_SUMMARY': "gs://fake\_bucket\_id/{yyyy}/{mm}/{dd}/FITBIT/DAILY\_ACTIVITY\_SUMMARY/NA/ACTIVITY\_MEASUREMENTS/\*",

&nbsp;   'HEARTRATE': "gs://fake\_bucket\_id/{yyyy}/{mm}/{dd}/FITBIT/HEARTRATE/NA/VITAL\_MEASUREMENTS/\*",

&nbsp;   'DAILY\_SLEEP\_SUMMARY': "gs://fake\_bucket\_id/{yyyy}/{mm}/{dd}/FITBIT/DAILY\_SLEEP\_SUMMARY/NA/ACTIVITY\_MEASUREMENTS/\*",

&nbsp;   'INTRADAY\_STEPS': "gs://fake\_bucket\_id/{yyyy}/{mm}/{dd}/FITBIT/INTRADAY\_STEPS/NA/ACTIVITY\_MEASUREMENTS/\*",

}



for MM in \[str(m).zfill(2) for m in list(range(1,13))]:

&nbsp;   for cat in URIS: 

&nbsp;       upload\_cat\_year\_month(cat = cat, yyyy = '2025', MM = MM, start\_day= '00')

&nbsp;   parse\_to\_table()



```





\## Results

The following JSON file (\*\*synthetic\*\*) located in the Google bucket fake\_bucket\_id', will return two tables in Google Big Query:



\#### Input

(filename = gs://fake\_bucket\_id/2025/09/02/FITBIT/DAILY\_SLEEP\_SUMMARY/NA/ACTIVITY\_MEASUREMENTS/522222/2025-09-01-00.00.00\_2025-09-01-23.59.59-cd9ec7a8af7f97byy3jy52a4d796cu83.JSON)



{'sleep': \[{'dateOfSleep': '2025-09-01',

&nbsp;  'duration': 29340056,

&nbsp;  'efficiency': 93,

&nbsp;  'endTime': '2025-09-01T09:29:00.000',

&nbsp;  'infoCode': 0,

&nbsp;  'isMainSleep': True,

&nbsp;  'levels': {'data': \[{'dateTime': '2025-09-01T01:20:00.000','level': 'light','seconds': 9451},

&nbsp;    {'dateTime': '2025-09-01T02:01:30.000', 'level': 'deep', 'seconds': 948},

&nbsp;    {'dateTime': '2025-09-01T02:09:30.000', 'level': 'light', 'seconds': 654},

&nbsp;    {'dateTime': '2025-09-01T02:21:30.000', 'level': 'deep', 'seconds': 1200},

&nbsp;    {'dateTime': '2025-09-01T02:39:30.000', 'level': 'light', 'seconds': 841},

&nbsp;    {'dateTime': '2025-09-01T02:52:30.000', 'level': 'rem', 'seconds': 324},

&nbsp;    {'dateTime': '2025-09-01T02:59:30.000',

&nbsp;     'level': 'light',

&nbsp;     'seconds': 4110},

&nbsp;    {'dateTime': '2025-09-01T04:08:00.000', 'level': 'wake', 'seconds': 1100},

&nbsp;    {'dateTime': '2025-09-01T04:27:30.000',

&nbsp;     'level': 'light',

&nbsp;     'seconds': 4110},

&nbsp;    {'dateTime': '2025-09-01T05:36:00.000', 'level': 'wake', 'seconds': 1364},

&nbsp;    {'dateTime': '2025-09-01T05:58:00.000',

&nbsp;     'level': 'light',

&nbsp;     'seconds': 2790},

&nbsp;    {'dateTime': '2025-09-01T06:44:30.000', 'level': 'rem', 'seconds': 200},

&nbsp;    {'dateTime': '2025-09-01T06:49:30.000', 'level': 'light', 'seconds': 879},

&nbsp;    {'dateTime': '2025-09-01T06:52:30.000', 'level': 'deep', 'seconds': 279},

&nbsp;    {'dateTime': '2025-09-01T06:57:00.000',

&nbsp;     'level': 'light',

&nbsp;     'seconds': 5790},

&nbsp;    {'dateTime': '2025-09-01T08:33:30.000', 'level': 'wake', 'seconds': 954},

&nbsp;    {'dateTime': '2025-09-01T08:44:30.000',

&nbsp;     'level': 'light',

&nbsp;     'seconds': 2670}],

&nbsp;   'shortData': \[{'dateTime': '2025-09-01T01:23:00.000',

&nbsp;     'level': 'wake',

&nbsp;     'seconds': 120},

&nbsp;    {'dateTime': '2025-09-01T01:27:00.000', 'level': 'wake', 'seconds': 50},

&nbsp;    {'dateTime': '2025-09-01T02:12:00.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T02:39:00.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T03:19:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T04:57:30.000', 'level': 'wake', 'seconds': 89},

&nbsp;    {'dateTime': '2025-09-01T05:22:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T06:00:30.000', 'level': 'wake', 'seconds': 55},

&nbsp;    {'dateTime': '2025-09-01T06:04:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T06:07:00.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T06:09:00.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T06:19:00.000', 'level': 'wake', 'seconds': 89},

&nbsp;    {'dateTime': '2025-09-01T06:27:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T07:09:30.000', 'level': 'wake', 'seconds': 110},

&nbsp;    {'dateTime': '2025-09-01T07:51:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T08:50:30.000', 'level': 'wake', 'seconds': 175},

&nbsp;    {'dateTime': '2025-09-01T09:00:30.000', 'level': 'wake', 'seconds': 25},

&nbsp;    {'dateTime': '2025-09-01T09:06:00.000', 'level': 'wake', 'seconds': 110},

&nbsp;    {'dateTime': '2025-09-01T09:25:30.000', 'level': 'wake', 'seconds': 20}],

&nbsp;   'summary': {'deep': {'count': 2, 'minutes': 25, 'thirtyDayAvgMinutes': 16},

&nbsp;    'light': {'count': 27, 'minutes': 375, 'thirtyDayAvgMinutes': 400},

&nbsp;    'rem': {'count': 4, 'minutes': 12, 'thirtyDayAvgMinutes': 30},

&nbsp;    'wake': {'count': 21, 'minutes': 70, 'thirtyDayAvgMinutes': 50}}},

&nbsp;  'logId': 46565657789,

&nbsp;  'logType': 'auto\_detected',

&nbsp;  'minutesAfterWakeup': 0,

&nbsp;  'minutesAsleep': 500,

&nbsp;  'minutesAwake': 71,

&nbsp;  'minutesToFallAsleep': 0,

&nbsp;  'startTime': '2025-09-01T01:20:00.000',

&nbsp;  'timeInBed': 369,

&nbsp;  'type': 'stages'}],

&nbsp;'summary': {'stages': {'deep': 25, 'light': 286, 'rem': 15, 'wake': 69},

&nbsp; 'totalMinutesAsleep': 286,

&nbsp; 'totalSleepRecords': 1,

&nbsp; 'totalTimeInBed': 500}}



\#### Output

&nbsp;- Table 1

&nbsp; <img width="975" height="79" alt="image" src="https://github.com/user-attachments/assets/bf794e86-c539-48e8-a72a-0e8c7047d5cb" /> <img width="492" height="67" alt="image" src="https://github.com/user-attachments/assets/55643156-d4bd-4214-b74f-10da0c387aae" />





&nbsp;- Table 2

<img width="762" height="60" alt="image" src="https://github.com/user-attachments/assets/2bfce0d9-5126-439c-b4b4-05e06b056aac" />





