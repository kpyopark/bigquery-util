# bigquery-util

## [badquery3](badquery3.sql)

The sql script - badquery3 - can help you see the average slot utilization per project. 

In this script, you MUST modify the following keywords to match your project environment. 

- <<master-project>> in line 175, 177: This value should be the master project id in your organization. All reservations and assignment meta are stored in the master project's information schema in BigQuery. 

[How to use it ?]

This script can support the various granularity of time frame of your slot utilization. 
At first, you can modify the following values in 'tb_settings' CTE.

- search_start_time : if you want to expand search range, you just modify this value such like this. "current_timestamp() - interval 7 day" -> "current_timestamp() - interval 30 day"
- search_finish_time : if you want to reduce search range, you just modify this value such like this. "current_timestamp()" -> "current_timestamp() - interval 1 day"
- bucket_time : This values means granularity of timefame. (unit is second). If you set this value was 3600, the all values(utilization, slots and etc) would be calculated under 1 hour time frame. 
