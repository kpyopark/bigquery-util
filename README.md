# bigquery-util

## [elapsedtime_bytesprocessed](elapsedtime_bytesprocessed.sql)

This query shows the average elapsed_time per bytes_processed.

For example.

| Row	| bucket_bytes_processed | p0_elapsed_time | p25_elapsed_time | p50_elapsed_time | p75_elapsed_time | p100_elapsed_time |
|-----|------------------------|-----------------|------------------|------------------|------------------|-------------------|
|1|null|0.0|0.0|0.0|0.0|0.0|
|2|100000|29.0|58.0|62.0|66.0|754.0|
|3|1000000|538.0|545.0|594.0|650.0|655.0|
|4|500000000|162.0|412.0|618.5|777.5|903.0|
|5|99999999999|602.0|602.0|602.0|602.0|602.0|

## [badquery_monitoring](badquery_monitoring.sql)

This query shows the very resource consuming queries in the job list.

Before to use it, you must change the value of the fields in the CTE table(tb_settings).

- threshold_maximum_slot_ms : This value has very similar feature like the maximum_bytes_billed. It uses total_slot_ms instead of bytes_billed.
- threshold_elapsed_ms & threshold_average_slot_usage : If a query would consume slots in a certain period time with high average resource, it would be a bad query. 

## [badquery3](badquery3.sql)

The sql script - badquery3 - can help you see the average slot utilization per project. 

In this script, you MUST modify the following keywords to match your project environment. 

- `<<master-project>>` in line 175, 177: This value MUST be replaced with the master project id in your organization. All reservations and assignment information are stored in the master project's information schema in BigQuery. 

[How to use it ?]

This script can support the various granularity of time frame of your slot utilization. 
At first, you can modify the following values in 'tb_settings' CTE.

- search_start_time : if you want to expand search range, you just modify this value such like this. "current_timestamp() - interval 7 day" -> "current_timestamp() - interval 30 day"
- search_finish_time : if you want to reduce search range, you just modify this value such like this. "current_timestamp()" -> "current_timestamp() - interval 1 day"
- bucket_time : This values means granularity of timefame. (unit is second). If you set this value was 3600, the all values(utilization, slots and etc) would be calculated under 1 hour time frame. 
