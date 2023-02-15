with tb_settings as (
  select
    current_timestamp() - interval 7 day as search_start_time,
    current_timestamp() as search_finish_time, 
    600 as bucket_time ,
    1000 as search_maximum_bytes_processed,
    10000 as search_maximum_waiting_ms, 
    100 as check_elapsed_ms, 
    10 as check_average_slot_usage, 
    10000 as check_bytes_spilled_to_disk, 
    [100000,500000,1000000,5000000,10000000,50000000,100000000,500000000,99999999999] as bytes_processed_bucket,
    ['DONE'] as check_state
), 
tb_jobs as (
  select
    creation_time, 
    job_id, 
    job_stages, 
    job_type, 
    priority, 
    project_id, 
    query, 
    start_time, 
    end_time,
    state, 
    statement_type, 
    timeline, 
    total_bytes_processed, 
  from 
    -- `region-US`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
    `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
  where
    1=1
    and creation_time between search_start_time and search_finish_time
    and state in unnest(check_state)
),
tb_job_elapsedtime_processedbytes as (
select 
  total_bytes_processed, 
  range_bucket(total_bytes_processed, bytes_processed_bucket) as bucket_index,
  extract(MILLISECOND FROM end_time - start_time) as elapsed_time,
  job_type,
  statement_type
from
  tb_jobs, tb_settings
),
tb_stat as (
select 
  bucket_index,
  any_value(p0_elapsed_time) as p0_elapsed_time,
  any_value(p25_elapsed_time) as p25_elapsed_time,
  any_value(p50_elapsed_time) as p50_elapsed_time,
  any_value(p75_elapsed_time) as p75_elapsed_time,
  any_value(p100_elapsed_time) as p100_elapsed_time
from (
  select 
    bucket_index,
    PERCENTILE_CONT(elapsed_time,0) over (partition by bucket_index) as p0_elapsed_time,
    PERCENTILE_CONT(elapsed_time,0.25) over (partition by bucket_index) as p25_elapsed_time,
    PERCENTILE_CONT(elapsed_time,0.5) over (partition by bucket_index) as p50_elapsed_time,
    PERCENTILE_CONT(elapsed_time,0.75) over (partition by bucket_index) as p75_elapsed_time,
    PERCENTILE_CONT(elapsed_time,1) over (partition by bucket_index) as p100_elapsed_time,
  from 
    tb_job_elapsedtime_processedbytes, tb_settings
)
group by 1
order by 1
)
select
  bytes_processed_bucket[offset(bucket_index)] as bucket_bytes_processed,
  p0_elapsed_time,
  p25_elapsed_time, 
  p50_elapsed_time,
  p75_elapsed_time,
  p100_elapsed_time
from
  tb_stat, tb_settings
order by 1
-- If you want to retrieve raw dataset,
-- Remakr above last 'select' statement and unremark the below select
-- select
--   bytes_processed_bucket[offset(bucket_index)] as bucket_bytes_processed,
--   elapsed_time, 
--   job_type,
--   statement_type
-- from 
--   tb_job_elapsedtime_processedbytes  
