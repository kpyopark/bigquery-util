with tb_settings as (
  select
    current_timestamp() - interval 31 day as search_start_time,
    current_timestamp() - interval 1 day as search_end_time, 
    60 as bucket_time , -- second
    ['DONE'] as check_state
), 
tb_interval as (
  select t0 as ctime_bucket
  from tb_settings ts, unnest(GENERATE_TIMESTAMP_ARRAY(timestamp_seconds(DIV(UNIX_SECONDS(ts.search_start_time), ts.bucket_time)* ts.bucket_time), timestamp_seconds(DIV(UNIX_SECONDS(ts.search_end_time), ts.bucket_time)*ts.bucket_time), interval ts.bucket_time second)) as t0
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
    state, 
    statement_type, 
    timeline, 
    total_bytes_processed, 
    total_slot_ms as finished_total_slot_ms
  from 
    `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
  where
    1=1
    and creation_time between search_start_time and search_end_time
    and state in unnest(check_state)
), 
tb_tlo as (
  select 
    job_id, 
    elapsed_ms,
    start_time,
    timestamp_diff(start_time, creation_time, MILLISECOND) as waiting_ms,
    datetime_add(start_time, interval ifnull(elapsed_ms,0) millisecond) as etime,
    ifnull(lag(elapsed_ms) over (partition by job_id order by elapsed_ms),0) as prev_elapsed_ms,
    total_slot_ms, 
    ifnull(lag(total_slot_ms) over (partition by job_id order by elapsed_ms),0) as prev_total_slot_ms,
    safe_divide(total_slot_ms, elapsed_ms) as average_slot_usage,
    pending_units, 
    completed_units, 
    active_units,
  from 
    tb_jobs tbo, unnest(timeline) tl, tb_settings
),
tb_tloe as (
  select
    tlo.elapsed_ms,
    start_time,
    waiting_ms,
    etime,
    lag(etime) over (partition by job_id order by elapsed_ms) as prev_etime,
    average_slot_usage,
    safe_divide((total_slot_ms-prev_total_slot_ms), (elapsed_ms - prev_elapsed_ms)) as unit_slot_usage,
    pending_units,
    completed_units,
    active_units,
  from 
    tb_tlo tlo, tb_settings ts
),
tb_tloe2 as (
  select
      elapsed_ms,
      case when prev_etime is null then waiting_ms else null end as target_waiting_ms, 
      etime,
      ifnull(prev_etime, start_time) as prev_etime,
      average_slot_usage,
      unit_slot_usage,
      pending_units,
      completed_units,
      active_units,
  from 
    tb_tloe
),
tb_tloe3 as (
  select
      elapsed_ms,
      target_waiting_ms,
      timestamp_add(etime, interval cast(timestamp_diff(etime, prev_etime, MILLISECOND) / 2 as int64) MILLISECOND) as ctime,
      average_slot_usage,
      unit_slot_usage,
      pending_units,
      completed_units,
      active_units,
  from 
    tb_tloe2
),
tb_stat as (
  select
    timestamp_seconds(div(UNIX_SECONDS(ctime), bucket_time) *  bucket_time) as ctime_bucket,
    elapsed_ms,
    target_waiting_ms,
    average_slot_usage,
    unit_slot_usage,
    pending_units,
    completed_units,
    active_units,
  from 
    tb_tloe3 tloe3, tb_settings
),
tb_stat2 as (
  select 
    ctime_bucket, 
    count(1) as job_count_per_timebucket,
    avg(elapsed_ms) as average_elapsed_ms,
    avg(target_waiting_ms) as average_waiting_ms,
    avg(average_slot_usage) as average_slot_usage,
    avg(unit_slot_usage) as average_unit_slot_usage,
    avg(pending_units) as average_pending_units,
    avg(completed_units) as average_completed_units,
    avg(active_units) as average_active_units
  from 
    tb_interval left outer join tb_stat stat using(ctime_bucket), tb_settings
  group by 1
)
select 
  ctime_bucket, 
  job_count_per_timebucket,
  average_elapsed_ms,
  average_waiting_ms,
  average_slot_usage,
  average_unit_slot_usage,
  average_pending_units,
  average_completed_units,
  average_active_units
from 
  tb_stat2
order by 1
