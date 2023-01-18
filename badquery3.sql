with tb_settings as (
  select
    current_timestamp() - interval 30 minute as search_start_time,
    current_timestamp() as search_finish_time, 
    10 as bucket_time ,
    1000 as search_maximum_bytes_processed,
    10000 as search_maximum_waiting_ms, 
    100 as check_elapsed_ms, 
    10 as check_average_slot_usage, 
    10000 as check_bytes_spilled_to_disk, 
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
tb_tlo as (
  select 
    creation_time, 
    job_id, 
    elapsed_ms,
    state,
    start_time,
    timestamp_diff(start_time, creation_time, MILLISECOND) as waiting_ms,
    datetime_add(start_time, interval ifnull(elapsed_ms,0) millisecond) as etime,
    lag(elapsed_ms) over (partition by job_id order by elapsed_ms) as prev_elapsed_ms,
    total_slot_ms, 
    lag(total_slot_ms) over (partition by job_id order by elapsed_ms) as prev_total_slot_ms,
    safe_divide(total_slot_ms, elapsed_ms) as average_slot_usage,
    safe_divide((total_slot_ms-lag(total_slot_ms) over (partition by job_id order by elapsed_ms)), (elapsed_ms - lag(elapsed_ms) over (partition by job_id order by elapsed_ms))) as unit_slot_usage,
    pending_units, 
    completed_units, 
    active_units,
    total_bytes_processed
  from 
    tb_jobs tbo, unnest(timeline) tl, tb_settings
),
tb_tloe as (
  select
    creation_time, 
    job_id, 
    tlo.elapsed_ms,
    state,
    start_time,
    waiting_ms,
    prev_elapsed_ms,
    etime,
    lag(etime) over (partition by job_id order by elapsed_ms) as prev_etime,
    total_slot_ms,
    prev_total_slot_ms,
    average_slot_usage,
    unit_slot_usage,
    pending_units,
    completed_units,
    active_units,
    total_bytes_processed
  from 
    tb_tlo tlo, tb_settings ts
  where
    1=1
    -- and tlo.elapsed_ms > ts.check_elapsed_ms
),
tb_tloe2 as (
  select
      creation_time, 
      job_id, 
      elapsed_ms,
      state,
      start_time,
      waiting_ms,
      case when prev_etime is null then waiting_ms else 0 end as target_waiting_ms, 
      prev_elapsed_ms,
      etime,
      ifnull(prev_etime, start_time) as prev_etime,
      total_slot_ms,
      prev_total_slot_ms,
      average_slot_usage,
      unit_slot_usage,
      pending_units,
      completed_units,
      active_units,
      total_bytes_processed
  from 
    tb_tloe
),
tb_tloe3 as (
  select
      creation_time, 
      job_id, 
      elapsed_ms,
      state,
      start_time,
      waiting_ms,
      target_waiting_ms,
      prev_elapsed_ms,
      etime,
      prev_etime,
      timestamp_add(etime, interval cast(timestamp_diff(etime, prev_etime, MILLISECOND) / 2 as int64) MILLISECOND) as ctime,
      total_slot_ms,
      prev_total_slot_ms,
      average_slot_usage,
      unit_slot_usage,
      pending_units,
      completed_units,
      active_units,
      total_bytes_processed
  from 
    tb_tloe2
),
tb_stat as (
  select
        timestamp_seconds(div(UNIX_SECONDS(ctime), bucket_time) *  bucket_time) as ctime_bucket,
        creation_time, 
        job_id, 
        elapsed_ms,
        state,
        start_time,
        waiting_ms,
        target_waiting_ms,
        prev_elapsed_ms,
        etime,
        prev_etime,
        ctime,
        total_slot_ms,
        prev_total_slot_ms,
        average_slot_usage,
        unit_slot_usage,
        pending_units,
        completed_units,
        active_units,
        total_bytes_processed
  from 
    tb_tloe3, tb_settings
),
tb_stat2 as (
  select 
    ctime_bucket, 
    sum(target_waiting_ms) as sum_waiting_ms,
    sum(average_slot_usage) as sum_average_slot_usage,
    sum(unit_slot_usage) as sum_unit_slot_usage,
    sum(pending_units) as sum_pending_units,
    sum(completed_units) as sum_completed_units,
    sum(active_units) as sum_active_units
  from 
    tb_stat, tb_settings
  group by 1
)
select 
  ctime_bucket, 
  tb_stat2.sum_waiting_ms / bucket_time as sum_waiting_ms,
  sum_average_slot_usage / bucket_time as sum_average_slot_usage,
  sum_unit_slot_usage / bucket_time as sum_unit_slot_usage,
  sum_pending_units / bucket_time as sum_pending_units,
  sum_completed_units / bucket_time as sum_completed_units,
  sum_active_units / bucket_time as sum_active_units
from 
  tb_stat2, tb_settings
order by 1 asc