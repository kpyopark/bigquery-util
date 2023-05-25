with tb_settings as (
  select
    current_timestamp() - interval 7 day as search_start_time,
    current_timestamp() as search_finish_time, 
    1000 as search_maximum_bytes_processed,
    10000 as search_maximum_waiting_ms, 
    60 as check_elapsed_ms, 
    10000 as check_average_slot_usage, 
    100000 as check_total_slot_ms,
    10000 as check_bytes_spilled_to_disk,
    ['RUNNING'] as check_state
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
    `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
  where
    creation_time between search_start_time and search_finish_time
    and state in unnest(check_state)
), 
tb_tlo as (
  select 
    creation_time, 
    job_id, 
    elapsed_ms,
    query,
    lag(elapsed_ms) over (partition by job_id order by elapsed_ms) as prev_elapsed_ms,
    total_slot_ms, 
    lag(total_slot_ms) over (partition by job_id order by elapsed_ms) as prev_total_slot_ms,
    safe_divide(total_slot_ms, elapsed_ms) as average_slot_usage,
    safe_divide((total_slot_ms-lag(total_slot_ms) over (partition by job_id order by elapsed_ms)), (elapsed_ms - lag(elapsed_ms) over (partition by job_id order by elapsed_ms))) as unit_slot_usage,
    pending_units, 
    completed_units, 
    active_units
  from 
    tb_jobs tbo, unnest(timeline) tl, tb_settings
)
select
  *
from 
  tb_tlo tlo, tb_settings ts
where
  ( tlo.elapsed_ms > ts.check_elapsed_ms and tlo.average_slot_usage > ts.check_average_slot_usage)
  or ( tlo.total_slot_ms > ts.check_total_slot_ms )
