with tb_settings as (
  select
    current_timestamp() - interval 60 day as search_start_time,
    current_timestamp() as search_finish_time, 
    10 as bucket_time ,
    1000 as threshold_elapsed_ms, 
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
    timestamp_diff(end_time, start_time, millisecond) as elapsed_ms
  from 
    `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
  where
    1=1
    and creation_time between search_start_time and search_finish_time
    and state in unnest(check_state)
    and timestamp_diff(end_time, start_time, millisecond) > threshold_elapsed_ms
), 
tb_tlo as (
  select 
    tbo.creation_time, 
    tbo.job_id,
    tbo.job_stages,
    tbo.job_type,
    tbo.priority,
    tbo.project_id,
    tbo.query, 
    tbo.start_time,
    tbo.state,
    tbo.statement_type,
    tl.elapsed_ms,
    timestamp_diff(tbo.start_time, tbo.creation_time, MILLISECOND) as waiting_ms,
    datetime_add(tbo.start_time, interval ifnull(tl.elapsed_ms,0) millisecond) as etime,
    lag(tl.elapsed_ms) over (partition by tbo.job_id order by tl.elapsed_ms) as prev_elapsed_ms,
    tl.total_slot_ms, 
    lag(tl.total_slot_ms) over (partition by tbo.job_id order by tl.elapsed_ms) as prev_total_slot_ms,
    safe_divide(tl.total_slot_ms, tl.elapsed_ms) as average_slot_usage,
    safe_divide((tl.total_slot_ms-lag(tl.total_slot_ms) over (partition by tbo.job_id order by tl.elapsed_ms)), (tl.elapsed_ms - lag(tl.elapsed_ms) over (partition by tbo.job_id order by tl.elapsed_ms))) as unit_slot_usage,
    tl.pending_units, 
    tl.completed_units, 
    tl.active_units,
    tbo.total_bytes_processed
  from 
    tb_jobs tbo, unnest(timeline) tl
),
tb_tloe as (
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
    tlo.elapsed_ms,
    waiting_ms,
    etime,
    prev_elapsed_ms,
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
    tlo.elapsed_ms between ts.threshold_elapsed_ms and (ts.threshold_elapsed_ms + 3000)
),
tb_tloe2 as (
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
    elapsed_ms,
    waiting_ms,
    etime,
    prev_etime,
    prev_elapsed_ms,
    case when prev_etime is null then waiting_ms else 0 end as target_waiting_ms, 
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
    job_stages,
    job_type,
    priority,
    project_id,
    query,
    start_time,
    state,
    statement_type,
    elapsed_ms,
    waiting_ms,
    etime,
    prev_etime,
    prev_elapsed_ms,
    target_waiting_ms, 
    total_slot_ms,
    prev_total_slot_ms,
    average_slot_usage,
    unit_slot_usage,
    pending_units,
    completed_units,
    active_units,
    total_bytes_processed,
    timestamp_add(etime, interval cast(timestamp_diff(etime, prev_etime, MILLISECOND) / 2 as int64) MILLISECOND) as ctime
  from 
    tb_tloe2
),
tb_stat as (
select distinct
  creation_time, 
  job_id, 
  job_type,
  priority,
  project_id,
  query,
  start_time,
  state,
  statement_type,
  waiting_ms,
  last_value(elapsed_ms) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) elapsed_ms,
  last_value(etime) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) etime,
  last_value(total_slot_ms) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) total_slot_ms,
  last_value(average_slot_usage) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) average_slot_usage,
  last_value(pending_units) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) pending_units,
  last_value(completed_units) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) completed_units,
  last_value(active_units) over (partition by job_id order by elapsed_ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) active_units
from 
  tb_tloe3, tb_settings
)
select 
  any_value(max_average_slot_usage) as max_average_slot_usage,
  any_value(p99_average_slot_usage) as p99_average_slot_usage,
  any_value(p95_average_slot_usage) as p95_average_slot_usage,
  from (
select 
  percentile_disc(average_slot_usage, 1) over (partition by state) as max_average_slot_usage,
  percentile_disc(average_slot_usage, 0.99) over (partition by state) as p99_average_slot_usage,
  percentile_disc(average_slot_usage, 0.95) over (partition by state) as p95_average_slot_usage
  from tb_stat
  )
 