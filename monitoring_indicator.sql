with tb_settings as (
  select
    current_timestamp() - interval 60 day as search_start_time,
    current_timestamp() as search_finish_time, 
    5 as bucket_time , -- second
    [10000,50000,100000,500000,1000000,5000000,10000000,50000000,10000000000000] as total_slot_ms_bucket,
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
    total_slot_ms as finished_total_slot_ms
  from 
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
    ifnull(lag(elapsed_ms) over (partition by job_id order by elapsed_ms),0) as prev_elapsed_ms,
    total_slot_ms, 
    ifnull(lag(total_slot_ms) over (partition by job_id order by elapsed_ms),0) as prev_total_slot_ms,
    safe_divide(total_slot_ms, elapsed_ms) as average_slot_usage,
    pending_units, 
    completed_units, 
    active_units,
    total_bytes_processed,
    finished_total_slot_ms
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
    safe_divide((total_slot_ms-prev_total_slot_ms), (elapsed_ms - prev_elapsed_ms)) as unit_slot_usage,
    pending_units,
    completed_units,
    active_units,
    total_bytes_processed,
    finished_total_slot_ms
  from 
    tb_tlo tlo, tb_settings ts
  where
    1=1
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
      total_bytes_processed,
      finished_total_slot_ms
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
      total_bytes_processed,
      finished_total_slot_ms
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
        (select sum(inx) from unnest((select array(select cast(sign(floor(finished_total_slot_ms / b)) as int64) from tb_settings, unnest(tb_settings.total_slot_ms_bucket) b))) inx) as target_slot_ms_bucket,
        prev_total_slot_ms,
        average_slot_usage,
        unit_slot_usage,
        pending_units,
        completed_units,
        active_units,
        total_bytes_processed,
        finished_total_slot_ms
  from 
    tb_tloe3, tb_settings t
),
tb_stat2 as (
  select 
    ctime_bucket, 
    target_slot_ms_bucket,
    count(1) as job_count_per_timebucket,
    avg(elapsed_ms) as average_elapsed_ms,
    avg(target_waiting_ms) as average_waiting_ms,
    avg(average_slot_usage) as average_slot_usage,
    avg(unit_slot_usage) as average_unit_slot_usage,
    avg(pending_units) as average_pending_units,
    avg(completed_units) as average_completed_units,
    avg(active_units) as average_active_units
  from 
    tb_stat, tb_settings
  group by 1, 2
), 
tb_stat_by_timebucket as (
  select 
    ctime_bucket, 
    target_slot_ms_bucket,
    job_count_per_timebucket,
    average_elapsed_ms,
    average_waiting_ms,
    average_slot_usage,
    average_unit_slot_usage,
    average_pending_units,
    average_completed_units,
    average_active_units
  from 
    tb_stat2, tb_settings
)
select distinct
  total_slot_ms_bucket[offset(target_slot_ms_bucket)] total_slot_ms_bucket, 
  percentile_disc(average_elapsed_ms, 0.25) over (partition by target_slot_ms_bucket) as elapsed_25,
  percentile_disc(average_elapsed_ms, 0.50) over (partition by target_slot_ms_bucket) as elapsed_50,
  percentile_disc(average_elapsed_ms, 0.75) over (partition by target_slot_ms_bucket) as elapsed_75
from 
  tb_stat_by_timebucket, tb_settings
order by 1 asc