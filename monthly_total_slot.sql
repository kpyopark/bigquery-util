with tb_settings as (
  select
    interval 30 day as search_pending_time,
    interval 60 day as target_period,
),
tb_search_con as (
  select
    current_timestamp() - search_pending_time - target_period as search_start_time,
    current_timestamp() - search_pending_time as search_finish_time, 
  from tb_settings
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
    total_slot_ms,
    total_bytes_processed, 
  from 
    -- `region-US`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_search_con
    `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_search_con
  where
    1=1
    and total_slot_ms is not null
    and creation_time between search_start_time and search_finish_time
)
select sum(total_slot_ms) / 1000 as monthly_total_slot,
  sum(total_slot_ms) / 1000 / 30 as average_daily_slot,
  sum(total_slot_ms) / 1000 / 30 / 24 as average_hourly_slot,
  sum(total_slot_ms) / 1000 / 30 / 24 / 3600 as average_sec_slot,
  from tb_jobs
