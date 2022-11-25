with tb_settings as (
  select timestamp_sub(current_timestamp(),interval 10 hour) as start_date,
         current_timestamp() as end_date,
         10000 as maximum_bytes_billed,
         10000000 as bytes_spilled_to_disk
)
, tb_org as (
  select project_id, user_email, job_id, job_type, total_slot_ms, query, job_stages, total_bytes_processed
    from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
   where creation_time between start_date and end_date
)
,tb_stages as (
  select tbo.project_id, tbo.user_email, tbo.job_id, tbo.job_type, tbo.total_slot_ms, tbo.query, tbo.total_bytes_processed, js.*
    from tb_org tbo, unnest(job_stages) js
)
, tb_steps as (
  select tbs.project_id, tbs.user_email, tbs.job_id, tbs.job_type, tbs.total_slot_ms, tbs.query, tbs.total_bytes_processed, tbs.name as stage_name, tbs.id, tbs.shuffle_output_bytes, tbs.shuffle_output_bytes_spilled, tbs.records_read, tbs.records_written, tbs.parallel_inputs, st.*
    from tb_stages tbs, unnest(steps) st
)
, tb_substeps as (
  select tbs.project_id, tbs.user_email, tbs.job_id, tbs.job_type, tbs.total_slot_ms, tbs.query, tbs.total_bytes_processed, tbs.stage_name, tbs.id, tbs.shuffle_output_bytes, tbs.shuffle_output_bytes_spilled, tbs.records_read, tbs.records_written, tbs.parallel_inputs, 
    tbs.kind, substep
   from tb_steps tbs, unnest(substeps) substep
)
select distinct project_id, user_email, job_id, query, job_type
  from tb_substeps, tb_settings
 where shuffle_output_bytes_spilled > bytes_spilled_to_disk  
   or total_bytes_processed > maximum_bytes_billed
