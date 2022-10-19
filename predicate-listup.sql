with tb_settings as (
  select cast('2022-10-17' as timestamp) as start_date
)
, tb_org as (
  select project_id, user_email, job_id, job_type, total_slot_ms, query, job_stages
    from `<<project-id>>`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, tb_settings
   where creation_time > start_date
)
,tb_stages as (
  select tbo.project_id, tbo.user_email, tbo.job_id, tbo.job_type, tbo.total_slot_ms, tbo.query, js.*
    from tb_org tbo, unnest(job_stages) js
)
, tb_steps as (
  select tbs.project_id, tbs.user_email, tbs.job_id, tbs.job_type, tbs.total_slot_ms, tbs.query, tbs.name as stage_name, tbs.id, tbs.shuffle_output_bytes, tbs.shuffle_output_bytes_spilled, tbs.records_read, tbs.records_written, tbs.parallel_inputs, st.*
    from tb_stages tbs, unnest(steps) st
)
, tb_substeps as (
  select tbs.project_id, tbs.user_email, tbs.job_id, tbs.job_type, tbs.total_slot_ms, tbs.query, tbs.stage_name, tbs.id, tbs.shuffle_output_bytes, tbs.shuffle_output_bytes_spilled, tbs.records_read, tbs.records_written, tbs.parallel_inputs, 
    tbs.kind, substep
   from tb_steps tbs, unnest(substeps) substep
)
, tb_readstep as (
  select *
    from tb_substeps
   where kind = 'READ'
     and stage_name like '%Input'
)
, tb_tables as (
  select *, regexp_extract(substep, r" (.+?)$") as table_name
    from tb_readstep
   where substep like 'FROM %'
)
, tb_columns as (
  select *, split(param, ':')[offset(0)] as key, split(param, ':')[offset(1)] as col
    from tb_readstep, unnest(split(substep, ',')) param
   where substep like '$%'
)
, tb_predicate as (
  select ts.*, predicate_key
    from tb_readstep ts, unnest(regexp_extract_all(substep, r"\$[0-9]+")) predicate_key
   where substep like 'WHERE%'
)
select tp.project_id, tp.user_email, tp.job_id, tp.job_type, tp.total_slot_ms, tp.query, tp.shuffle_output_bytes, tp.shuffle_output_bytes_spilled, tp.records_read, tp.records_written, tp.parallel_inputs, tp.stage_name, tp.substep, tt.table_name, tc.col as predicate_column
  from tb_predicate tp 
       join tb_columns tc on (tp.project_id = tc.project_id and tp.user_email = tc.user_email and tp.job_id = tc.job_id and tp.stage_name = tc.stage_name and tp.kind = tc.kind and tp.predicate_key = tc.key)
       join tb_tables tt on (tp.project_id = tt.project_id and tp.user_email = tt.user_email and tp.job_id = tt.job_id and tp.stage_name = tt.stage_name and tp.kind = tt.kind)