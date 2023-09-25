with tb_interval as (
select t0
  from unnest(generate_array(0,60*24 - 1)) as t0
)
, tb_timebucket as (
select timestamp_sub(TIMESTAMP_TRUNC(current_timestamp(), MINUTE, "Asia/Seoul"), interval t0 minute) as stb,
       timestamp_sub(TIMESTAMP_TRUNC(current_timestamp(), MINUTE, "Asia/Seoul"), interval t0 - 1 minute) as etb
  from tb_interval
)
, tb_all_queries as (
select TIMESTAMP_TRUNC(creation_time, MINUTE, "Asia/Seoul") as cmin, 
	   TIMESTAMP_TRUNC(start_time, MINUTE, "Asia/Seoul") as smin,
	   TIMESTAMP_TRUNC(case when end_time is null then current_timestamp() end, MINUTE, "Asia/Seoul") as emin,
	   extract(minute from (TIMESTAMP_TRUNC(case when end_time is null then current_timestamp() else end_time end, MINUTE, "Asia/Seoul") - TIMESTAMP_TRUNC(start_time, MINUTE, "Asia/Seoul"))) + 1.0 as minbucket_count,
       *
  from `region-asia-northeast3.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
 where creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 26 HOUR)
   -- and total_slot_ms is not null
)
, tb_avg_slotms as (
select total_slot_ms / minbucket_count as avg_slot_ms,
       total_bytes_processed / minbucket_count as avg_bytes_processed,
       *
  from tb_all_queries
)
, tb_stat_base as (
select stb, 
       project_id, 
       job_id, 
       job_type, 
       statement_type,
       reservation_id,
       priority,
       state,
       avg_slot_ms,
       avg_bytes_processed
  from tb_timebucket ttb left join tb_avg_slotms tas on (tas.start_time >= ttb.stb and ifnull(tas.end_time, tas.start_time) < ttb.etb)
)
select stb, 
       project_id, 
       job_type, 
       statement_type, 
       reservation_id,
       priority,
       state,
       sum(avg_slot_ms) as total_slot_ms,
       sum(avg_bytes_processed) as total_bytes_processed
  from tb_stat_base
 group by stb, project_id, job_type, statement_type, reservation_id, priority, state
 order by stb desc
  ;