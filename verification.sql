-- Information Schema. JOB_BY_XXX
-- 1. Each Row means Each JOB. How to check use the below query
select job_id
  from `region-asia-northeast3.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
 group by job_id 
 having count(1) > 1
 -- value : 0. Each row must be to representative of each job.

 -- 2. When the column 'total_slot_ms' can have 0 value.
select *
  from `region-asia-northeast3.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
 where total_slot_ms is null
   and error_result is null
   and cache_hit is not true
   and (statement_type not like 'CREATE%' and statement_type not like 'TRUNCATE%')
   ;
 -- a. invalid query. (error_result is not null)
 -- b. cache hit!
 -- c. DDL only exclude CTAS.

-- 3. What do Time slices represent? Stage ? Phase ? Real T+0,1,2,3,4? I Think that 3rd one - T+0, 1, 2, 3, 
with max_row as (
select *
  from `region-asia-northeast3.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
 where job_id = 'job_KsBH0ich0yuaKTkLSXfX9cTNBcyL'
)
select tl.*
  from max_row cross join unnest(timeline) as tl
;
-- 627 87524 0 560 0 : Initialization Time need 
-- 677 87524 0 560 0