with most_used_queries as (
select query_info.query_hashes.normalized_literals as hash_id, count(1) as used_count, sum(total_slot_ms) as total_slot_sum, sum(total_bytes_processed) as total_bytes_sum
  from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
 group by query_info.query_hashes.normalized_literals
),
tb_sample_query as (
select query, hash_id
from (  
select query, query_info.query_hashes.normalized_literals as hash_id, row_number() over (partition by query_info.query_hashes.normalized_literals order by total_slot_ms desc) as _rnk
  from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
)
where _rnk = 1
)
select * 
  from most_used_queries join tb_sample_query using(hash_id)
order by 2 desc

-- If there are few queries in jobs_by view, you can use more concise version of the above query.
-- with most_used_queries as (
-- select query_info.query_hashes.normalized_literals as hash_id, count(1) as used_count, sum(total_slot_ms) as total_slot_sum, sum(total_bytes_processed) as total_bytes_sum, array_agg(query order by total_slot_ms desc limit 1)[safe_offset(0)] as query
--   from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
--  group by query_info.query_hashes.normalized_literals
-- )
-- select * 
--   from most_used_queries
-- order by 2 desc

-- Added. For spilling detection
-- with spilled_queries as (
-- select query_info.query_hashes.normalized_literals as hash_id, sum(js.shuffle_output_bytes_spilled) as sum_of_spilled
--   from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, unnest(job_stages) js
--  group by query_info.query_hashes.normalized_literals
-- ),
-- most_used_queries as (
-- select query_info.query_hashes.normalized_literals as hash_id, count(1) as used_count, sum(total_slot_ms) as total_slot_sum, sum(total_bytes_processed) as total_bytes_sum, array_agg(query order by total_slot_ms desc limit 1)[safe_offset(0)] as query
--   from `region-asia-northeast3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
--  group by query_info.query_hashes.normalized_literals
-- )
-- select * 
--   from most_used_queries join spilled_queries using(hash_id)
-- order by 2 desc
