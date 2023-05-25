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
