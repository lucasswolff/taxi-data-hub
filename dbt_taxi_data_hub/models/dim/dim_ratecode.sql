with seed_ratecode as (
    select * from {{ ref('seed_ratecode') }}
)
select 
    ratecodeid::int as ratecode_id,  
    ratecodedesc::varchar as ratecode_desc
from seed_ratecode