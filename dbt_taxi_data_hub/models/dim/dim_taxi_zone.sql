with seed_taxi_zone as (
    select * from {{ ref('seed_taxi_zone') }}
)
select 
    t.locationid::int as location_id,  
    b.borough_id::int as borough_id,
    t.borough::varchar as borough,
    t."zone"::varchar as "zone",
    t.service_zone::varchar as service_zone
from seed_taxi_zone t
left join seed_borough b on b.borough = t.borough