with seed_taxi_zone as (
    select * from {{ ref('seed_taxi_zone') }}
)
select 
    locationid::int as location_id,  
    borough::varchar as borough,
    "zone"::varchar as "zone",
    service_zone::varchar as service_zone
from seed_taxi_zone