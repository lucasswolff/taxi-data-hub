{{ config(materilized='table') }}

with seed_trip_type as (
    select * from {{ ref('seed_trip_type') }}
)
select 
    trip_type::int as trip_type,  
    trip_type_desc::varchar as trip_type_desc
from seed_trip_type