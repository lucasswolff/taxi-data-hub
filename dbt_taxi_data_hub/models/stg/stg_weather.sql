with stg_weather as (
    select * from {{ ref('src_weather') }}
)
select
    "datetime"::timestamp as "datetime",
    latitude::float as latitude,
    longitude::float as longitude,
    borough::varchar as borough,
    temperature_2m::float as temperature,
    precipitation::float as precipitation,
    rain::float as rain,
    snowfall::float as snowfall,
    api_inserted_datetime::timestamp as api_inserted_datetime
from stg_weather