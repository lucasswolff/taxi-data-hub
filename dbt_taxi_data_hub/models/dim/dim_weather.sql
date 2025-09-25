with weather_data as (
    select * from {{ ref('stg_weather') }}
),
seed_borough as (
    select * from {{ ref('seed_borough') }}
),
weather_dedup as (
    select 
        *, 
        row_number() over (partition by datetime, borough order by api_inserted_datetime desc) as row_n
    from weather_data
    qualify row_n = 1
)
select 
    w.datetime,
    w.latitude,
    w.longitude,
    b.borough_id::int as borough_id,
    w.borough,
    w.temperature,
    w.precipitation,
    w.rain,
    w.snowfall,
    w.api_inserted_datetime
from weather_dedup w
left join seed_borough b on b.borough = w.borough