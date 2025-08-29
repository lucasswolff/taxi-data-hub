{% set adapter = target.type %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge' if adapter in ['snowflake', 'bigquery', 'redshift', 'databricks'] else 'append',
    unique_key = 'trip_sk'
) }}

{% set start_date = var('start_date', False) %}
{% set end_date   = var('end_date', False) %}

with taxi_outl as (
    select * from {{ ref('feat_taxi_outliers') }}
    where 1=1
    {% if is_incremental() %}
        {% if start_date and end_date %}

            and file_date >= '{{ start_date }}'
            and file_date <= '{{ end_date }}'

        {% else %}

            and file_date > (select max(file_date) from {{ this }})

        {% endif %}
    {% endif %}
),
weather as (
    select * from {{ ref('dim_weather') }}
),
taxi_hourly as (
    select 
        *, 
        date_trunc('hour', pickup_datetime) as pickup_datetime_hourly,
        date_trunc('hour', dropoff_datetime) as dropoff_datetime_hour
    from taxi_outl
)
select 
    t.*,
    wp.temperature as pickup_temperature,
    wp.precipitation as pickup_precipitation,
    wp.rain as pickup_rain,
    wp.snowfall as pickup_snowfall,
    wd.temperature as dropoff_temperature,
    wd.precipitation as dropoff_precipitation,
    wd.rain as dropoff_rain,
    wd.snowfall as dropoff_snowfall
from taxi_hourly t 
left join weather wp on t.pickup_borough_id = wp.borough_id and t.pickup_datetime_hourly = wp.datetime
left join weather wd on t.dropoff_borough_id = wd.borough_id and t.dropoff_datetime_hour = wd.datetime

