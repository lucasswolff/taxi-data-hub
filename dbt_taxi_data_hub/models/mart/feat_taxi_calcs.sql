{{ config(materialized = 'view') }}

with yellow_green_union as (
    select * from {{ ref('fct_yellow_green_taxi_union') }}
),
ratio_calcs as (
    select 
        *,
        (trip_distance/nullif(trip_duration_min, 0))::float as miles_per_minute,
        (miles_per_minute*60)::float as miles_per_hour,
        (fare_amount/nullif(trip_distance, 0))::float as fare_amount_per_mile,
        (fare_amount/nullif(trip_duration_min,0))::float as fare_amount_per_min
    from yellow_green_union
),
date_calcs as (
    select
        *,

        year(pickup_datetime) as pickup_year,
        month(pickup_datetime) as pickup_month,
        day(pickup_datetime) as pickup_day,
        hour(pickup_datetime) as pickup_hour,

        year(dropoff_datetime) as dropoff_year,
        month(dropoff_datetime) as dropoff_month,
        day(dropoff_datetime) as dropoff_day,
        hour(dropoff_datetime) as dropoff_hour,
    from ratio_calcs
)
select * from date_calcs