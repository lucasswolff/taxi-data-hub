{{ config(materialized = 'view') }}

with yellow_green_union as (
    select * from {{ ref('fct_yellow_green_taxi_union') }}
),
ratio_calcs as (
    select 
        *,
        (trip_distance/ifnull(trip_duration_min, 0))::float as miles_per_minute,
        (miles_per_minute*60)::float as miles_per_hour,
        (fare_amount/ifnull(trip_distance, 0))::float as fare_amount_per_mile,
        (fare_amount/trip_duration_min)::float as fare_amount_per_min
    from yellow_green_union
),
date_calcs as (
    select
        *,

        strftime(pickup_datetime, '%Y')  :: int as pickup_year,
        strftime(pickup_datetime, '%m')  :: int as pickup_month,
        strftime(pickup_datetime, '%d')  :: int as pickup_day,
        strftime(pickup_datetime, '%H')  :: int as pickup_hour,

        strftime(dropoff_datetime, '%Y') :: int as dropoff_year,
        strftime(dropoff_datetime, '%m') :: int as dropoff_month,
        strftime(dropoff_datetime, '%d') :: int as dropoff_day,
        strftime(dropoff_datetime, '%H') :: int as dropoff_hour
    from ratio_calcs
)
select * from date_calcs