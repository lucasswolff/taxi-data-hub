{{ config(materialized = 'view') }}


with base as (
    select * from {{ ref('feat_taxi_outliers') }}
)
select 
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_year,
    dropoff_month,
    dropoff_day,
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    count(*) number_of_trips,
    sum(trip_distance) as trip_distance,
    sum(fare_amount) as fare_amount,
    sum(total_amount) as total_amount
from base
where 
    not trip_outlier_flag 
    and not fare_amount_outlier_flag
group by all
