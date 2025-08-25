{{ config(materialized = 'view') }}

with taxi_ratios as (
    select * from {{ ref('feat_taxi_calcs') }}
),
outliers as (
    select 
        *,
        
        case
            when miles_per_minute < 0.01 or miles_per_minute > 2 then true
            when trip_distance > 100 and trip_duration_min < 60 then true
            when trip_distance < 20 and trip_duration_min > 60*4 then true
            when trip_distance < 50 and trip_duration_min > 60*8 then true
            when trip_distance < 100 and trip_duration_min > 60*12 then true
            else false
        end as trip_outlier_flag,

        case
            when fare_amount_per_mile < 0 or fare_amount_per_mile > 30 then true
            when fare_amount_per_min < 0 or fare_amount_per_min > 15 then true
            when fare_amount_per_min = 0 and fare_amount_per_mile = 0 then true
            else false
        end as fare_amount_outlier_flag
    from taxi_ratios
)
select * from outliers