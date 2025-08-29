{% set adapter = target.type %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge' if adapter in ['snowflake', 'bigquery', 'redshift', 'databricks'] else 'append',
    unique_key = 'trip_sk'
) }}

{% set start_date = var('start_date', False) %}
{% set end_date   = var('end_date', False) %}

with base as (
    select *
    from {{ ref('int_yellow_taxi_integrated') }} 
),
fct_incremental as (
    select * 
    from base
    where 1=1
    {% if is_incremental() %}
        {% if start_date and end_date %}

            and file_date >= '{{ start_date }}'
            and file_date <= '{{ end_date }}'

        {% else %}

            and file_date > (select max(file_date) from {{ this }})

        {% endif %}
    {% endif %}
)
select
    trip_sk,
    taxi_type,
    file_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    pickup_borough_id,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_location_id,
    dropoff_borough_id,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    vendor_id,
    vendor_desc,
    trip_type,
    trip_type_desc,
    ratecode_id,
    ratecode_desc,
    payment_type,
    payment_type_desc,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount
from fct_incremental