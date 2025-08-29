{% macro integrate_taxi(source_model) %}

with base as (
    select * from {{ ref(source_model) }}
),
joins as (
    select 
        b.*,
        pt.payment_type_desc,
        rc.ratecode_desc,
        tp.trip_type_desc,
        tz_pickup.borough_id as pickup_borough_id,
        tz_pickup.borough as pickup_borough,
        tz_pickup."zone" as pickup_zone,
        tz_pickup.service_zone as pickup_service_zone,
        tz_dropoff.borough_id as dropoff_borough_id,
        tz_dropoff.borough as dropoff_borough,
        tz_dropoff."zone" as dropoff_zone,
        tz_dropoff.service_zone as dropoff_service_zone,
        v.vendor_desc
    from base b
    left join {{ ref('dim_payment_type') }} pt on b.payment_type = pt.payment_type
    left join {{ ref('dim_ratecode') }} rc on b.ratecode_id = rc.ratecode_id
    left join {{ ref('dim_trip_type') }} tp on b.trip_type = tp.trip_type
    left join {{ ref('dim_taxi_zone') }} tz_pickup on b.pickup_location_id = tz_pickup.location_id
    left join {{ ref('dim_taxi_zone') }} tz_dropoff on b.dropoff_location_id = tz_dropoff.location_id 
    left join {{ ref('dim_vendor') }} v on b.vendor_id = v.vendor_id
)
select * from joins

{% endmacro %}