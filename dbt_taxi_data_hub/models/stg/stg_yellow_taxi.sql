WITH stg_yellow AS (
    SELECT * FROM {{ ref('src_yellow_taxi') }}
)
SELECT
    VendorID::INT as vendor_id, 
    tpep_pickup_datetime::TIMESTAMP as pickup_datetime, 
    tpep_dropoff_datetime::TIMESTAMP as dropoff_datetime,
    PULocationID::INT as pickup_location_id, 
    DOLocationID::INT as dropoff_location_id,
    RatecodeID::INT as ratecode_id,
    store_and_fwd_flag::VARCHAR as store_and_fwd_flag,
    payment_type::INT as payment_type,
    passenger_count::INT as passenger_count, 
    trip_distance::FLOAT as trip_distance,
    fare_amount::FLOAT as fare_amount,
    extra::FLOAT as extra,
    mta_tax::FLOAT as mta_tax,
    tip_amount::FLOAT as tip_amount,
    tolls_amount::FLOAT as tolls_amount,
    improvement_surcharge::FLOAT as improvement_surcharge,
    total_amount::FLOAT as total_amount,
    congestion_surcharge::FLOAT as congestion_surcharge,
    Airport_fee::FLOAT as airport_fee
FROM stg_yellow
