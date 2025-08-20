with stg_yellow as (
    select * from {{ ref('src_yellow_taxi') }}
)
select
    regexp_extract(filename, '(\d{4})-\d{2}\.parquet$', 1) as file_year,
    regexp_extract(filename, '\d{4}-(\d{2})\.parquet$', 1) as file_month,
    vendorid::int as vendor_id, 
    tpep_pickup_datetime::timestamp as pickup_datetime, 
    tpep_dropoff_datetime::timestamp as dropoff_datetime,
    pulocationid::int as pickup_location_id, 
    dolocationid::int as dropoff_location_id,
    ratecodeid::int as ratecode_id,
    store_and_fwd_flag::varchar as store_and_fwd_flag,
    payment_type::int as payment_type,
    passenger_count::int as passenger_count, 
    trip_distance::float as trip_distance,
    fare_amount::float as fare_amount,
    extra::float as extra,
    mta_tax::float as mta_tax,
    tip_amount::float as tip_amount,
    tolls_amount::float as tolls_amount,
    improvement_surcharge::float as improvement_surcharge,
    total_amount::float as total_amount,
    congestion_surcharge::float as congestion_surcharge,
    airport_fee::float as airport_fee,
    cbd_congestion_fee::float as cbd_congestion_fee
from stg_yellow
