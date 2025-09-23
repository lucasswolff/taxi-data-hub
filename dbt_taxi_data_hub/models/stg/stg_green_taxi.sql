with stg_green as (
    select * from {{ ref('src_green_taxi') }}
)
{% if target.name == 'dev' %}
select
    regexp_extract(filename, '(\d{4})-\d{2}\.parquet$', 1) as file_year,
    regexp_extract(filename, '\d{4}-(\d{2})\.parquet$', 1) as file_month,
    vendorid::int as vendor_id, 
    lpep_pickup_datetime::datetime as pickup_datetime, 
    lpep_dropoff_datetime::datetime as dropoff_datetime,
    pulocationid::int as pickup_location_id, 
    dolocationid::int as dropoff_location_id,
    ratecodeid::int as ratecode_id,
    trip_type::int as trip_type,
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
    congestion_surcharge::float as congestion_surcharge,
    ehail_fee::float as ehail_fee, 
    total_amount::float as total_amount,
    cbd_congestion_fee::float as cbd_congestion_fee
{% else %}
select 
    regexp_substr(source_file_name, 'green_tripdata_(\\d{4})_\\d{2}\\.parquet', 1, 1, '', 1) as file_year,
    regexp_substr(source_file_name, 'green_tripdata_\\d{4}_(\\d{2})\\.parquet', 1, 1, '', 1) as file_month,
    data:VendorID::int as vendor_id, 
    (to_timestamp(data:lpep_pickup_datetime::number / 1e6))::datetime as pickup_datetime, 
    (to_timestamp(data:lpep_dropoff_datetime::number / 1e6))::datetime as dropoff_datetime,
    data:PULocationID::int as pickup_location_id, 
    data:DOLocationID::int as dropoff_location_id,
    data:RatecodeID::int as ratecode_id,
    data:trip_type::int as trip_type,
    data:store_and_fwd_flag::varchar as store_and_fwd_flag,
    data:payment_type::int as payment_type,
    data:passenger_count::int as passenger_count, 
    data:trip_distance::float as trip_distance,
    data:fare_amount::float as fare_amount,
    data:extra::float as extra,
    data:mta_tax::float as mta_tax,
    data:tip_amount::float as tip_amount,
    data:tolls_amount::float as tolls_amount,
    data:improvement_surcharge::float as improvement_surcharge,
    data:congestion_surcharge::float as congestion_surcharge,
    data:Ehail_fee::float as ehail_fee, 
    data:total_amount::float as total_amount,
    data:cbd_congestion_fee::float as cbd_congestion_fee
{% endif %}
from stg_green