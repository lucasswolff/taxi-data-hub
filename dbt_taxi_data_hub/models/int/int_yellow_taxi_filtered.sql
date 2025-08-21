-- mudar para macro
with base as (
    select * from int_yellow_taxi_cleansed
),
reversals as (
    select vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, trip_distance, 1 as index_
    from base
    group by all
    having sum(total_amount) = 0 and count(*) = 2
),
filter_reversal as ( -- filter reversal (e.g. 10 + (-10) = 0 --> -10 is the reversal of 10)
    select b.* 
    from base b
    left join reversals r
        on b.vendor_id = r.vendor_id
    and b.pickup_datetime = r.pickup_datetime
    and b.dropoff_datetime = r.dropoff_datetime
    and b.pickup_location_id = r.pickup_location_id
    and b.dropoff_location_id = r.dropoff_location_id
    and b.trip_distance = r.trip_distance
    where r.index_ is null  -- anti-join
),
filter_zero_negative as (
    select * from filter_reversal
    where 
        not (trip_distance = 0 and total_amount = 0) -- remove when both are zero
        and trip_distance >= 0
        and total_amount >= 0
        and fare_amount >= 0
),
filter_duplicates as (
    select 
        *,
        count(*) over (partition by trip_sk order by total_amount desc) as row_number
    from int_yellow_taxi_cleansed 
    qualify row_number = 1
)
select * exclude(row_number)
from filter_duplicates