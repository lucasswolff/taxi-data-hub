with base_yellow as (
    select *,
        3 as trip_type
    from {{ ref('stg_yellow_taxi') }}
),
correct_total_amount as (
    select
        * exclude(total_amount),
        coalesce(fare_amount,0) + coalesce(extra,0) + coalesce(mta_tax,0)
            + coalesce(tip_amount,0) + coalesce(tolls_amount,0) + coalesce(improvement_surcharge,0)
            + coalesce(congestion_surcharge,0) + coalesce(airport_fee,0) + coalesce(cbd_congestion_fee,0)
        as total_amount
    from base_yellow
),
correct_timestamp_step1 as (
    select * exclude(pickup_datetime, dropoff_datetime),
        -- correct pickup
        case
            -- case 1: pickup year matches file_year
            when extract(year from pickup_datetime) = file_year::int
                then pickup_datetime

            -- case 2: some data right before new year's midnight may flow into the next year file. We don't want to change the year in these cases
            when extract(year from pickup_datetime) = file_year::int - 1
                 and extract(month from pickup_datetime) = 12
                 and extract(day from pickup_datetime) = 31
                then pickup_datetime

            -- otherwise: rebuild datetime with file_year + original month/day/time
            else make_timestamp(
                     file_year::int,
                     extract(month from pickup_datetime),
                     extract(day from pickup_datetime),
                     extract(hour from pickup_datetime),
                     extract(minute from pickup_datetime),
                     extract(second from pickup_datetime)
                 )
        end as pickup_datetime,

        -- correct dropoff
        case
            -- case 1: dropoff year matches file_year
            when year(dropoff_datetime) = file_year::int
                then dropoff_datetime

            -- case 2: new year's eve trip starting prev year and dropping off jan 1 of file_year
            when year(pickup_datetime) = file_year::int - 1
                 and month(pickup_datetime) = 12
                 and day(pickup_datetime) = 31
                then dropoff_datetime

            -- case 3: new year's eve trip starting file_year and dropping off jan 1 of file_year+1
            when year(pickup_datetime) = file_year::int
                 and month(pickup_datetime) = 12
                 and day(pickup_datetime) = 31
                 and year(dropoff_datetime) = file_year::int + 1
                 and month(dropoff_datetime) = 1
                 and day(dropoff_datetime) = 1
                then dropoff_datetime

            -- otherwise: rebuild datetime with file_year
            else make_timestamp(
                     file_year::int,
                     month(dropoff_datetime),
                     day(dropoff_datetime),
                     hour(dropoff_datetime),
                     minute(dropoff_datetime),
                     second(dropoff_datetime)
                 )
        end as dropoff_datetime
    from correct_total_amount
),
correct_timestamp_step2 as (
    select * exclude(dropoff_datetime),
    case
        -- if dropoff is earlier than pickup, uses pickup (makes it a 0 minutes trip, but avoids negative)
        when dropoff_datetime < pickup_datetime then pickup_datetime
        else dropoff_datetime
    end as dropoff_datetime
    from correct_timestamp_step1
), 
replace_nulls_and_zeros as (
    select
        coalesce(vendor_id,99) as vendor_id,
        coalesce(trip_type,4) as trip_type,
        coalesce(ratecode_id,99) as ratecode_id,
        coalesce(store_and_fwd_flag,'N') as store_and_fwd_flag,
        coalesce(payment_type,5) as payment_type,
        coalesce(pickup_location_id,264) as pickup_location_id,
        coalesce(dropoff_location_id,264) as dropoff_location_id,
        coalesce(pickup_datetime, (file_year || '-' || file_month || '-01')::datetime) as pickup_datetime,
        coalesce(dropoff_datetime, (file_year || '-' || file_month || '-01')::datetime) as dropoff_datetime,
        case 
            when passenger_count is null or passenger_count = 0 then 1
            else passenger_count
        end as passenger_count,
        coalesce(trip_distance, 0) as trip_distance,
        coalesce(fare_amount, 0) as fare_amount,
        coalesce(extra, 0) as extra,
        coalesce(mta_tax, 0) as mta_tax,
        coalesce(tip_amount, 0) as tip_amount,
        coalesce(tolls_amount, 0) as tolls_amount,
        coalesce(improvement_surcharge, 0) as improvement_surcharge,
        coalesce(congestion_surcharge, 0) as congestion_surcharge,
        coalesce(airport_fee, 0) as airport_fee,
        coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
        coalesce(total_amount, 0) as total_amount,
        file_year,
        file_month
    from correct_timestamp_step2
)
select * from replace_nulls_and_zeros