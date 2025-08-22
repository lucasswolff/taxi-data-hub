{% macro clean_taxi(source_model, taxi_type) %}

with base as (
    select *
        {% if taxi_type == 'yellow' %}
            , 3 as trip_type
        {% endif %}
    from {{ ref(source_model) }}
),
correct_total_amount as (
    select
        * exclude(total_amount),
        coalesce(fare_amount,0) 
            + coalesce(extra,0) 
            + coalesce(mta_tax,0)
            + coalesce(tip_amount,0) 
            + coalesce(tolls_amount,0) 
            + coalesce(improvement_surcharge,0)
            + coalesce(congestion_surcharge,0)
        {% if taxi_type == 'yellow' %}
            + coalesce(airport_fee,0)
        {% endif %}
        {% if taxi_type == 'green' %}
            + coalesce(ehail_fee,0)
        {% endif %}
        + coalesce(cbd_congestion_fee,0)
        as total_amount
    from base
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
            when dropoff_datetime < pickup_datetime then pickup_datetime
            else dropoff_datetime
        end as dropoff_datetime
    from correct_timestamp_step1
),
replace_nulls_and_zeros as (
    select
        (file_year || '-' || file_month || '-01')::date as file_date,
        coalesce(vendor_id,99) as vendor_id,
        coalesce(trip_type,4) as trip_type,
        coalesce(ratecode_id,99) as ratecode_id,
        coalesce(store_and_fwd_flag,'N') as store_and_fwd_flag,
        coalesce(payment_type,5) as payment_type,
        coalesce(pickup_location_id,264) as pickup_location_id,
        coalesce(dropoff_location_id,264) as dropoff_location_id,
        coalesce(pickup_datetime, (file_year || '-' || file_month || '-01')::timestamp) as pickup_datetime,
        coalesce(dropoff_datetime, (file_year || '-' || file_month || '-01')::timestamp) as dropoff_datetime,
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
        {% if taxi_type == 'yellow' %}
            coalesce(airport_fee, 0) as airport_fee,
        {% endif %}
        {% if taxi_type == 'green' %}
            coalesce(ehail_fee, 0) as ehail_fee,
        {% endif %}
        coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
        coalesce(total_amount, 0) as total_amount,
        file_year,
        file_month
    from correct_timestamp_step2
),
final_calculations as (
    select 
        *,  
        round(extract(epoch from (dropoff_datetime - pickup_datetime)) / 60.0, 2) as trip_duration_min,
        '{{ taxi_type }}' as taxi_type
    from replace_nulls_and_zeros
)
select 
    {{ dbt_utils.generate_surrogate_key([
        'taxi_type', 
        'vendor_id', 
        'pickup_datetime', 
        'dropoff_datetime',
        'trip_distance',
        'fare_amount',
        'pickup_location_id',
        'dropoff_location_id'
    ]) }} as trip_sk,
    *
from final_calculations

{% endmacro %}
