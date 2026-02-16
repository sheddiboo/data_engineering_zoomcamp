{{
    config(
        materialized='view'
    )
}}

with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

trips_with_rn as (
    select 
        *,
        row_number() over(
            partition by vendor_id, pickup_datetime 
            order by pickup_datetime
        ) as rn
    from trips_unioned
)

select
    -- Explicitly list all columns here instead of using EXCEPT
    trip_id,
    vendor_id,
    service_type,
    rate_code,
    pickup_location_id,
    pickup_datetime,
    dropoff_location_id,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description
from trips_with_rn
where rn = 1