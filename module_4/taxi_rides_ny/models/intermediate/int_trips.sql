{{ config(materialized='view') }}

with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

dim_payment_type as (
    select * from {{ ref('payment_type_lookup') }}
),

trips_with_rn as (
    select 
        trips_unioned.*,
        row_number() over(
            partition by vendor_id, pickup_datetime, pickup_location_id 
            order by pickup_datetime
        ) as rn
    from trips_unioned
)

select
    -- FIX: Use COALESCE(vendor_id, -1) so the ID doesn't become NULL
    cast(coalesce(vendor_id, -1) as varchar) || cast(pickup_datetime as varchar) || cast(pickup_location_id as varchar) || '-' || cast(rn as varchar) as trip_id,
    
    -- Select standard columns
    vendor_id,
    service_type,
    rate_code_id,
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
    
    -- Specify the table alias here to solve ambiguity
    trips_with_rn.payment_type, 

    -- Join correctly using the description
    coalesce(dim_payment_type.description, 'Unknown') as payment_type_description

from trips_with_rn
left join dim_payment_type 
    on trips_with_rn.payment_type = dim_payment_type.payment_type