{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'green_tripdata') }}
),

renamed as (
    select
        -- identifiers
        try_cast(vendorid as integer) as vendor_id,
        try_cast(ratecodeid as integer) as rate_code_id,
        try_cast(pulocationid as integer) as pickup_location_id,
        try_cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        try_cast(passenger_count as integer) as passenger_count,
        try_cast(trip_distance as double) as trip_distance,
        try_cast(trip_type as integer) as trip_type,

        -- payment info
        try_cast(fare_amount as double) as fare_amount,
        try_cast(extra as double) as extra,
        try_cast(mta_tax as double) as mta_tax,
        try_cast(tip_amount as double) as tip_amount,
        try_cast(tolls_amount as double) as tolls_amount,
        try_cast(ehail_fee as double) as ehail_fee,
        try_cast(improvement_surcharge as double) as improvement_surcharge,
        try_cast(total_amount as double) as total_amount,
        try_cast(payment_type as integer) as payment_type
        
    from source
    where try_cast(vendorid as integer) is not null
)

select * from renamed

-- Limit data for dev environment
{% if target.name == 'dev' %}
  limit 100
{% endif %}