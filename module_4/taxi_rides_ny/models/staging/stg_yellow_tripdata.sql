{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers
        try_cast(vendorid as integer) as vendor_id,
        try_cast(ratecodeid as integer) as rate_code_id,
        try_cast(pulocationid as integer) as pickup_location_id,
        try_cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        try_cast(passenger_count as integer) as passenger_count,
        try_cast(trip_distance as double) as trip_distance,
        -- Yellow taxis are always Street Hail (1), so we hardcode this
        1 as trip_type, 

        -- payment info
        try_cast(fare_amount as double) as fare_amount,
        try_cast(extra as double) as extra,
        try_cast(mta_tax as double) as mta_tax,
        try_cast(tip_amount as double) as tip_amount,
        try_cast(tolls_amount as double) as tolls_amount,
        0 as ehail_fee,
        try_cast(improvement_surcharge as double) as improvement_surcharge,
        try_cast(total_amount as double) as total_amount,
        try_cast(payment_type as integer) as payment_type

    from source
    -- Safety check: drop rows where vendorid cannot be parsed
    -- where try_cast(vendorid as integer) is not null  <-- COMMENTED OUT TO KEEP ALL DATA
)

select * from renamed

-- Limit data for dev environment
{% if target.name == 'dev' %}
  limit 100
{% endif %}