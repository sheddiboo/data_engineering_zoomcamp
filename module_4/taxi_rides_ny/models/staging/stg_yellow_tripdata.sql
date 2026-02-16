{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'yellow_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(vendorid as integer) as vendor_id,
        {{ safe_cast('ratecodeid', 'integer') }} as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        1 as trip_type, -- Yellow taxis are always street-hail

        -- payment info
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        0 as ehail_fee,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(total_amount as double) as total_amount,
        {{ safe_cast('payment_type', 'integer') }} as payment_type

    from source
    where vendorid is not null
)

select * from renamed

-- Limit data for dev environment
{% if target.name == 'dev' %}
where pickup_datetime >= cast('2019-01-01' as timestamp)
  and pickup_datetime < cast('2019-02-01' as timestamp)
{% endif %}