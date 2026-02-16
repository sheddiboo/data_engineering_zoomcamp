{{ config(materialized='view') }}

with unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    -- Assuming you created this seed or table earlier
    select * from {{ ref('payment_type_lookup') }}
),

enriched as (
    select
        -- Generate unique trip identifier
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,

        u.vendor_id,
        u.service_type,
        u.rate_code_id,
        u.pickup_location_id,
        u.dropoff_location_id,
        u.pickup_datetime,
        u.dropoff_datetime,
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description,
        
        -- Row number for deduplication
        row_number() over(
            partition by u.vendor_id, u.pickup_datetime, u.pickup_location_id, u.service_type
            order by u.dropoff_datetime
        ) as rn

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
)

-- Filter for first record to replace QUALIFY
select 
    * except(rn) 
from enriched 
where rn = 1