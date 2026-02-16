{{ config(materialized='table') }}

with vendor_ids as (
    select distinct vendor_id from {{ ref('fct_trips') }}
)

select
    vendor_id,
    {{ get_vendor_data('vendor_id') }} as vendor_name
from vendor_ids
