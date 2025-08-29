{{ config(materialized = 'view') }}

with seed_vendor as (
    select * from {{ ref('seed_vendor') }}
)
select 
    vendor_id::int as vendor_id,  
    vendor_desc::varchar as vendor_desc
from seed_vendor