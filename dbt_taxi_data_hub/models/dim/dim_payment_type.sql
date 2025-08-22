with seed_payment_type as (
    select * from {{ ref('seed_payment_type') }}
)
select 
    payment_type::int as payment_type, 
    payment_type_desc::varchar as payment_type_desc
from seed_payment_type