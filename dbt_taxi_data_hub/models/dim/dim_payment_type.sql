{{ config(materilized='table') }}

WITH seed_payment_type AS (
    SELECT * FROM {{ ref('seed_payment_type') }}
)
SELECT 
    payment_type::INT AS payment_type, 
    payment_type_desc::VARCHAR AS payment_type_desc
FROM seed_payment_type