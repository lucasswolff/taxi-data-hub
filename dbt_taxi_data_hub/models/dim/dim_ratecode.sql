{{ config(materilized='table') }}

WITH seed_ratecode AS (
    SELECT * FROM {{ ref('seed_ratecode') }}
)
SELECT 
    RatecodeID::INT AS ratecode_id,  
    RatecodeDesc::VARCHAR AS ratecode_desc
FROM seed_ratecode