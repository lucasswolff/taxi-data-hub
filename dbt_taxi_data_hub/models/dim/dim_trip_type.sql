{{ config(materilized='table') }}

WITH seed_trip_type AS (
    SELECT * FROM {{ ref('seed_trip_type') }}
)
SELECT 
    trip_type::INT AS trip_type,  
    trip_type_desc::VARCHAR AS trip_type_desc
FROM seed_trip_type