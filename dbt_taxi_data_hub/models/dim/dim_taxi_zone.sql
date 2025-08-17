{{ config(materilized='table') }}

WITH seed_taxi_zone AS (
    SELECT * FROM {{ ref('seed_taxi_zone') }}
)
SELECT 
    LocationID::INT AS location_id,  
    Borough::VARCHAR AS borough,
    "Zone"::VARCHAR AS "zone",
    service_zone::VARCHAR AS service_zone
FROM seed_taxi_zone