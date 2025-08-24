select trip_sk, count(*)
from {{ ref('fct_yellow_green_taxi_union') }} 
group by trip_sk
having count(*) > 1
