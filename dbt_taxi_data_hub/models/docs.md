{% docs int_yellow_taxi_cleansed %}

Intermediate step between staging and fact for yellow taxi.
Clean possible data errors, like correct total amount and timestamp,
replace nulls and zeros, and create surrogate key.

{% enddocs %}

{% docs int_green_taxi_cleansed %}

Intermediate step between staging and fact for green taxi.
Clean possible data errors, like correct total amount and timestamp,
replace nulls and zeros, and create surrogate key.

{% enddocs %}


{% docs int_yellow_taxi_filtered %}

Intermediate step between int_cleansed and fact for yellow taxi.  
Filters reversals, zero and negative values, and duplicates.

{% enddocs %}

{% docs int_green_taxi_filtered %}

Intermediate step between int_cleansed and fact for green taxi.  
Filters reversals, zero and negative values, and duplicates.

{% enddocs %}

{% docs int_yellow_taxi_integrated %}

Intermediate step between int_filtered and fact for yellow taxi.  
Joins with dims: payment_type, ratecode, trip_type, vendor, and taxi_zone.

{% enddocs %}

{% docs int_green_taxi_integrated %}

Intermediate step between int_filtered and fact for green taxi.  
Joins with dims: payment_type, ratecode, trip_type, vendor, and taxi_zone.

{% enddocs %}

{% docs fct_yellow_taxi %}

Standardized and integrated data for yellow taxi, cleansed and ready to serve analytics.

{% enddocs %}

{% docs fct_green_taxi %}

Standardized and integrated data for green taxi, cleansed and ready to serve analytics.

{% enddocs %}
