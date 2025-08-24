{% test first_of_month_date(model, column_name) %}

select *
from {{ model }}
where extract(day from {{ column_name }}) != 1

{% endtest %}
