{% if target.name == 'dev' %}
    select *
    from read_parquet('../raw_data/green/*.parquet', union_by_name = true, filename=true)
{% else %}
    select *
    from raw.green_taxi_raw
{% endif %}
