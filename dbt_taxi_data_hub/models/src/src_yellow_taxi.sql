{% if target.name == 'dev' %}
    select *
    from read_parquet('../raw_data/yellow/*.parquet', union_by_name = true, filename=true)
{% else %}
    select *
    from raw.yellow_taxi_raw
{% endif %}
