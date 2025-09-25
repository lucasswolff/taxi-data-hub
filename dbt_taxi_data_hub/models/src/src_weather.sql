{% if target.name == 'dev' %}
    select *
    from read_parquet('../raw_data/weather/weather_api.parquet')
{% else %}
    select *
    from raw.weather_raw
{% endif %}
