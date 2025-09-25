FROM astrocrpublic.azurecr.io/runtime:3.0-10

RUN pip install "dbt-core== 1.10.7" "dbt-duckdb==1.9.4" "dbt-snowflake==1.10.0"