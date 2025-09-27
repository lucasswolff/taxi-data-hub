# TAXI DATA HUB
This project was created with the goal to create a complete data warehouse using market best practices.
It starts with the ingestion of raw data from multiple sources, until the vizualization in dashboard.

The main datasource can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. 
This source has data at a high granularity for each taxi trip in New York. With millions of trips per month, it makes this data very suitable for a project like this.

The data was stored and transformed in Snowflake, using dbt as a tool for transformation, python for ingestion and airflow for orchestration. 
Below the details of it.

## Data Flow Diagram
fazer no draw.io

## Data Ingestion
The taxi data in the source is stored in the parquet format. The first step is to download this data and send it to snowflake.
For this a data pipeline was created using the PUT and COPY commands from Snowflake. That can be found at scripts/snf_ingestion_raw.py
In this script the data is downloaded to a temporary storage on docker, PUT into snowflake stage and then COPY INTO the raw table in snowflake.
At this point, the table is on a semi-structured format, and later will be handled by the dbt transformation.

Another important data is weather data, which can be used for comparision between the weather and taxi trips (e.g. does the number of trips increase when raining?).
The data captured using the openmeteo api, which is handled by python using pandas, and then saved to Snowflake using snowpark. 
https://open-meteo.com/

Lastly, a lockup table for taxi locations - which will be used to bring zone and bourugh information - is saved to dbt seeds using python.

## Data Transformation
Having all the data stored either in snowflake raw schema or seeds, it's time do an important (and very cool) step in our project: the data transformation.
For that, the chosen tool was dbt (data build tool). DBT is a SQL-based tool, but with some powerfull engineering, testing and documentation capabilities. 

1a) From the raw layer, the data is sent to a Staging Layer, selecting the columns and datatypes into a tabular format. 

1b) From the seeds (lockup tables) we create the DIMs (dimensional tables).

2)  Then an intermediete (INT) step is done to clean the data, filter it and join the data in Staging with DIM.
   
3)  From this cleaned and integrated layer, the data is materilized into two Fact tables: green taxi and yellow taxi
   
4)  Both are then unioned to have a unified view of the taxi trips

   
5)  Lastly the layer MART is created, there's where all the business logics exist and can be used for analyzes, KPIs and machine learning. 
    For example, the outliers are flagged (e.g. very high fare, which probably is an error), some KPIs are created and we bring the weather data

You can see below you can see the described data flow: 

<img width="1851" height="660" alt="dbt-dag (1)" src="https://github.com/user-attachments/assets/b2d12fe4-460b-446c-8940-99a3b6b2a111" />
<img width="1851" height="660" alt="dbt-dag (2)" src="https://github.com/user-attachments/assets/21bd1fe6-f8be-4537-8a41-936ad9a47496" />


## Data Testing
DBT also provide an easy and modular way to test the data. 
Several tests (100+) are done in order to check for nulls, wrong column type, zeros or negatives where could not exit, etc.

## Orchestration
The orchestartion tool used is Airflow for the reasons of being open-source, having an extensive online community and integrate well with dbt.
The flow is similar to what was described above: first it runs the scripts to ingest data into snowflake.
Once all are completed without error, the dbt pipeline runs. This pipeline was broken into several tasks.
So it make sure the upstream layers is completed and tested before the next downstream runs. 

## Local development
In order to reduce the cloud cost of running all development in snowflake, the development was using a local instance of duckdb.
The downside is that the sintax from duckdb in some cases is different from snowflake, throwing unexpected errors in Production.  
Gladly dbt can be very helpful in these cases, since it allows to easily and modularly change your code if working in prod or dev.

## Conclusion
**1)** Overall tech stack feedback
Dbt is a really cool and powerfull tool to work it.
It make things easy to test and mentioned above, and helps during the documentation process. 
The lineage provided can also be very helpful for troubleshooting or for overall understanding. 
And it's very good to prevent repetition and make the code more modular. One example in this project is that the tranformation for cleaning and filtering the yellow taxi and the green taxi are very similar, but with some differences. Dbt provide the macros feature, which allows the developer to reuse the code and adapt if necessary (e.g. change just one part if yellow or green taxi). 

Aiflow ...

**2)** DBT advantage over spark for data transformation 
This project is quite similar to another using the same data developed by me https://github.com/lucasswolff/taxi-data-pipeline.
The project used spark instead of dbt and was mostly hosted in aws.

I noticed that dbt was a lot easier to work with and to develop the whole pipeline.
A great advantage is for testing the data. While in spark a whole function needed to be created for a simple test (e.g. check for nulls), in dbt that can be done with a few words.
Also, since the transformation is done in the warehouse, there's no need to worry about the hosting of the transformation layer.
Another advantage, in this case for me, is that since I greatly more experienced with SQL than spark, it made the learning curve very short compared to spark. And that might be the case for several analysts or data engineering teams.

Of course, spark still has its advantage for working with raw data and its parallel processing capabilities makes it very suitable for massive data transfers/transformations.
But once the data is loaded into the warehouse, and if the data movement is not massive (i.e. less than 1TB per hour), dbt is very likely to the be a better tool for the job.

**3)** Duckdb for local development vs developing in snowflake
