# Layer Order (Source → Target)

src (Source) - Raw data sources. Materialization = View
stg (Staging) - Light transformations and cleaning. Materialization = View
int (Intermediate) - Business logic and complex transformations. Materialization = View (table if heavy transformation)
dim (Dimensions) - Dimension tables for star schema. Materialization = Table
fct (Facts) - Fact tables for star schema. Materialization = Incremental
mart (Data Marts) - Final business-ready datasets. Materialization = Table if connected to dashboard. View if light join/filter on top of dim and fct

# Detailed Layer Descriptions
## Sources (src_)

Raw data from external systems (databases, APIs, files)
Defined in sources.yml files
No transformations, just documentation and testing

## Staging (stg_)

One-to-one with source tables
Light cleaning: renaming columns, casting data types, basic filtering
Standardizes column names and formats
Foundation for all downstream models

## Intermediate (int_)

Business logic implementations
Joins between staging models
Complex calculations and aggregations
Not exposed to end users

## Dimensions (dim_)

Descriptive attributes for business entities
Slowly changing dimensions (SCD) logic
Customer, product, location tables
Support star schema design

## Facts (fct_)

Event or transaction tables
Measures and metrics
References to dimension tables
Core of star schema design

## Marts (mart_)

Final, business-ready datasets
Optimized for specific use cases
What analysts and BI tools consume