-- Create a role for dbt
CREATE ROLE dbt_role;

CREATE DATABASE taxi_data_hub;

-- Create a new user
CREATE USER dbt_user
  PASSWORD = '<password>'
  LOGIN_NAME = dbt_user
  DEFAULT_ROLE = dbt_role
  DEFAULT_WAREHOUSE = compute_wh
  DEFAULT_NAMESPACE = dbt_db.public
  MUST_CHANGE_PASSWORD = FALSE;

-- Grant role to user
GRANT ROLE dbt_role TO USER dbt_user;

-- Grant privileges to the role
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dbt_role;
GRANT USAGE ON DATABASE taxi_data_hub TO ROLE dbt_role;

USE DATABASE taxi_data_hub;

GRANT CREATE SCHEMA ON DATABASE taxi_data_hub TO ROLE dbt_role;

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fct;
CREATE SCHEMA IF NOT EXISTS int;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS raw;

GRANT USAGE ON SCHEMA taxi_data_hub.public TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.stg TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.dim TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.fct TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.int TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.mart TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.raw TO ROLE dbt_role;
GRANT USAGE ON SCHEMA taxi_data_hub.information_schema TO ROLE dbt_role;

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA taxi_data_hub.public TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.stg TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.dim TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.fct TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.int TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.mart TO ROLE dbt_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA  taxi_data_hub.raw TO ROLE dbt_role;
_role;

-- let dbt manage objects
GRANT MODIFY, MONITOR ON WAREHOUSE compute_wh TO ROLE dbt_role;
