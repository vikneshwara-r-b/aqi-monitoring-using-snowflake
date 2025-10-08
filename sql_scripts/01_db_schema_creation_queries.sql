-- use sysadmin role and default warehouse.
use role sysadmin;
use warehouse compute_wh;

-- create a warehouse before you try this..

-- create development database/schema  if does not exist
create database if not exists aqi_monitoring_db;
create schema if not exists aqi_monitoring_db.bronze;
create schema if not exists aqi_monitoring_db.silver;
create schema if not exists aqi_monitoring_db.gold;
create schema if not exists aqi_monitoring_db.consumption;

show schemas in database aqi_monitoring_db;
-- visit the object explorer home page from webui.

-- having load_wh warehouse
create warehouse if not exists load_wh
     comment = 'this is load warehosue for loading all the JSON files'
     warehouse_size = 'medium' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- all the ETL workload will be manage by it.
create warehouse if not exists transform_wh
     comment = 'this is ETL warehouse for all loading activity' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- specific virtual warehouse with differt resume time (for streamlit, it should be longer)
 create warehouse if not exists streamlit_wh
     comment = 'this is streamlit virtual warehouse' 
     warehouse_size = 'x-small' 
     auto_resume = true
     auto_suspend = 600 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- having adhoc warehouse
create warehouse if not exists adhoc_wh
     comment = 'this is adhoc warehosue for all adhoc & development activities' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

show warehouses;
-- visit the warehouse home page from webui.