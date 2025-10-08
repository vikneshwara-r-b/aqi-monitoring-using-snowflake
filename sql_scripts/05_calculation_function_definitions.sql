use role sysadmin;
use schema aqi_monitoring_db.gold;
use warehouse adhoc_wh;

create or replace function prominent_index(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
returns varchar
language python
runtime_version = '3.9'
handler = 'prominent_index'
AS ' 
def prominent_index(pm25, pm10, so2, no2, nh3, co, o3):
    # Handle None values by replacing them with 0
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    nh3 = nh3 if nh3 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0

    # Create a dictionary to map variable names to their values
    variables = {''PM25'': pm25, ''PM10'': pm10, ''SO2'': so2, ''NO2'': no2, ''NH3'': nh3, ''CO'': co, ''O3'': o3}
    
    # Find the variable with the highest value
    max_variable = max(variables, key=variables.get)
    
    return max_variable
';
-- testing
-- 	56,70 , 12	4	17	47	3
-- 	89,70 , 12	4	17	47	3
select prominent_index(56,70,12,4,17,47,3) ;

create or replace function three_sub_index_criteria(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
returns number(38,0)
language python
runtime_version = '3.9'
HANDLER = 'three_sub_index_criteria'
AS '
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3  ):
    pm_count = 0
    non_pm_count = 0

    if pm25 is not None and pm25 > 0:
        pm_count = 1
    elif pm10 is not None and pm10 > 0:
        pm_count = 1

    non_pm_count = min(2, sum(p is not None and p != 0 for p in [so2, no2, nh3, co, o3]))

    return pm_count + non_pm_count
';