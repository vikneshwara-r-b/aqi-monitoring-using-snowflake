-- change context
use role sysadmin;
use schema aqi_monitoring_db.bronze;
use warehouse adhoc_wh;


-- create an internal stage and enable directory service
create stage if not exists landing_zone
directory = ( enable = true)
comment = 'all the air quality raw data will store in this internal stage location';


 -- create file format to process the JSON file
  create file format if not exists json_file_format 
      type = 'JSON'
      compression = 'AUTO' 
      comment = 'this is json file format object';


  show stages;

  -- run the list command to check it
  list @landing_zone;

  -- level-1
select 
    * 
from 
    @aqi_monitoring_db.bronze.landing_zone
    (file_format => JSON_FILE_FORMAT) t;

  -- JSON file analysis using json editor
  -- level-2
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version  
    from @aqi_monitoring_db.bronze.landing_zone
    (file_format => JSON_FILE_FORMAT) t;

-- level3
select 
    Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
    t.$1,
    t.$1:total::int as record_count,
    t.$1:version::text as json_version,
    -- meta data information
    metadata$filename as _stg_file_name,
    metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY as _stg_file_md5,
    current_timestamp() as _copy_data_ts

from @aqi_monitoring_db.bronze.landing_zone
(file_format => JSON_FILE_FORMAT) t;
  
-- creating a raw table to have air quality data
create or replace table raw_aqi (
    id int primary key autoincrement,
    index_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version text not null,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

  -- load the data that has been downloaded manually
COPY INTO raw_aqi (
    index_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
) 
FROM 
(
    SELECT 
        TRY_TO_TIMESTAMP(t.$1:records[0].last_update::TEXT, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
        t.$1,
        t.$1:total::INT AS record_count,
        t.$1:version::TEXT AS json_version,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
            
    FROM @aqi_monitoring_db.bronze.landing_zone AS t
)
FILE_FORMAT = (FORMAT_NAME = 'aqi_monitoring_db.bronze.JSON_FILE_FORMAT');

-- Create the Snowpipe
CREATE OR REPLACE PIPE copy_air_quality_data_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_aqi (
    index_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
) 
FROM 
(
    SELECT 
        TRY_TO_TIMESTAMP(t.$1:records[0].last_update::TEXT, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
        t.$1,
        t.$1:total::INT AS record_count,
        t.$1:version::TEXT AS json_version,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
            
    FROM @aqi_monitoring_db.bronze.landing_zone AS t
)
FILE_FORMAT = (FORMAT_NAME = 'aqi_monitoring_db.bronze.JSON_FILE_FORMAT');

-- Show the pipe's notification channel
SHOW PIPES LIKE 'copy_air_quality_data_pipe';


-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('copy_air_quality_data_pipe');

-- Manually trigger pipe for existing files (if needed)
-- ALTER PIPE copy_air_quality_data_pipe REFRESH;

-- Pause/Resume pipe
-- ALTER PIPE copy_air_quality_data_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
-- ALTER PIPE copy_air_quality_data_pipe SET PIPE_EXECUTION_PAUSED = FALSE;


-- check the data
select *
    from raw_aqi
    limit 10;

-- select with ranking
select 
    index_record_ts,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5 ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from raw_aqi 
order by index_record_ts desc
limit 10;