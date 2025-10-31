--create table in bronze and ingest data
USE DATABASE nyc_trip_db;
USE SCHEMA bronze;

--create file format as parquet
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';

--create stored procedure
CREATE OR REPLACE PROCEDURE bronze.load_rawdata()
RETURNS STRING
AS
BEGIN
    --yellow trip data (raw)         
    CREATE OR REPLACE TABLE bronze.yellow_trip_rawdata
    AS
    SELECT *, METADATA$FILENAME AS source_file
    FROM @public.nyc_trip_stage (FILE_FORMAT => 'parquet_format')
    WHERE METADATA$FILENAME ILIKE 'yellow_tripdata_2024-%';

    --green raw data
    CREATE OR REPLACE TABLE bronze.green_trip_rawdata
    AS
    SELECT *, METADATA$FILENAME AS source_file
    FROM @public.nyc_trip_stage (FILE_FORMAT => 'parquet_format')
    WHERE METADATA$FILENAME ILIKE 'green_tripdata_2024-%';
END;

CALL bronze.load_rawdata()