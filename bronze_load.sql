--create table in bronze and ingest data
USE DATABASE nyc_trip_db;
USE SCHEMA bronze;

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

--fhv raw data
CREATE OR REPLACE TABLE bronze.fhv_trip_rawdata
AS
SELECT *, METADATA$FILENAME AS source_file
FROM @public.nyc_trip_stage (FILE_FORMAT => 'parquet_format')
WHERE METADATA$FILENAME ILIKE 'fhv_tripdata_2024-%';

--check tables
SELECT * FROM bronze.fhv_trip_rawdata;
SELECT * FROM bronze.green_trip_rawdata;
SELECT * FROM bronze.yellow_trip_rawdata;