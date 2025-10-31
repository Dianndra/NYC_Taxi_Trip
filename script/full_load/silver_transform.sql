USE DATABASE nyc_trip_db;
USE SCHEMA silver;
------------------------------------------------------
/*
TRANSFORMATION PROCESS
1. Flatten and structure data
2. Standardize datatype
3. 
*/
------------------------------------------------------
--1. Flatten each table from bronze to show all columns 

--get columns from the yellow trip raw data and create a flattened table from the raw data
CREATE OR REPLACE TABLE silver.yellow_trip_cleandata (
    vendor_id NUMBER(38,0),
    pickup_datetime NUMBER(38,0),
    dropoff_datetime NUMBER(38,0),
    passenger_count NUMBER(38,0),
    trip_distance FLOAT,
    rate_code_id NUMBER(38,0),
    store_and_fwd_flag VARCHAR,
    pickup_location_id NUMBER(38,0),
    dropoff_ocation_id NUMBER(38,0),
    payment_type NUMBER(38,0),
    fare_amount FLOAT,
    extra FLOAT,
    meter_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    trip_type NUMBER(38, 0),
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    source_file VARCHAR
    ); 

INSERT INTO silver.yellow_trip_cleandata
SELECT 
    $1:VendorID::NUMBER(38, 0), 
    $1:tpep_pickup_datetime::NUMBER(38, 0), 
    $1:tpep_dropoff_datetime::NUMBER(38, 0), 
    $1:passenger_count::NUMBER(38, 0), 
    $1:trip_distance::FLOAT, 
    $1:RatecodeID::NUMBER(38, 0), 
    $1:store_and_fwd_flag::VARCHAR, 
    $1:PULocationID::NUMBER(38, 0), 
    $1:DOLocationID::NUMBER(38, 0), 
    $1:payment_type::NUMBER(38, 0), 
    $1:fare_amount::FLOAT, 
    $1:extra::FLOAT, 
    $1:mta_tax::FLOAT, 
    $1:tip_amount::FLOAT, 
    $1:tolls_amount::FLOAT,
    $1:trip_type::NUMBER(38, 0),
    $1:improvement_surcharge::FLOAT, 
    $1:total_amount::FLOAT, 
    $1:congestion_surcharge::FLOAT, 
    $1:Airport_fee::FLOAT, 
    source_file::VARCHAR
FROM bronze.yellow_trip_rawdata;


--get columns from the green trip raw data and create a flattened table from the raw data
CREATE OR REPLACE TABLE silver.green_trip_cleandata (
    vendor_id NUMBER(38, 0), 
    pickup_datetime NUMBER(38, 0),
    dropoff_datetime NUMBER(38, 0),
    store_and_fwd_flag VARCHAR,
    rate_code_id NUMBER(38, 0), 
    pickup_location_id NUMBER(38, 0) ,
    dropoff_location_id NUMBER(38, 0), 
    passenger_count NUMBER(38, 0),
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    meter_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    payment_type NUMBER(38, 0),
    trip_type NUMBER(38, 0),
    congestion_surcharge FLOAT,
    source_file VARCHAR
    ); 

INSERT INTO silver.green_trip_cleandata
SELECT 
    $1:VendorID::NUMBER(38, 0), 
    $1:lpep_pickup_datetime::NUMBER(38, 0), 
    $1:lpep_dropoff_datetime::NUMBER(38, 0), 
    $1:store_and_fwd_flag::VARCHAR, 
    $1:RatecodeID::NUMBER(38, 0), 
    $1:PULocationID::NUMBER(38, 0), 
    $1:DOLocationID::NUMBER(38, 0), 
    $1:passenger_count::NUMBER(38, 0), 
    $1:trip_distance::FLOAT, 
    $1:fare_amount::FLOAT, 
    $1:extra::FLOAT, 
    $1:mta_tax::FLOAT, 
    $1:tip_amount::FLOAT, 
    $1:tolls_amount::FLOAT, 
    $1:improvement_surcharge::FLOAT,
    $1:total_amount::FLOAT, 
    $1:payment_type::NUMBER(38, 0), 
    $1:trip_type::NUMBER(38, 0), 
    $1:congestion_surcharge::FLOAT,
    source_file::VARCHAR
FROM bronze.green_trip_rawdata;

--get columns from the fhv trip raw data and create a flattened table from the raw data
CREATE OR REPLACE TABLE silver.fhv_trip_cleandata (
    dispatching_base_num VARCHAR, 
    pickup_datetime NUMBER(38, 0), 
    dropoff_datetime NUMBER(38, 0),
    pickup_location_id NUMBER(38, 0),
    dropoff_location_id NUMBER(38, 0), 
    shared_ride_flag NUMBER(38, 0), 
    affiliated_base_number VARCHAR,
    source_file VARCHAR
    )
    ; 

INSERT INTO silver.fhv_trip_cleandata 
SELECT 
    $1:dispatching_base_num::VARCHAR, 
    $1:pickup_datetime::NUMBER(38, 0), 
    $1:dropOff_datetime::NUMBER(38, 0), 
    $1:PUlocationID::NUMBER(38, 0), 
    $1:DOlocationID::NUMBER(38, 0), 
    $1:SR_Flag::NUMBER(38, 0), 
    $1:Affiliated_base_number::VARCHAR,
    source_file::VARCHAR
FROM bronze.fhv_trip_rawdata;

---------------------------------------------------------------------
--2. Standardize data type

--green_trip_cleandata
--PICKUP_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.green_trip_cleandata
ADD COLUMN PICKUP_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.green_trip_cleandata
SET PICKUP_DATETIME_TS = TO_TIMESTAMP_NTZ(PICKUP_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.green_trip_cleandata DROP COLUMN PICKUP_DATETIME;
ALTER TABLE silver.green_trip_cleandata RENAME COLUMN PICKUP_DATETIME_TS TO PICKUP_DATETIME;

--DROPOFF_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.green_trip_cleandata
ADD COLUMN DROPOFF_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.green_trip_cleandata
SET DROPOFF_DATETIME_TS = TO_TIMESTAMP_NTZ(DROPOFF_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.green_trip_cleandata DROP COLUMN DROPOFF_DATETIME;
ALTER TABLE silver.green_trip_cleandata RENAME COLUMN DROPOFF_DATETIME_TS TO DROPOFF_DATETIME;

--yellow_trip_cleandata
--PICKUP_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.yellow_trip_cleandata
ADD COLUMN PICKUP_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.yellow_trip_cleandata
SET PICKUP_DATETIME_TS = TO_TIMESTAMP_NTZ(PICKUP_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.yellow_trip_cleandata DROP COLUMN PICKUP_DATETIME;
ALTER TABLE silver.yellow_trip_cleandata RENAME COLUMN PICKUP_DATETIME_TS TO PICKUP_DATETIME;

--DROPOFF_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.yellow_trip_cleandata
ADD COLUMN DROPOFF_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.yellow_trip_cleandata
SET DROPOFF_DATETIME_TS = TO_TIMESTAMP_NTZ(DROPOFF_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.yellow_trip_cleandata DROP COLUMN DROPOFF_DATETIME;
ALTER TABLE silver.yellow_trip_cleandata RENAME COLUMN DROPOFF_DATETIME_TS TO DROPOFF_DATETIME;

--fhv_trip_cleandata
--PICKUP_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.fhv_trip_cleandata
ADD COLUMN PICKUP_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.fhv_trip_cleandata
SET PICKUP_DATETIME_TS = TO_TIMESTAMP_NTZ(PICKUP_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.fhv_trip_cleandata DROP COLUMN PICKUP_DATETIME;
ALTER TABLE silver.fhv_trip_cleandata RENAME COLUMN PICKUP_DATETIME_TS TO PICKUP_DATETIME;

--DROPOFF_DATETIMEDROPOFF_DATETIME
--create new column in table as timestamp
ALTER TABLE silver.fhv_trip_cleandata
ADD COLUMN DROPOFF_DATETIME_TS TIMESTAMP_NTZ;

--update column to standardize date
UPDATE silver.fhv_trip_cleandata
SET DROPOFF_DATETIME_TS = TO_TIMESTAMP_NTZ(DROPOFF_DATETIME/1000000);

--drop old column and change column name of new column
ALTER TABLE silver.fhv_trip_cleandata DROP COLUMN DROPOFF_DATETIME;
ALTER TABLE silver.fhv_trip_cleandata RENAME COLUMN DROPOFF_DATETIME_TS TO DROPOFF_DATETIME;

--change STORE_AND_FWD_FLAG to bool in green and yellow data
--yellow clean data
ALTER TABLE silver.yellow_trip_cleandata
ADD COLUMN STORE_AND_FWD_FLAG_BOOL BOOLEAN;

UPDATE silver.yellow_trip_cleandata
SET STORE_AND_FWD_FLAG_BOOL = 
    CASE 
        WHEN STORE_AND_FWD_FLAG = 'Y' THEN TRUE
        WHEN STORE_AND_FWD_FLAG = 'N' THEN FALSE
        ELSE NULL
    END;
    
ALTER TABLE silver.yellow_trip_cleandata DROP COLUMN STORE_AND_FWD_FLAG;

ALTER TABLE silver.yellow_trip_cleandata 
RENAME COLUMN STORE_AND_FWD_FLAG_BOOL TO STORE_AND_FWD_FLAG;

--green clean data
ALTER TABLE silver.green_trip_cleandata
ADD COLUMN STORE_AND_FWD_FLAG_BOOL BOOLEAN;

UPDATE silver.green_trip_cleandata
SET STORE_AND_FWD_FLAG_BOOL = 
    CASE 
        WHEN STORE_AND_FWD_FLAG = 'Y' THEN TRUE
        WHEN STORE_AND_FWD_FLAG = 'N' THEN FALSE
        ELSE NULL
    END;
    
ALTER TABLE silver.green_trip_cleandata DROP COLUMN STORE_AND_FWD_FLAG;

ALTER TABLE silver.green_trip_cleandata 
RENAME COLUMN STORE_AND_FWD_FLAG_BOOL TO STORE_AND_FWD_FLAG;

/*
--fhv clean data - shared ride to bool
ALTER TABLE silver.fhv_trip_cleandata
ADD COLUMN SHARED_RIDE_FLAG_BOOL BOOLEAN;

UPDATE silver.fhv_trip_cleandata
SET SHARED_RIDE_FLAG_BOOL = 
    CASE 
        WHEN SHARED_RIDE_FLAG = 1 THEN TRUE
        WHEN SHARED_RIDE_FLAG = NULL THEN FALSE
        ELSE NULL
    END;
    
ALTER TABLE silver.fhv_trip_cleandata DROP COLUMN SHARED_RIDE_FLAG;

ALTER TABLE silver.fhv_trip_cleandata 
RENAME COLUMN SHARED_RIDE_FLAG_BOOL TO SHARED_RIDE_FLAG;

*/