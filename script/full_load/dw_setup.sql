<<<<<<< HEAD
--create db
CREATE OR REPLACE DATABASE nyc_trip_db;
USE DATABASE nyc_trip_db;

--create staging area
CREATE OR REPLACE STAGE nyc_trip_stage;


--create file format as parquet
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';



--insert data into stage using snowinsight 
-- go to catalogue>Nyc_trip_db>public>stage
--double click on stage name and add file with the file button at top right

list @public.nyc_trip_stage;

--create schema 
CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA gold;
=======
--create db
CREATE OR REPLACE DATABASE nyc_trip_db;
USE DATABASE nyc_trip_db;

--create staging area
CREATE OR REPLACE STAGE nyc_trip_stage;

--insert data into stage using snowinsight 
-- go to catalogue>Nyc_trip_db>public>stage
--double click on stage name and add file with the file button at top right

--create schema 
CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA gold;
>>>>>>> def9b2e (First commit)
