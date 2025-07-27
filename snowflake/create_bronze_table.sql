create or replace database taxi_db_practice;
use database taxi_db_practice;
create or replace schema Bronze;
use schema Bronze;


create or replace TABLE TAXI_DB_PRACTICE.BRONZE.RAW (
	VENDORID BIGINT,
	TPEP_PICKUP_DATETIME timestamp_tz,
	TPEP_DROPOFF_DATETIME timestamp_tz,
	PASSENGER_COUNT BIGINT,
	TRIP_DISTANCE DOUBLE,
	RATECODEID BIGINT,
	STORE_AND_FWD_FLAG VARCHAR(16777216),
	PULOCATIONID BIGINT,
	DOLOCATIONID BIGINT,
	PAYMENT_TYPE BIGINT,
	FARE_AMOUNT double,
	EXTRA DOUBLE,
	MTA_TAX DOUBLE,
	TIP_AMOUNT DOUBLE,
	TOLLS_AMOUNT DOUBLE,
	IMPROVEMENT_SURCHARGE DOUBLE,
	TOTAL_AMOUNT DOUBLE,
	CONGESTION_SURCHARGE INT,
	AIRPORT_FEE INT
);


----------------- S3  INTEGRATION -------------------
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration_raw
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'your-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-bronze-demo/raw/');

--------- DESCRIBING INTEGRATION --------------------
DESC INTEGRATION my_s3_integration_raw;
--------- CREATING EXT STAGE  FOR BRONZE -----------------

CREATE OR REPLACE STAGE TAXI_DB_PRACTICE.BRONZE.ext1_stage_taxi_s3_bronze
URL = 's3://nyc-bronze-demo/raw/'
STORAGE_INTEGRATION = my_s3_integration_raw
FILE_FORMAT = (TYPE = PARQUET);
--------------- LISTING THE EXT STAGE --------------
-- LIST @ext1_stage_taxi_s3_bronze;
----------------- creating the silver table in silver schema -------------
select * from TAXI_DB_PRACTICE.BRONZE.RAW limit 10; 

