create or replace database taxi_db_practice;
use database taxi_db_practice;

create or replace schema Silver;

CREATE or replace TABLE TAXI_DB_PRACTICE.SILVER.CLEANED(
    PUBorough                 VARCHAR,
    DOBorough                 VARCHAR,
    PULocationID              BIGINT,
    DOLocationID              BIGINT,
    VendorID                  BIGINT,
    tpep_pickup_datetime      TIMESTAMP_TZ,
    tpep_dropoff_datetime     TIMESTAMP_TZ,
    passenger_count           VARCHAR,
    trip_distance             FLOAT,
    RatecodeID                VARCHAR,
    store_and_fwd_flag        VARCHAR,
    payment_type              FLOAT,
    fare_amount               FLOAT,
    extra                     FLOAT,
    mta_tax                   FLOAT,
    tip_amount                FLOAT,
    tolls_amount              FLOAT,
    improvement_surcharge     FLOAT,
    total_amount              FLOAT,
    PUZone                    VARCHAR,
    PUservice_zone            VARCHAR,
    DOZone                    VARCHAR,
    DOservice_zone            VARCHAR,
    trip_duration             FLOAT,
    speed                     FLOAT,
    pickup_date               DATE,
    pickup_time               VARCHAR,
    dropoff_date              DATE,
    dropoff_time              VARCHAR
);
----- creating s3 integration  to connect external stage - for silver -----------
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration_Processed
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'your-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-bronze-demo/processed/');

----------- desc the process -------------
desc integration my_s3_integration_Processed;


----------- creating the external stage -----------
CREATE OR REPLACE STAGE TAXI_DB_PRACTICE.Silver.ext1_stage_taxi_s3_Silver
URL = 's3://nyc-bronze-demo/processed/'
STORAGE_INTEGRATION = my_s3_integration_Processed
FILE_FORMAT = (TYPE = PARQUET);

list @ext1_stage_taxi_s3_Silver;
-- drop stage ext1_stage_taxi_s3_Silver;
