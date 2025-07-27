------ creating the snow pipe -------------------
CREATE OR REPLACE PIPE snowpipe_taxi_parquet
  AUTO_INGEST = TRUE
  AS
  COPY INTO TAXI_DB_PRACTICE.SILVER.CLEANED
  FROM (
    SELECT
      $1:PUBorough::STRING,
      $1:DOBorough::STRING,
      $1:PULocationID::BIGINT,
      $1:DOLocationID::BIGINT,
      $1:VendorID::BIGINT,
      TO_TIMESTAMP_TZ($1:tpep_pickup_datetime::STRING),
      TO_TIMESTAMP_TZ($1:tpep_dropoff_datetime::STRING),
      $1:passenger_count::STRING,
      $1:trip_distance::FLOAT,
      $1:RatecodeID::STRING,
      $1:store_and_fwd_flag::STRING,
      $1:payment_type::FLOAT,
      $1:fare_amount::FLOAT,
      $1:extra::FLOAT,
      $1:mta_tax::FLOAT,
      $1:tip_amount::FLOAT,
      $1:tolls_amount::FLOAT,
      $1:improvement_surcharge::FLOAT,
      $1:total_amount::FLOAT,
      $1:PUZone::STRING,
      $1:PUservice_zone::STRING,
      $1:DOZone::STRING,
      $1:DOservice_zone::STRING,
      $1:trip_duration::FLOAT,
      $1:speed::FLOAT,
      $1:pickup_date::DATE,
 $1:pickup_time::STRING,
      $1:dropoff_date::DATE,
      $1:dropoff_time::STRING
    FROM @ext1_stage_taxi_s3_Silver
  )
  FILE_FORMAT = (TYPE = 'PARQUET');
select count(*)from TAXI_DB_PRACTICE.SILVER.CLEANED;

list  @ext1_stage_taxi_s3_Silver;
