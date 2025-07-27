 ----------------- Gold layer ----------------------
  --- fact table -------
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.fact
AS SELECT

  VendorID ,
  PULocationID ,
  DOLocationID , -- FK to dim_location_zone
  passenger_count ,
  trip_distance ,
  RatecodeID ,
  payment_type ,
  fare_amount ,
  extra ,
  mta_tax ,
  tip_amount ,
  tolls_amount,
  improvement_surcharge,
  total_amount ,
  trip_duration ,
  speed ,
  pickup_date ,
  pickup_time ,
  dropoff_date ,
  dropoff_time ,

  

  -- Flags
  CASE
    WHEN store_and_fwd_flag = 'Y' THEN 1
    WHEN store_and_fwd_flag = 'N' THEN 0
    ELSE NULL
  END AS is_store_and_forwarded
  from TAXI_DB_PRACTICE.SILVER.CLEANED;

