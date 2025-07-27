-- dim store and forward flag --
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_STORE_FORWARD_FLAG AS
SELECT DISTINCT
  store_and_fwd_flag,
  CASE
    WHEN store_and_fwd_flag IN ('Y','y') THEN 'Trip stored temporarily'
    WHEN store_and_fwd_flag IN ('N','n') THEN 'Trip sent immediately'
    ELSE 'Unknown'
  END AS flag_description
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;
select * from TAXI_DB_PRACTICE.GOLD.DIM_STORE_FORWARD_FLAG;

--- dimension passenger count ---
DROP TABLE PASSENGER_COUNT;
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT AS
SELECT DISTINCT
  passenger_count,
  CASE 
    WHEN passenger_count = '1.0' THEN 'Single Passenger'
    WHEN passenger_count BETWEEN '2.0' AND '5.0' THEN 'Small Group'
    ELSE 'Large Group'
  END AS passenger_group
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;


SELECT * FROM TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT ORDER BY PASSENGER_COUNT;

--- VALID OR INVALID TRIP ----
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_VALID_INVALID_TRIP AS
SELECT DISTINCT
  passenger_count,
  CASE 
    WHEN passenger_count BETWEEN '1.0' AND '5.0' THEN 'Valid Trip'
    ELSE 'Invalid Trip'
  END AS valid_invalid_trip
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;


SELECT * FROM TAXI_DB_PRACTICE.GOLD.DIM_VALID_INVALID_TRIP order by passenger_count;

---  dim payment type ---
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_PAYMENT_TYPE AS
SELECT DISTINCT
  payment_type,
  CASE 
    WHEN payment_type = 1 THEN 'Credit Card'
    WHEN payment_type = 2 THEN 'Cash'
    WHEN payment_type = 3 THEN 'No Charge'
    WHEN payment_type = 4 THEN 'Dispute'
    WHEN payment_type = 5 THEN 'Unknown'
    WHEN payment_type = 6 THEN 'Voided Trip'
    ELSE 'Other'
  END AS payment_method
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;
select * from TAXI_DB_PRACTICE.GOLD.DIM_PAYMENT_TYPE order by payment_type;


---- dim time bucket -----
CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_TIME_BUCKET AS
SELECT DISTINCT
  PICKUP_TIME,
  CASE 
    WHEN TO_NUMBER(SUBSTR(pickup_time, 1, 2)) BETWEEN 5 AND 11 THEN 'Morning'
    WHEN TO_NUMBER(SUBSTR(pickup_time, 1, 2)) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN TO_NUMBER(SUBSTR(pickup_time, 1, 2)) BETWEEN 17 AND 21 THEN 'Evening'
    ELSE 'Night'
  END AS time_of_day
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;

select * from TAXI_DB_PRACTICE.GOLD.DIM_TIME_BUCKET ;

--- dim time  date ---

 CREATE OR REPLACE TABLE TAXI_DB_PRACTICE.GOLD.DIM_DATE AS
SELECT DISTINCT
  pickup_date,
  EXTRACT(YEAR FROM pickup_date) AS year,
  EXTRACT(QUARTER FROM pickup_date) AS quarter,
  EXTRACT(MONTH FROM pickup_date) AS month,
  EXTRACT(DAY FROM pickup_date) AS day,
  DAYOFWEEK(pickup_date) AS day_of_week_number,
  TO_CHAR(pickup_date, 'DY') AS day_name_abbr
FROM TAXI_DB_PRACTICE.SILVER.CLEANED;


select * from TAXI_DB_PRACTICE.GOLD.DIM_DATE ;
