
------ KPI ------
-- 1 . total trips per payment type --
SELECT
  p.payment_type,
  COUNT(*) AS total_trips,
  SUM(f.total_amount) AS total_revenue
FROM TAXI_DB_PRACTICE.GOLD.fact f
JOIN  TAXI_DB_PRACTICE.GOLD.DIM_PAYMENT_TYPE p ON f.payment_type = p.payment_type
GROUP BY p.payment_type
ORDER BY total_trips DESC;
-- 2. average trip distance by PASSENGER COUNT --

SELECT
  r.PASSENGER_COUNT,
  AVG(f.TRIP_DISTANCE) AS avg_distance
FROM  TAXI_DB_PRACTICE.GOLD.fact f
JOIN  TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT r ON f.PASSENGER_COUNT = r.PASSENGER_COUNT
GROUP BY r.PASSENGER_COUNT
ORDER BY avg_distance DESC;

-- 3.Total Trips by Pickup Date and Passenger Count--
SELECT
  d.pickup_date,
  p.passenger_count,
  COUNT(*) AS total_trips,
FROM  TAXI_DB_PRACTICE.GOLD.fact f
JOIN TAXI_DB_PRACTICE.GOLD.DIM_DATE d ON f.pickup_date = d.pickup_date 
JOIN  TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT p ON f.passenger_count = p.passenger_count
GROUP BY d.pickup_date , p.passenger_count
ORDER BY d.pickup_date , p.passenger_count;

--4.
SELECT
  z.zone_name AS pickup_zone,
  COUNT(*) AS total_trips,
  ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM TAXI_DB_PRACTICE.GOLD.fact f
JOIN  TAXI_DB_PRACTICE.GOLD.DIM_LOCATION_ZONE z 
  ON f.PULocationID = z.zone_id 
GROUP BY z.zone_name
ORDER BY total_revenue DESC ;


---- monthly ------------
SELECT
  TO_CHAR(d.pickup_date, 'YYYY-MM') AS pickup_month,
  p.passenger_count,
  COUNT(*) AS total_trips,
  ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM TAXI_DB_PRACTICE.GOLD.fact f
JOIN TAXI_DB_PRACTICE.GOLD.DIM_DATE d 
  ON f.pickup_date = d.pickup_date
JOIN TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT p 
  ON f.passenger_count = p.passenger_count
GROUP BY TO_CHAR(d.pickup_date, 'YYYY-MM'), p.passenger_count
ORDER BY pickup_month, p.passenger_count;

---------------- DOC KPI  --------
SELECT
  SUM(TO_NUMBER(passenger_count)) AS total_passengers,
  ROUND(SUM(fare_amount), 2) AS total_fare_amount,
  ROUND(SUM(tip_amount), 2) AS total_tip_amount,
  ROUND(SUM(fare_amount + tip_amount + tolls_amount + extra + mta_tax + improvement_surcharge), 2) AS total_revenue
FROM TAXI_DB_PRACTICE.GOLD.fact
WHERE fare_amount > 0 AND TRY_TO_NUMBER(passenger_count) IS NOT NULL;

----------------------------------------------------------


select
p.passenger_group,
ROUND(SUM(f.fare_amount), 2) AS total_fare_amount,
ROUND(SUM(f.tip_amount), 2) AS total_tip_amount,
ROUND(SUM(f.fare_amount + f.tip_amount + f.tolls_amount + f.extra + f.mta_tax + f.improvement_surcharge), 2) AS total_revenue from
TAXI_DB_PRACTICE.GOLD.FACT f
join
TAXI_DB_PRACTICE.GOLD.DIM_PASSENGER_COUNT p
on f.passenger_count = p.passenger_count
group by  p.passenger_group;

SELECT
  p.valid_invalid_trip,
  SUM(TRY_TO_NUMBER(f.passenger_count)) AS total_passenger_count,
  ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM TAXI_DB_PRACTICE.GOLD.fact f
JOIN TAXI_DB_PRACTICE.GOLD.DIM_VALID_INVALID_TRIP p
  ON f.passenger_count = p.passenger_count
WHERE TRY_TO_NUMBER(f.passenger_count) IS NOT NULL'>5'
ORDER BY total_revenue DESC;
