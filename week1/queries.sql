-- Selecting total trips on Jan 15
SELECT 
	CAST(lpep_pickup_datetime as DATE) as date_pickup,
	CAST(lpep_dropoff_datetime as DATE) as date_dropoff,
	COUNT(1) as num_trips
FROM green_taxi
WHERE CAST(lpep_pickup_datetime as DATE) = '2019-01-15' AND
CAST(lpep_dropoff_datetime as DATE) = '2019-01-15'
GROUP BY date_pickup, date_dropoff

-- largest trip day (longest distance) based on pickup time
SELECT 
	CAST(lpep_pickup_datetime as DATE) as pickup_date,
	MAX(trip_distance) as trip_distance
FROM green_taxi
GROUP BY pickup_date
ORDER BY trip_distance DESC
LIMIT 1

-- how many trips with num_passengers 2 or 3
SELECT
	CAST(lpep_pickup_datetime AS DATE) as date,
	passenger_count,
	COUNT(1) AS num_trips
FROM green_taxi
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' AND
passenger_count BETWEEN 2 AND 3
GROUP BY date, passenger_count

-- largest tip from Astoria zone pickup
SELECT 
	MAX(gt.tip_amount) as tip,
	zp."Zone" as pickup_zone,
	zd."Zone" as dropoff_zone,
	CAST(gt.lpep_pickup_datetime AS DATE) as date_pickup
FROM green_taxi gt INNER JOIN taxi_zones AS zp 
	ON zp."LocationID" = gt."PULocationID"
INNER JOIN taxi_zones AS zd
	ON zd."LocationID" = gt."DOLocationID"
WHERE zp."Zone" = 'Astoria'
GROUP BY pickup_zone, dropoff_zone, date_pickup
ORDER BY tip DESC
LIMIT 1