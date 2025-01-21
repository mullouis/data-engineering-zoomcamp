-----------------------------------------
Question 1. Understanding docker first run
-----------------------------------------

docker run -it python:3.12.8 /bin/bash

:/# pip list
Package    Version
---------- -------
pip        24.3.1

ANSWER: 24.3.1

-----------------------------------------
Question 2. Understanding Docker networking and docker-compose
-----------------------------------------
ANSWER: db:5432

-----------------------------------------
-- Question 3. Trip Segmentation Count
-----------------------------------------
ANSWER: 104,802; 198,924; 109,603; 27,678; 35,189

SELECT 
	SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS up_to_1_mile,
	SUM(CASE WHEN trip_distance > 1 AND trip_distance  <=3 THEN 1 ELSE 0 END) AS between_1_and_3_miles,
	SUM(CASE WHEN trip_distance > 3 AND trip_distance  <=7 THEN 1 ELSE 0 END) AS between_3_and_7_miles,
	SUM(CASE WHEN trip_distance > 7 AND trip_distance  <=10 THEN 1 ELSE 0 END) AS between_7_and_10_miles,
	SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS over_10_miles
FROM public.green_taxi_data
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
  AND CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'

-----------------------------------------
Question 4. Longest trip for each day
-----------------------------------------
Answer: 2019-10-31

SELECT
    CAST(lpep_pickup_datetime AS DATE) as pickup_day,
	lpep_pickup_datetime,
	SUM(trip_distance) AS trip_distance
FROM public.green_taxi_data
GROUP BY pickup_day,lpep_pickup_datetime
ORDER BY trip_distance DESC
LIMIT 1;

-----------------------------------------
-- Question 5. Three biggest pickup zones
-----------------------------------------
Answer: East Harlem North, East Harlem South, Morningside Heights

SELECT 
	CAST(A.lpep_pickup_datetime AS DATE) AS pickup_date,
	B."Zone" as pickup_zone,
	SUM(A.total_amount) AS total_amount
FROM public.green_taxi_data A
LEFT JOIN public.zones B ON A."PULocationID" = B."LocationID"
WHERE CAST(A.lpep_pickup_datetime AS DATE) = '2019-10-18' 
GROUP BY pickup_date,B."Zone"
HAVING SUM(A.total_amount) > 13000 
ORDER BY total_amount DESC
LIMIT 3;	

-----------------------------------------
-- Question 6. Largest tip
-----------------------------------------
ANSWER: Yorkville West  (bigest amoint of total tips)
ANSWER: Newark Airpoty zone with the single largest tip 

SELECT 
	CAST(A.lpep_pickup_datetime AS DATE) AS pickup_date,
	B."Zone" AS pickup_zone,
	C."Zone" AS dropoff_zone,
    SUM(A.tip_amount) AS tip_amount
FROM public.green_taxi_data A
LEFT JOIN public.zones B ON A."PULocationID" = B."LocationID"
LEFT JOIN public.zones C ON A."DOLocationID" = C."LocationID"
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-10-20'
AND  B."Zone" = 'East Harlem North'
GROUP BY 	
pickup_date,B."Zone",C."Zone"
ORDER BY tip_amount DESC
or 

SELECT 
	A.lpep_pickup_datetime,
	B."Zone" AS pickup_zone,
	C."Zone" AS dropoff_zone,
    MAX(A.tip_amount) AS tip_amount
FROM public.green_taxi_data A
LEFT JOIN public.zones B ON A."PULocationID" = B."LocationID"
LEFT JOIN public.zones C ON A."DOLocationID" = C."LocationID"
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-10-20'
AND  B."Zone" = 'East Harlem North'
GROUP BY 	
A.lpep_pickup_datetime,B."Zone",C."Zone"
ORDER BY tip_amount DESC

-----------------------------------------
-- Question 7. Terraform Workflow
-----------------------------------------
ANSWER: 
terraform init, terraform apply -auto-approve, terraform destroy