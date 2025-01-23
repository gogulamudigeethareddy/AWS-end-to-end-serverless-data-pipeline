SELECT SUBSTR(tpep_pickup_datetime, 1, 7) "Period", COUNT(*) "Total Records"
FROM   raw_yellow_tripdata
GROUP BY SUBSTR(tpep_pickup_datetime, 1, 7) 
ORDER BY 1;

SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata 
WHERE  SUBSTR(tpep_pickup_datetime, 1, 7) NOT LIKE '2020%';

SELECT COUNT(*) "Count"
FROM   raw_yellow_tripdata
WHERE  vendorid IS NULL
AND    SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020%';

SELECT COUNT(*) "Count"
FROM raw_yellow_tripdata
WHERE vendorid IS NOT NULL
AND SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';

SELECT td.*, pu.*, do.*
FROM raw_yellow_tripdata td,
     raw_taxi_zone_lookup pu,
     raw_taxi_zone_lookup do
WHERE td.pulocationid = pu.locationid AND   
      td.pulocationid = do.locationid AND   
      vendorid IS NOT NULL AND 
      SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%'
LIMIT 100;

SELECT Count(*) "Count"
FROM raw_yellow_tripdata td,
     raw_taxi_zone_lookup pu,
     raw_taxi_zone_lookup do
WHERE td.pulocationid = pu.locationid AND   
      td.pulocationid = do.locationid AND   
      vendorid IS NOT NULL AND 
      SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';


