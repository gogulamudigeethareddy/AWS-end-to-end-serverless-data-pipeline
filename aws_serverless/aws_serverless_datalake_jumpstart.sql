-- Exploring Data in the Data Lake

-- Sample Data
SELECT * 
FROM raw_yellow_tripdata 
LIMIT 10;

-- Query 1: Total Records by Period
SELECT 
    SUBSTR(tpep_pickup_datetime, 1, 7) AS "Period", 
    COUNT(*) AS "Total Records"
FROM 
    raw_yellow_tripdata
GROUP BY 
    SUBSTR(tpep_pickup_datetime, 1, 7)
ORDER BY 
    "Period";

-- Query 2: Count of Records Not from 2020
SELECT 
    COUNT(*) AS "Count"
FROM 
    raw_yellow_tripdata 
WHERE  
    SUBSTR(tpep_pickup_datetime, 1, 7) NOT LIKE '2020%';

-- Query 3: Count of Records with NULL VendorID in 2020
SELECT 
    COUNT(*) AS "Count"
FROM 
    raw_yellow_tripdata
WHERE  
    vendorid IS NULL
    AND SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020%';

-- Query 4: Count of Records with Non-NULL VendorID in January 2020
SELECT 
    COUNT(*) AS "Count"
FROM 
    raw_yellow_tripdata
WHERE 
    vendorid IS NOT NULL
    AND SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-01%';

-- Query 5: Detailed Records with Location Lookup for January 2020
SELECT 
    td.*, 
    pu.*, 
    do.*
FROM 
    raw_yellow_tripdata td
JOIN 
    raw_taxi_zone_lookup pu ON td.pulocationid = pu.locationid
JOIN 
    raw_taxi_zone_lookup do ON td.dolocationid = do.locationid
WHERE 
    vendorid IS NOT NULL
    AND SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-01%';