# aws

- `aws_serverless/aws_serverless_datalake_jumpstart.sql`: Contains SQL queries for analyzing taxi trip data.
- `README.md`: Project documentation.

## SQL Queries

The SQL queries in `aws_serverless/aws_serverless_datalake_jumpstart.sql` perform the following operations:

1. **Monthly Record Count**:
    ```sql
    SELECT SUBSTR(tpep_pickup_datetime, 1, 7) "Period", COUNT(*) "Total Records"
    FROM   raw_yellow_tripdata
    GROUP BY SUBSTR(tpep_pickup_datetime, 1, 7) 
    ORDER BY 1;
    ```

2. **Count of Records Not from 2020**:
    ```sql
    SELECT COUNT(*) "Count"
    FROM   raw_yellow_tripdata 
    WHERE  SUBSTR(tpep_pickup_datetime, 1, 7) NOT LIKE '2020%';
    ```

3. **Count of Records with Null Vendor ID in 2020**:
    ```sql
    SELECT COUNT(*) "Count"
    FROM   raw_yellow_tripdata
    WHERE  vendorid IS NULL
    AND    SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020%';
    ```

4. **Count of Records with Non-null Vendor ID in January 2020**:
    ```sql
    SELECT COUNT(*) "Count"
    FROM raw_yellow_tripdata
    WHERE vendorid IS NOT NULL
    AND SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';
    ```

5. **Sample Data with Location Lookup**:
    ```sql
    SELECT td.*, pu.*, do.*
    FROM raw_yellow_tripdata td,
         raw_taxi_zone_lookup pu,
         raw_taxi_zone_lookup do
    WHERE td.pulocationid = pu.locationid AND   
          td.pulocationid = do.locationid AND   
          vendorid IS NOT NULL AND 
          SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%'
    LIMIT 100;
    ```

6. **Count of Records with Location Lookup in January 2020**:
    ```sql
    SELECT Count(*) "Count"
    FROM raw_yellow_tripdata td,
         raw_taxi_zone_lookup pu,
         raw_taxi_zone_lookup do
    WHERE td.pulocationid = pu.locationid AND   
          td.pulocationid = do.locationid AND   
          vendorid IS NOT NULL AND 
          SUBSTR(tpep_pickup_datetime, 1, 7) LIKE '2020-1%';
    ```

## Usage

To run these queries, you need access to the AWS serverless data lake where the `raw_yellow_tripdata` and `raw_taxi_zone_lookup` tables are stored. You can execute these queries using a SQL client that supports AWS serverless databases.

## License

This project is licensed under the MIT License. See the LICENSE file for details.