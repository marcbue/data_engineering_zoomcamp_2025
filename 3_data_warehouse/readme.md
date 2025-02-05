SQL queries for Homework used in GCP:

-- Create External data table
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp_week_3_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-week-3-bucket/yellow_tripdata_2024-*.parquet']
);

-- Create Regular Table
CREATE OR REPLACE TABLE `zoomcamp_week_3_dataset.yellow_tripdata`
AS
SELECT * FROM `zoomcamp_week_3_dataset.external_yellow_tripdata`;

-- Look at the data from External table
SELECT * FROM `zoomcamp_week_3_dataset.external_yellow_tripdata` LIMIT 10;

-- Look at the data from Regular table table
SELECT * FROM `zoomcamp_week_3_dataset.yellow_tripdata` LIMIT 10;

-- Question 1:  What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*) FROM `zoomcamp_week_3_dataset.external_yellow_tripdata`;

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT 
  COUNT(DISTINCT(PULocationID)) AS external_count
FROM `zoomcamp_week_3_dataset.external_yellow_tripdata`;
-- UNION ALL
SELECT 
  COUNT(DISTINCT(PULocationID)) AS regular_count
FROM `zoomcamp_week_3_dataset.yellow_tripdata`;

-- Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
SELECT
  PULocationID
FROM `zoomcamp_week_3_dataset.yellow_tripdata`;
SELECT
  PULocationID, DOLocationID
FROM `zoomcamp_week_3_dataset.yellow_tripdata`;

-- Question 4: How many records have a fare_amount of 0?
SELECT COUNT(*)
FROM `zoomcamp_week_3_dataset.yellow_tripdata`
WHERE fare_amount = 0;

-- Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter 
-- based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
CREATE OR REPLACE TABLE zoomcamp_week_3_dataset.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_dropoff_datetime )
CLUSTER BY VendorID AS
SELECT * FROM `zoomcamp_week_3_dataset.yellow_tripdata`;
-- Check partitons
SELECT table_name, partition_id, total_rows
FROM `zoomcamp_week_3_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned_clustered'
ORDER BY total_rows DESC;

-- Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 03/01/2024 and 03/15/2024 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. 
-- What are these values?
SELECT 
  DISTINCT(VendorID)
FROM `zoomcamp_week_3_dataset.yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT 
  DISTINCT(VendorID)
FROM `zoomcamp_week_3_dataset.yellow_tripdata_partitoned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

