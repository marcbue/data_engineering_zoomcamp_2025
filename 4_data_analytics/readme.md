terraform apply

If data not downloaded:
python load_taxi_data.py
python fix_schema_of_parquet_files.py
python load_taxi_data.py

If data downloaded:
python fix_schema_of_parquet_files.py
python load_taxi_data.py

Then in Big Query:
Create a query:
-- Create External data tables
-- Green
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp_week_4_dataset.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-week-4-bucket/green_tripdata_*.parquet']
);
-- Yellow
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp_week_4_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-week-4-bucket/yellow_tripdata_*.parquet']
);
-- fhv
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp_week_4_dataset.external_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-week-4-bucket/fhv_tripdata_*.parquet']
);

-- Create Regular data tables
-- Green
CREATE OR REPLACE TABLE `zoomcamp_week_4_dataset.green_tripdata`
AS
SELECT * FROM `zoomcamp_week_4_dataset.external_green_tripdata`;
-- Yellow
CREATE OR REPLACE TABLE `zoomcamp_week_4_dataset.yellow_tripdata`
AS
SELECT * FROM `zoomcamp_week_4_dataset.external_yellow_tripdata`;
-- fhv
CREATE OR REPLACE TABLE `zoomcamp_week_4_dataset.fhv_tripdata`
AS
SELECT * FROM `zoomcamp_week_4_dataset.external_fhv_tripdata`;

Now the data sets should be there to work further