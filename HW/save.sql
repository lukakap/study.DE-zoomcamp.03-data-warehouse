-- Creating external table
CREATE OR REPLACE EXTERNAL TABLE angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external
  OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp-hw-3/green_tripdata_2022.parquet']);

-- select VendorID, PULocationID from gree_trips_dataset.green_trips_dataset_external limit 10; 

-- Create table from external table
CREATE OR REPLACE TABLE angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_native
AS
SELECT * FROM gree_trips_dataset.green_trips_dataset_external;

-- Count of records
SELECT DISTINCT PULocationID FROM angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external;

SELECT DISTINCT PULocationID FROM angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_native;

-- How many records have a fare_amount of 0
SELECT COUNT(*) 
FROM angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external
WHERE fare_amount = 0;

-- create partitioned and clustered new table
CREATE OR REPLACE TABLE angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_partitoned
PARTITION BY DATE(lpep_pickup_datetime);

-- replace above tables with two datetime column with correct type
CREATE OR REPLACE TABLE angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external AS
SELECT 
  VendorID,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS lpep_dropoff_datetime,
  store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM 
  angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external;


CREATE OR REPLACE TABLE angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_native AS
SELECT 
  *,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS lpep_dropoff_datetime
FROM 
  angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_native;



-- SELECT DISTINCT PULocationID, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS corrected_timestamp
-- FROM angular-sign-demo-405419.gree_trips_dataset.green_trips_dataset_external
-- WHERE 
-- TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) 
-- BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30')
-- LIMIT 10;
