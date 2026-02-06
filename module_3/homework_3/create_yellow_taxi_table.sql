-- SQL Setup for AWS Athena

-- Drop the table if it exists to ensure a fresh start
DROP TABLE IF EXISTS yellow_taxi_2024;

-- Create the External Table
CREATE EXTERNAL TABLE yellow_taxi_2024 (
  VendorID BIGINT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count BIGINT,
  trip_distance DOUBLE,
  RatecodeID BIGINT,
  store_and_fwd_flag STRING,
  PULocationID BIGINT,
  DOLocationID BIGINT,
  payment_type BIGINT,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  congestion_surcharge DOUBLE,
  Airport_fee DOUBLE
)
STORED AS PARQUET
LOCATION 's3://zoomcamp-data-shedrack-2026/homework_data/';