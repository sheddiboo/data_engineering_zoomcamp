# Module 3 Homework: Data Warehousing (AWS Edition)

This folder contains the solution for the Module 3 homework. 

**Note on Cloud Provider:**
The original course material uses Google Cloud Platform (GCP). For this submission, I adapted the workflow to run on **AWS**:
* **Storage:** Replaced Google Cloud Storage (GCS) with **Amazon S3**.
* **Data Warehouse:** Replaced BigQuery with **AWS Athena** (Serverless SQL engine) to query the Parquet files directly from S3.
* **Ingestion:** Used a custom Python script with `boto3` to orchestrate the download and upload process.

## Folder Contents
* `load_yellow_taxi_aws.py`: A Python script that downloads the 2024 Yellow Taxi Parquet files (Jan–June) and uploads them to a specified S3 bucket. It handles authentication via `boto3` and ensures data integrity.
* `create_yellow_taxi_table.sql`: The SQL commands executed in AWS Athena to define the external table schema. This script includes specific data type adjustments (e.g., using `BIGINT` instead of `INT64`) to resolve schema mismatches with the Parquet files.

## Homework Answers

**Question 1: Counting records**
20,332,093

**Question 2: Data read estimation**
0 MB for the External Table and 155.12 MB for the Materialized Table

**Question 3: Understanding columnar storage**
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**Question 4: Counting zero fare trips**
8,333

**Question 5: Partitioning and clustering**
Partition by tpep_dropoff_datetime and Cluster on VendorID

**Question 6: Partition benefits**
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

**Question 7: External table storage**
GCP Bucket (or S3 Bucket in my AWS setup)

**Question 8: Clustering best practices**
False

**Question 9: Understanding table scans**
0 Bytes