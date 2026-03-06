#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F

# Parse S3 input and output arguments
parser = argparse.ArgumentParser(description='Production Taxi ETL: EC2 to Athena')
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()

# Configure S3A for IAM Role-based authentication (Instance Profile)
conf = SparkConf() \
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .set("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")

# Initialize Session
spark = SparkSession.builder.appName('taxi_production_etl').config(conf=conf).getOrCreate()

# Load and standardize Green/Yellow Data
df_green = spark.read.option("recursiveFileLookup", "true").parquet(args.input_green) \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(args.input_yellow) \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# Unified Schema
common_columns = [
    'VendorID', 'pickup_datetime', 'dropoff_datetime', 'PULocationID', 
    'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 
    'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount'
]

# Union and View Creation
df_green_sel = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.createOrReplaceTempView('trips_data')

# Revenue Aggregation
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(total_amount) AS revenue_monthly_total_amount,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM trips_data
GROUP BY 1, 2, 3
""")

# Output to S3: Partitioned by service_type for Athena performance
df_result.write.partitionBy("service_type").mode("overwrite").parquet(args.output)