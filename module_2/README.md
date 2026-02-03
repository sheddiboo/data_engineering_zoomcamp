# Module 2: Workflow Orchestration with Kestra (AWS Edition)

This folder contains the homework solution for **Module 2: Workflow Orchestration** of the Data Engineering Zoomcamp.

Instead of the default Google Cloud Platform (GCP) setup, I implemented the ETL pipelines using an **AWS Stack** consisting of **S3 (Data Lake)** and **Athena (Serverless SQL)**.

## 🚀 Project Description

The goal of this module was to orchestrate an ETL pipeline that:
1.  **Extracts** NYC Taxi Data (Yellow & Green) from GitHub.
2.  **Loads** the raw CSV files into an **AWS S3 Bucket**.
3.  **Transforms/Analyzes** the data by creating External Tables in **AWS Athena**.
4.  **Schedules** the workflow to run daily using Timezone-aware triggers.
5.  **Backfills** historical data (2020-2021) automatically.

## 🛠️ Tech Stack

* **Orchestration:** [Kestra](https://kestra.io/) (running via Docker)
* **Storage:** AWS S3
* **Query Engine:** AWS Athena
* **Language:** YAML (Kestra Flows)

## 📂 Flow Files

* `aws_kv.yaml`: Sets up the KV store variables for AWS credentials, bucket names, and regions.
* `aws_taxi_scheduled.yaml`: The main ETL flow. It handles downloading, unzipping, uploading to S3, and creating Athena tables. It supports both **Scheduled** runs and **Manual Backfills**.

## 📝 Homework Solution

Here are the answers to the Module 2 Quiz based on the data backfilled (Jan 2020 - July 2021).

**Question 1:** Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the extract task)?
* **Answer:** `128.3 MiB`

**Question 2:** What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
* **Answer:** `green_tripdata_2020-04.csv`

**Question 3:** How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
* **Answer:** `24,648,499`

**Question 4:** How many rows are there for the Green Taxi data for all CSV files in the year 2020?
* **Answer:** `1,734,051`

**Question 5:** How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
* **Answer:** `1,925,152`

**Question 6:** How would you configure the timezone to New York in a Schedule trigger?
* **Answer:** Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration

