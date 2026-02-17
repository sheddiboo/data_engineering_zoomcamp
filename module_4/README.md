# Module 4: Analytics Engineering

This project utilizes **dbt Core** locally to transform and model NYC taxi trip data stored in **AWS S3**, using **AWS Athena** as the cloud data warehouse. The pipeline converts raw, messy datasets into structured, analytics-ready tables.

---

## Architecture Overview
- **Data Source**: NYC Green, Yellow, and FHV trip data (2019-2020) stored in S3 as Gzipped CSVs.
- **Warehouse**: AWS Athena (Engine v3) for SQL execution and metadata management.
- **Transformation Tool**: dbt Core (local CLI) using the `dbt-athena-community` adapter.
- **Storage**: S3 buckets for both source data and dbt-managed table results.


---

## Directory Structure & Components

The project follows a standard dbt structure enhanced with custom scripts and macros for an AWS environment:

### 1. Ingestion (`ingest_to_s3.py`)
A custom Python script is used to programmatically download raw taxi data from the GitHub repository and upload it directly to specific S3 prefixes. This ensures the raw data is organized in a way that Athena can partition effectively.

### 2. Seeds (`/seeds`)
Static lookup data used to enrich the taxi trip records:
* **`taxi_zone_lookup.csv`**: Maps location IDs to boroughs and zones.
* **`payment_type_lookup.csv`**: Maps numeric payment codes to human-readable descriptions.

### 3. Macros (`/macros`)
Custom reusable SQL snippets that standardize logic across the project:
* **`get_trip_duration_minutes.sql`**: Calculates the delta between pickup and dropoff times.
* **`get_vendor_description.sql`**: Logic to map raw vendor IDs to service names.
* **`safe_cast.sql`**: A utility to handle casting errors in raw data without breaking the dbt run.

### 4. Models (`/models`)
The core transformation logic organized into a modular architecture:
* **Staging Layer**: Standardizes raw tables with renaming, type casting, and initial filters for FHV, Green, and Yellow data.
* **Intermediate Layer**: Handles complex deduplication using `row_number()` and unions the multiple taxi datasets into a single source.
* **Marts Layer**: Final, reporting-ready fact and dimension tables, including the `fct_monthly_zone_revenue` model.


---

## Homework Results

| Question | Answer |
| :--- | :--- |
| **Question 1** | **stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned** |
| **Question 2** | **dbt will fail the test, returning a non-zero exit code** |
| **Question 3** | **12,998** |
| **Question 4** | **East Harlem North** |
| **Question 5** | **500,234** |
| **Question 6** | **43,244,693** |

---