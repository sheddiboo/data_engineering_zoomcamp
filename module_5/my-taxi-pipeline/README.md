# NYC Taxi Data Pipeline (Built with Bruin)

## Overview

This project contains an end-to-end ELT (Extract, Load, Transform) data pipeline built for the Data Engineering Zoomcamp (Module 5). It processes real NYC Taxi trip data, transforming raw Parquet files into clean, analytics-ready reporting tables.

The entire pipeline, from ingestion and orchestration to data quality and transformation, is powered entirely by **Bruin**, utilizing **DuckDB** as the local processing engine.

## What is Bruin?

[Bruin](https://getbruin.com/) is a unified, open-source CLI tool that simplifies the modern data stack. Instead of wiring together separate tools for different tasks (e.g., Airbyte for extraction, dbt for transformation, Airflow for orchestration, and Great Expectations for quality), Bruin handles them all in one place.

**Key Features of this Pipeline:**

* **Code-First:** Everything is defined as version-controllable text (SQL, Python, and YAML).
* **Multi-Language:** Mixes Python (for API extraction) and SQL (for transformations) in the same dependency graph.
* **Built-in Quality:** Column-level data quality checks e.g., `not_null`, `unique`, `positive` are integrated directly into the SQL asset definitions.
* **Smart Materialization:** Uses Bruin's `time_interval` strategy to efficiently process data incrementally without dropping and rebuilding entire tables.

## Pipeline Architecture

1. **Ingestion Layer `ingestion/`:** * A Python asset extracts raw NYC Taxi Parquet data from the public TLC endpoint. It relies on a `requirements.txt` file configured with pandas, requests, pyarrow, and python-dateutil to handle the dataframes and API requests.
* A Seed asset loads static payment type lookup data from a local CSV.

2. **Staging Layer `staging/`:** * SQL assets clean, normalize, and deduplicate the raw data using `ROW_NUMBER()`.
* Raw columns are renamed, and data is enriched by joining with the payment lookup table.

3. **Reports Layer `reports/`:** * SQL assets aggregate the staged data to calculate metrics like `trip_count`, `total_fare`, and `avg_fare` grouped by date, taxi type, and payment type.

## Project Structure

*Note: Some files are intentionally excluded from version control via `.gitignore` for security and storage reasons.*

```text
my-taxi-pipeline/
├── .gitignore                              # Git ignore file
├── README.md                               # Project documentation
├── requirements.txt                        # Root Python dependencies
├── .bruin.yml                              # Git ignored: Environments + connections local DuckDB
├── duckdb.db                               # Git ignored: Local DuckDB database file
├── venv/                                   # Git ignored: Python virtual environment
└── pipeline/
    ├── README.md                           # Pipeline specific documentation
    ├── pipeline.yml                        # Pipeline name, schedule, variables
    └── assets/
        ├── ingestion/
        │   ├── payment_lookup.asset.yml    # Seed asset definition
        │   ├── payment_lookup.csv          # Seed data (Ensure this is force-added if ignored by *.csv)
        │   ├── requirements.txt            # Asset Python dependencies: pandas, requests, pyarrow, python-dateutil
        │   └── trips.py                    # Python extraction script
        ├── reports/
        │   └── trips_report.sql            # Aggregations and analytics
        └── staging/
            └── trips.sql                   # Cleaning and transformation logic
```

## How to Run Locally

**1. Clone the repository and set up your local environment:**
Because `.bruin.yml` is git-ignored, you will need to create it in the root folder and add your DuckDB connection string before running.

**2. Install Bruin CLI:**

```bash
curl -LsSf [https://getbruin.com/install/cli](https://getbruin.com/install/cli) | sh
```

**3. Validate the Pipeline:**

```bash
bruin validate ./pipeline/pipeline.yml --environment default
```

**4. Run the Full Pipeline with backfill dates:**

```bash
bruin run ./pipeline/pipeline.yml \
  --environment default \
  --start-date 2024-01-01T00:00:00Z \
  --end-date 2024-01-31T23:59:59Z
```

---

## Module 5 Homework Answers

**Question 1. Bruin Pipeline Structure**

* **Question:** In a Bruin project, what are the required files/directories?
* **Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

**Question 2. Materialization Strategies**

* **Question:** You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?
* **Answer:** `time_interval` - incremental based on a time column

**Question 3. Pipeline Variables**

* **Question:** How do you override this when running the pipeline to only process yellow taxis?
* **Answer:** `bruin run --var 'taxi_types=["yellow"]'`

**Question 4. Running with Dependencies**

* **Question:** You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?
* **Answer:** `bruin run ingestion/trips.py --downstream`

**Question 5. Quality Checks**

* **Question:** You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?
* **Answer:** `name: not_null`

**Question 6. Lineage and Dependencies**

* **Question:** After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?
* **Answer:** `bruin lineage`

**Question 7. First-Time Run**

* **Question:** You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?
* **Answer:** `--full-refresh`