# Data Ingestion Pipeline (Dockerized)

This directory contains the setup for ingesting NY Taxi data using a fully containerized approach. The goal was to practice containerizing Python scripts and managing dependencies using **Docker** and **Docker Compose**.

## Components

* **`Dockerfile`**: Defines the Python environment, installing pandas, SQLAlchemy, and psycopg2.
* **`ingest_data.py`**: The logic for downloading data and uploading it to the database.
* **`docker-compose.yaml`**: Orchestrates the services (Ingestion Script + Database) to run together easily.

## How to Run

Instead of installing dependencies locally, we run the entire pipeline in containers:

1.  **Build and Run:**
    ```bash
    docker-compose up --build
    ```

2.  **Clean Up:**
    ```bash
    docker-compose down
    ```