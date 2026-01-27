# Terraform Infrastructure (Local Cloud)

This directory contains the Terraform configuration for spinning up a local data engineering environment using Docker. It mimics a cloud setup without needing a cloud provider account.

## Resources Created

* **MinIO:** An S3-compatible object storage server (mimics Google Cloud Storage).
    * **Port:** `9001` (Console), `9000` (API)
    * **Volume:** `./minio_data` (Persistent storage)
* **PostgreSQL:** A relational database (mimics BigQuery/Data Warehouse).
    * **Port:** `5433` (External access)
    * **Database:** `ny_taxi`
* **Docker Network:** `data_network` (Connects MinIO and Postgres).

## Usage

1.  **Initialize Terraform:**
    ```bash
    terraform init
    ```
2.  **Plan the Infrastructure:**
    ```bash
    terraform plan
    ```
3.  **Apply (Build) the Infrastructure:**
    ```bash
    terraform apply -auto-approve
    ```
4.  **Destroy (Shut Down):**
    ```bash
    terraform destroy -auto-approve
    ```

## Files
* `main.tf`: The primary configuration file defining the Docker provider and resources.
* `variables.tf`: Defines configurable variables (e.g., container names, ports).