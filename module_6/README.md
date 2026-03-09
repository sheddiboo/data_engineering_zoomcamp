# Module 6: Batch Processing with Apache Spark 🚀

This module focused on leveraging **Apache Spark** for scalable data processing. The project involved implementing a production-grade ETL pipeline that transitioned from local development in VS Code to a cloud-based execution on **AWS EC2**, interacting directly with data stored in **Amazon S3**.

---

## What is Apache Spark & PySpark?

To handle the massive scale of modern datasets, the project utilized:

* **Apache Spark:** A multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. It is designed for **distributed computing**, meaning it breaks large tasks into smaller pieces and processes them in parallel across many processors to save time.
* **PySpark:** The Python API for Apache Spark. It allows engineers to write clean Python code and use familiar libraries while the underlying Spark engine handles the heavy lifting in a high-performance Java Virtual Machine (JVM) environment.

---

## 🛠️ Tech Stack

* **Engine:** Apache Spark 3.5.3 (PySpark)
* **Cloud:** AWS EC2 (m7i-flex.large)
* **Storage:** Amazon S3 (Data Lake)
* **Environment:** Python 3.14 (Virtual Environment)

---

## 🏗️ Project Architecture

* **Local Development:** Spark transformations were developed and tested using Jupyter Notebooks (`.ipynb`) to analyze 2025 Yellow Taxi data.
* **Cloud Migration:** An AWS EC2 instance was provisioned and configured with an **IAM Instance Profile** to allow Spark to communicate with S3 securely without hardcoded credentials.
* **Data Lake Integration:** The `hadoop-aws` and `aws-java-sdk-bundle` packages were utilized to enable the **s3a://** filesystem protocol for direct cloud data access.
* **ETL Execution:** The `spark-submit` command was used to process millions of records, performing complex joins between taxi trip data and zone lookups, then writing the aggregated results back to S3 in the compressed **Parquet** format.

---

## 📊 Homework Highlights

### Question 2: Partitioning & Storage

The 2025-11 Yellow Taxi dataset was repartitioned into **4 chunks** to optimize parallel processing across the Spark cluster.

* **Average File Size:** Exactly **25MB** per partition.

### Question 3 & 4: Data Insights

Using **Spark SQL**, the following key metrics were extracted from the dataset:

* **Trip Count (Nov 15th):** 162,604 trips.
* **Longest Trip:** **90.64 hours** (calculated by handling `TIMESTAMP_NTZ` data types and utilizing `unix_timestamp` for duration calculation).

### Question 6: Least Frequent Pickup Zone

By joining the trip data with the `taxi_zone_lookup.csv`, the least frequent pickup zones were identified:

* **Result:** Governor's Island/Ellis Island/Liberty Island (tied with Arden Heights).
