
# Module 1 Homework: Docker & SQL

This contains the solutions for the Module 1 homework of the Data Engineering Zoomcamp (2026).

## Environment Setup

1.  **Start the Database:**
    ```bash
    docker compose up -d
    ```

2.  **Ingest Data:**
    The `ingest.py` script downloads the Green Taxi Data (Nov 2025) and Taxi Zones, and loads them into Postgres.
    ```bash
    python ingest.py
    ```

## Homework Answers

### Question 1: Understanding Docker Images
I ran the python image with bash entrypoint:
```bash
docker run -it --entrypoint bash python:3.13
pip --version

```

**Answer:** `25.3`

### Question 2: Docker Networking

Inside the Docker network, the container hostname matches the service name defined in `docker-compose.yaml`.
**Answer:** `db:5432`

### Question 3: Counting Short Trips

**Query:**

```sql
SELECT COUNT(*) 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1.0;

```

**Answer:** `8007`

### Question 4: Longest Trip Day

**Query:**

```sql
SELECT DATE(lpep_pickup_datetime), MAX(trip_distance)
FROM green_taxi_trips
WHERE trip_distance < 100
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

```

**Answer:** `2025-11-14`

### Question 5: Biggest Pickup Zone (Nov 18)

**Query:**

```sql
SELECT z."Zone", SUM(t.total_amount)
FROM green_taxi_trips t
JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

```

**Answer:** `East Harlem North`

### Question 6: Largest Tip Drop-off

**Query:**

```sql
SELECT zdo."Zone", MAX(t.tip_amount)
FROM green_taxi_trips t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01' AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;

```

**Answer:** `Yorkville West`

### Question 7: Terraform Workflow

**Answer:** `terraform init, terraform apply -auto-approve, terraform destroy`

