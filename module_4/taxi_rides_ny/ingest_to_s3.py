import requests
import boto3
from botocore.exceptions import ClientError

# Configuration
BUCKET_NAME = "zoomcamp-data-shedrack-2026"
AWS_REGION = "eu-west-1"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Map taxi types to your S3 folder structure
FOLDER_MAP = {
    "green": "raw/greendata",
    "yellow": "raw/yellowdata",
    "fhv": "raw/fhv"
}

def file_exists(s3_client, bucket, key):
    """Check if object exists in S3 using a lightweight HEAD request."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def ingest_data(taxi_type, years):
    """Downloads and uploads data for a specific taxi type and year range."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    folder = FOLDER_MAP.get(taxi_type)
    
    if not folder:
        print(f"Error: Unknown taxi type '{taxi_type}'")
        return

    print(f"--- Processing: {taxi_type} ---")

    for year in years:
        for month in range(1, 13):
            # Construct standard filename pattern
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            s3_key = f"{folder}/{filename}"
            url = f"{BASE_URL}/{taxi_type}/{filename}"

            # Check S3 first (Idempotency)
            if file_exists(s3, BUCKET_NAME, s3_key):
                print(f"Skipping {filename} (Already in S3)")
                continue

            # Stream Download and Upload
            print(f"Downloading and Uploading: {filename}...")
            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    # Streaming directly to S3 saves local disk space
                    s3.upload_fileobj(r.raw, BUCKET_NAME, s3_key)
                    print(f"Uploaded {s3_key}")
            except Exception as e:
                print(f"Failed {filename}: {e}")

if __name__ == "__main__":
    # Ingest Green and Yellow Taxi Data
    for taxi in ["green", "yellow"]:
        ingest_data(taxi, [2019, 2020])

    # Ingest FHV Data (Required for Homework)
    ingest_data("fhv", [2019])
    
    print("All Ingestion Complete")