import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
import boto3
import time
from botocore.exceptions import ClientError, NoCredentialsError

# --- CONFIGURATION ---
BUCKET_NAME = "zoomcamp-data-shedrack-2026"  
REGION = "eu-west-1"                          

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Initialize S3 Client
# standard boto3.client() automatically pulls keys from your 'aws configure' setup
try:
    s3_client = boto3.client('s3', region_name=REGION)
except NoCredentialsError:
    print("Error: No AWS credentials found.")
    print("Please run 'aws configure' in your terminal first.")
    exit(1)

def verify_authentication():
    """Checks if the credentials are valid by asking AWS 'Who am I?'"""
    sts = boto3.client('sts', region_name=REGION)
    try:
        identity = sts.get_caller_identity()
        print(f"Authenticated as: {identity['Arn']}")
    except ClientError as e:
        print("Authentication failed. Check your keys.")
        print(e)
        exit(1)

def create_bucket_if_not_exists(bucket_name, region):
    """Creates an S3 bucket if it doesn't already exist."""
    print(f"Checking bucket: {bucket_name}...")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket does not exist. Creating '{bucket_name}' in {region}...")
            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Successfully created bucket: {bucket_name}")
            except ClientError as create_error:
                print(f"Failed to create bucket: {create_error}")
                exit(1)
        elif error_code == '403':
            print(f"Error: Forbidden. Bucket '{bucket_name}' exists but you don't have access.")
            print("Try changing the BUCKET_NAME to something unique.")
            exit(1)
        else:
            print(f"Unexpected error checking bucket: {e}")
            exit(1)

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    
    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def upload_to_s3(file_path):
    if not file_path: return
    
    file_name = os.path.basename(file_path)
    print(f"Uploading {file_name} to S3...")
    
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, file_name)
        print(f"Uploaded: s3://{BUCKET_NAME}/{file_name}")
        # Remove local file after successful upload
        os.remove(file_path)
    except Exception as e:
        print(f"Failed to upload {file_name}: {e}")

if __name__ == "__main__":
    # 1. Check Auth
    verify_authentication()
    
    # 2. Check/Create Bucket
    create_bucket_if_not_exists(BUCKET_NAME, REGION)

    print(f"--- Starting Processing for {len(MONTHS)} files ---")

    # 3. Download files (Parallel)
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # 4. Upload to S3 (Parallel)
    # We filter(None, file_paths) to skip any failed downloads
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_s3, filter(None, file_paths))

    print("All files processed successfully.")