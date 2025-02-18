import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
from itertools import product
from pathlib import Path

# Change this to your bucket name
BUCKET_NAME = "zoomcamp-week-4-bucket"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/marc/data_engineering_zoomcamp_2025/4_data_analytics/secrets/zoomcamp-week-4-key.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
YEARS = [2019, 2020]
ALLOWED_TAGS = ['green', 'yellow', 'fhv']
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(tag, year, month):
    url = f"{BASE_URL}{tag}_tripdata_{year}-{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"{tag}_tripdata_{year}-{month}.parquet")
    if Path(file_path).exists():
        return file_path

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    # Determine which tags to process based on command-line parameters
    if len(sys.argv) >= 2:
        tag_arg = sys.argv[1]
        if tag_arg not in ALLOWED_TAGS:
            print(f"Invalid tag provided. Allowed tags: {ALLOWED_TAGS}")
            sys.exit(1)
        tags = [tag_arg]
    else:
        tags = ALLOWED_TAGS

    combinations = list(product(tags, YEARS, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(lambda args: download_file(*args), combinations))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")