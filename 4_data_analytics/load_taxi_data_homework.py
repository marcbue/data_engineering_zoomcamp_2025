import os
import sys
import urllib.request
import time
import gzip
import shutil
from itertools import product
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Import for Google Cloud Storage
from google.cloud import storage

# Change this to your bucket name
BUCKET_NAME = "zoomcamp-week-4-bucket"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/marc/data_engineering_zoomcamp_2025/4_data_analytics/secrets/zoomcamp-week-4-key.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
YEARS = [2019, 2020]
ALLOWED_TAGS = ['green', 'yellow', 'fhv']
DOWNLOAD_DIR = "."

# Use an 8MB chunk size for uploads (useful for large files)
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(tag, year, month):
    """
    Downloads the gzipped CSV file from the constructed URL.
    Returns the path to the downloaded file.
    """
    url = f"{BASE_URL}{tag}/{tag}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"{tag}_tripdata_{year}-{month}.csv.gz")
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

def decompress_file(file_path):
    """
    Decompresses a .gz file to a .csv file.
    If the decompressed file already exists, it returns its path.
    """
    if not file_path.endswith(".gz"):
        return file_path  # File is already uncompressed

    # Remove the .gz extension to form the new file name
    decompressed_file_path = file_path[:-3]
    if Path(decompressed_file_path).exists():
        return decompressed_file_path

    try:
        with gzip.open(file_path, 'rb') as f_in:
            with open(decompressed_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Decompressed {file_path} to {decompressed_file_path}")
        return decompressed_file_path
    except Exception as e:
        print(f"Failed to decompress {file_path}: {e}")
        return None

def verify_gcs_upload(blob_name):
    """
    Verifies that a blob with the given name exists in the GCS bucket.
    """
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    """
    Uploads a file to the GCS bucket with retries.
    """
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

    # Create all combinations for the selected tags, years, and months
    combinations = list(product(tags, YEARS, MONTHS))

    # 1. Download gzipped CSV files concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(lambda args: download_file(*args), combinations))

    # 2. Decompress the downloaded files concurrently (unpack the data)
    with ThreadPoolExecutor(max_workers=4) as executor:
        decompressed_file_paths = list(executor.map(decompress_file, filter(None, file_paths)))

    # 3. Upload the uncompressed CSV files to GCS concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, decompressed_file_paths))

    print("All files processed and uploaded to GCS.")
