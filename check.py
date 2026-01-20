import boto3
import lzma
import os
import shutil
from operator import itemgetter

# --- Configuration ---
BUCKET_NAME = 'golem-stats-database-backup'
PREFIX = 'db-backups/'
FILE_EXTENSION = '.sql.xz'
DOWNLOAD_DIR = './downloads'

def get_latest_backup_key(s3_client, bucket, prefix):
    """
    Lists files and returns the key of the newest file based on filename timestamp.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        print(f"Error listing objects: {e}")
        return None

    if 'Contents' not in response:
        return None

    # Filter for specific extension
    files = [
        obj for obj in response['Contents']
        if obj['Key'].endswith(FILE_EXTENSION)
    ]

    if not files:
        return None

    # Sort by 'Key' descending to get the latest timestamp
    latest_file = sorted(files, key=itemgetter('Key'), reverse=True)[0]
    return latest_file['Key']

def download_and_unpack():
    # boto3 will now automatically find the credentials from environment variables or config files
    s3 = boto3.client('s3')

    print(f"Searching for newest {FILE_EXTENSION} file in s3://{BUCKET_NAME}/{PREFIX}...")

    latest_key = get_latest_backup_key(s3, BUCKET_NAME, PREFIX)

    if not latest_key:
        print("No matching backup files found.")
        return

    filename = os.path.basename(latest_key)
    local_archive_path = os.path.join(DOWNLOAD_DIR, filename)
    output_sql_path = os.path.splitext(local_archive_path)[0]

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # 1. Download
    print(f"Downloading: {latest_key} -> {local_archive_path}")
    s3.download_file(BUCKET_NAME, latest_key, local_archive_path)

    # 2. Unpack (.xz)
    print(f"Unpacking to: {output_sql_path} ...")
    try:
        with lzma.open(local_archive_path, mode='rb') as f_in:
            with open(output_sql_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Success! File unpacked.")

        # Optional: cleanup
        # os.remove(local_archive_path)

    except lzma.LZMAError as e:
        print(f"Error unpacking LZMA/XZ file: {e}")

if __name__ == "__main__":
    download_and_unpack()