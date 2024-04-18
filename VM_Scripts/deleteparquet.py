from google.cloud import storage
import datetime
import os

def delete_old_parquet_files(bucket_name, subfolder, days_old=10):
    """
    Deletes Parquet files from a GCS bucket subfolder if they are older than a specified number of days.

    :param bucket_name: Name of the GCS bucket.
    :param subfolder: Prefix/path within the bucket to the target subfolder.
    :param days_old: Files older than this number of days will be deleted.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=subfolder)
    cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_old)

    for blob in blobs:
        if blob.name.endswith('.parquet') and blob.time_created < cutoff_date:
            print(f"Deleting {blob.name}...")
            blob.delete()

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-file.json"

    # Configuration
    bucket_name = "mage-zoom-class-abaker"
    subfolder = "raw_reddit"  # prefix for Parquet files

    # Call the function to delete old Parquet files
    delete_old_parquet_files(bucket_name, subfolder)
