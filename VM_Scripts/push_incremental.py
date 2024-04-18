from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
import os
from io import BytesIO
from google.cloud import storage, bigquery
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import os
from google.api_core.exceptions import NotFound 
from google.cloud import bigquery
import pandas as pd

def create_or_append_table_in_bigquery(dataframe, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Convert 'created_utc' from Unix timestamp (float) to datetime before defining the schema
    dataframe['created_utc'] = pd.to_datetime(dataframe['created_utc'], unit='s')

    try:
        table = client.get_table(table_ref)  # Make an API request.
    except:
        print(f"Table {table_id} not found in dataset {dataset_id}, creating a new one.")
        # Define schema based on DataFrame dtypes
        schema = []
        for column in dataframe.columns:
            if column == 'created_utc':
                schema.append(bigquery.SchemaField(column, 'TIMESTAMP'))
            elif pd.api.types.is_string_dtype(dataframe[column]):
                schema.append(bigquery.SchemaField(column, 'STRING'))
            elif pd.api.types.is_numeric_dtype(dataframe[column]):
                schema.append(bigquery.SchemaField(column, 'FLOAT64'))  # Or 'NUMERIC', 'INTEGER' as appropriate
            else:
                schema.append(bigquery.SchemaField(column, 'STRING'))  # Fallback data type

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Make an API request to create the table.
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config).result()


def download_parquet_from_gcs(bucket_name, subfolder, target_date):
    """
    Downloads Parquet files from Google Cloud Storage that contain the target_date in their filenames.

    :param bucket_name: Name of the GCS bucket.
    :param subfolder: Prefix/path within the bucket where the files are located.
    :param target_date: The specific date string to look for in the filenames.
    :return: A combined DataFrame containing data from all the matching Parquet files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=subfolder)

    dfs = []  # List to hold dataframes created from each parquet file
    for blob in blobs:
        # Check if the blob's name matches the required pattern (subfolder, target_date, and .parquet extension)
        if blob.name.endswith('.parquet') and target_date in blob.name and blob.name.startswith(subfolder):
            byte_stream = BytesIO()
            blob.download_to_file(byte_stream)
            byte_stream.seek(0)
            df = pq.read_table(byte_stream).to_pandas()
            dfs.append(df)

    if dfs:  # If there are any dataframes in the list, concatenate them into a single dataframe
        return pd.concat(dfs)
    else:  # Return an empty dataframe if no matching files were found
        return pd.DataFrame()

# Other parts of your script remain unchanged...

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

    # Google Cloud Storage and BigQuery configurations
    bucket_name = "mage-zoom-class-abaker"
    prefix = "raw_reddit"  # Prefix for Parquet files
    target_date = "20240327"  # Date string to filter files by
    project_id = "spring-line-411501"
    dataset_id = "Reddit_Refined"
    table_id = "Article_Data"

    # Download Parquet files from GCS with the target date in their filenames
    combined_df = download_parquet_from_gcs(bucket_name, prefix, target_date)

    combined_df = combined_df.drop_duplicates()

    # If combined_df is not empty, proceed to upload it to BigQuery
    if not combined_df.empty:
        print('in here')
        combined_df = combined_df.drop_duplicates()
        create_or_append_table_in_bigquery(combined_df, project_id, dataset_id, table_id)
