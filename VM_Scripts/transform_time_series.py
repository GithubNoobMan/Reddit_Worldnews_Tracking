from google.cloud import bigquery
import pandas as pd
import os

def fetch_data_from_bigquery(project_id, dataset_id, table_id):
    """
    Fetches data from a BigQuery table into a pandas DataFrame.
    """
    client = bigquery.Client(project=project_id)
    table_fqid = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        SELECT id, TIMESTAMP_SECONDS(UNIX_SECONDS(created_utc)) AS created_utc
        FROM `{table_fqid}`
    """

    # Execute the query and return a pandas DataFrame
    df = client.query(query).to_dataframe()
    return df


def aggregate_articles(df):
    """
    Aggregates article data by hourly intervals, counting unique articles in each interval.
    """
    # Convert 'created_utc' column to datetime format
    df['created_utc'] = pd.to_datetime(df['created_utc'])

    # Set 'created_utc' as the index of the DataFrame
    df.set_index('created_utc', inplace=True)

    # Resample the data by one hour intervals and count unique article IDs in each interval
    result = df.resample('1H').nunique().rename(columns={'id': 'unique_articles_count'})

    # Reset the index to turn the datetime index back into a regular column
    result.reset_index(inplace=True)

    return result

def create_or_append_table_in_bigquery(dataframe, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        table = client.get_table(table_ref)
    except:
        print(f"Table {table_id} not found in dataset {dataset_id}, creating a new one.")
        schema = [
            bigquery.SchemaField('created_utc', 'TIMESTAMP'),
            bigquery.SchemaField('unique_articles_count', 'INTEGER')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config).result()

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
    bucket_name = "mage-zoom-class-abaker"
    prefix = "raw_reddit"  # Prefix for Parquet files
    #target_date = "20240327"  # Date string to filter files by. This is a future use case.
    project_id = "spring-line-411501"
    dataset_id = "Reddit_Refined"
    destination_table_id = "Article_Time_Series"
    source_table_id = "Article_Data"

    # Step 1: Fetch data from BigQuery
    df = fetch_data_from_bigquery(project_id, dataset_id, source_table_id)
    df = df.drop_duplicates()

    # Step 2: Aggregate articles by 60-minute intervals
    aggregated_df = aggregate_articles(df)

    # Step 3: Push the aggregated data to BigQuery
    create_or_append_table_in_bigquery(aggregated_df, project_id, dataset_id, destination_table_id)

    print(f"Aggregated data successfully pushed to {project_id}.{dataset_id}.{destination_table_id}")
