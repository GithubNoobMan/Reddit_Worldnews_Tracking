#!/usr/bin/env python

import sys
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from datetime import datetime, timedelta,timezone
from google.cloud import storage
import pandas as pd
import pyarrow
import os
import logging
from google.cloud import bigquery


def read_prior_ids_from_gcs(bucket_name, source_blob_name):
    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Get the blob from the specified bucket
    blob = bucket.blob(source_blob_name)
    
    # Download the contents of the blob as a string
    prior_ids_str = blob.download_as_string()
    
    # Convert the string back to a list
    prior_ids = json.loads(prior_ids_str)
    
    return prior_ids


def write_prior_ids_to_gcs(prior_ids, bucket_name, destination_blob_name):
    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Create a new blob in the specified bucket
    blob = bucket.blob(destination_blob_name)
    
    # Convert the list of prior_ids to a string
    prior_ids_str = json.dumps(prior_ids)
    
    # Upload the string to GCS
    blob.upload_from_string(prior_ids_str)
    


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the specified bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)



def upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name):
    try:
        # Initialize the GCS client
        storage_client = storage.Client()

        # Get the GCS bucket object
        bucket = storage_client.bucket(bucket_name)

        # Upload the local log file to GCS
        blob = bucket.blob(gcs_log_file_path)
        blob.upload_from_filename(local_log_file_path)

        logging.info('Log file uploaded to GCS successfully.')
        try:
            os.remove(source_file_name)
            
        except OSError as e:
            print(f"Error deleting local file {source_file_name}: {e.strerror}")
    except Exception as e:
        # If an exception occurs during the upload process, log the error
        logging.error(f'Error uploading log file to GCS: {str(e)}')

    # Set up logging
    

   

if __name__ == '__main__':

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

    #bucket_name = os.environ.get('bucket_name')
    bucket_name = 'mage-zoom-class-abaker'
    gcs_log_file_path = f'logs/consumer_new/app_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")}' 

    local_log_file_path = f'consumer_log_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")}'
    #local_log_file_path = f'/app/{local_log_filename}'  

    logging.basicConfig(filename=local_log_file_path, level=logging.INFO)

    try:

        client = bigquery.Client()

        now_utc = datetime.now(timezone.utc)

    except Exception as e:
            # Log the exception
            logging.error(f'Error Connecting to client: Log file to GCS: {str(e)})')

            # Upload log file to GCS regardless of whether the script completes or not
            upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)

    try:
  
        # Pull in the info current articles ingested. FIX THIS.
        prior_ids = read_prior_ids_from_gcs(bucket_name, "prior_ids/prior_ids.json")

        logging.info(prior_ids)

    except Exception as e:
            # Log the exception
            logging.error(f'Error trying to query in ids: Log file to GCS: {str(e)})')

            # Upload log file to GCS regardless of whether the script completes or not
            upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)
    try: 
        # Parse the command line.
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        parser.add_argument('--reset', action='store_true')
        args = parser.parse_args()

        # Parse the configuration.
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        config_parser = ConfigParser()
        config_parser.read_file(args.config_file)
        config = dict(config_parser['default'])
        config.update(config_parser['consumer'])

        # Create Consumer instance
        consumer = Consumer(config)

    except Exception as e:
            # Log the exception
            logging.error(f'Error Parsing Config File: Log file to GCS: {str(e)})')

            # Upload log file to GCS regardless of whether the script completes or not
            upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)

    try:
        # Set up a callback to handle the '--reset' flag.
        def reset_offset(consumer, partitions):
            if args.reset:
                for p in partitions:
                    p.offset = OFFSET_BEGINNING
                consumer.assign(partitions)

        # Subscribe to topic
        topic = "get_new_articles"
        consumer.subscribe([topic], on_assign=reset_offset)

    except Exception as e:
            # Log the exception
            logging.error(f'Error setting topic: Log file to GCS: {str(e)})')

            # Upload log file to GCS regardless of whether the script completes or not
            upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)

    # Poll for new messages from Kafka and print them.
    try:
        counter = 0
        articles_data = []

        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    #print("Waiting...")
                    pass
                elif msg.error():
                    logging.error(msg.error())
                else:
                    counter = counter + 1

                    try:

                        # Extract the (optional) key and value, and print.
                        key = msg.key().decode('utf-8') if msg.key() is not None else None
                        value = msg.value().decode('utf-8') if msg.value() is not None else None
                            
                        data = json.loads(value)

            
                    except Exception as e:
                        # Log the exception
                        logging.error(f'Error key/value decoding: Log file to GCS: {str(e)})')

                        # Upload log file to GCS regardless of whether the script completes or not
                        upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)
\
                        write_prior_ids_to_gcs(prior_ids, bucket_name, "prior_ids/prior_ids.json")                        

                    try:
                
                        # Extract and print the desired information for each article
                        for article in data['data']['children']:
                               
                                article_data = article['data']
                                if article_data.get('id') not in prior_ids:
                                    article_dict = {'id': article_data.get('id'),
                                    'created_utc': article_data.get('created_utc'),
                                    'title': article_data.get('title'),
                                    'removed_by' : article_data.get('removed_by', 'N/A')}
                                    prior_ids.append(article_data.get('id'))
                                    articles_data.append(article_dict)
                        
                  

                        df = pd.DataFrame(articles_data)

                        if counter % 10 == 0:
                     
                            #reset counter                                
                            
                            logging.info(df)

                            # Generate a time-coded filename
                            filename = f"articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

                            df.to_parquet(filename, engine = 'pyarrow')                

                            # Upload the file to Google Cloud Storage

                            destination_blob_name = f"raw_reddit/{filename}"
                            bucket_name = 'mage-zoom-class-abaker'

                            upload_to_gcs(bucket_name = bucket_name, source_file_name = filename, destination_blob_name = destination_blob_name)

                            articles_data = []

                        if counter % 30 == 0:
                    
                            upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)
                    
                            write_prior_ids_to_gcs(prior_ids, bucket_name, "prior_ids/prior_ids.json")


                    except Exception as e:
                        # Log the exception
                        logging.error(f'Error article extraction and upload: Log file to GCS: {str(e)})')

                        # Upload log file to GCS regardless of whether the script completes or not
                        upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)
                        write_prior_ids_to_gcs(prior_ids, bucket_name, "prior_ids/prior_ids.json")

                        ##Create batch process to upload to google BQ. This is outside of this script, really!

            except Exception as e:
                # Log the exception
                logging.error(f'Error during listening topic: Log file to GCS: {str(e)})')
                write_prior_ids_to_gcs(prior_ids, bucket_name, "prior_ids/prior_ids.json")

                # Upload log file to GCS regardless of whether the script completes or not
                upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)

    except KeyboardInterrupt:
        # Log the exception
        logging.info(f'Process Finished')

        filename = f"articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

        try:
            if counter > 0:
                df.to_parquet(filename, engine = 'pyarrow')                

                # Upload the file to Google Cloud Storage

                destination_blob_name = f"raw_reddit/{filename}"
                bucket_name = 'mage-zoom-class-abaker'

                upload_to_gcs(bucket_name = bucket_name, source_file_name = filename, destination_blob_name = destination_blob_name)

                # Upload log file to GCS regardless of whether the script completes or not
                upload_log_to_gcs(local_log_file_path, gcs_log_file_path, bucket_name)

                write_prior_ids_to_gcs(prior_ids, bucket_name, "prior_ids/prior_ids.json")
        
        except Exception as e:
            logging.error(f'Error during final push: Log file to GCS: {str(e)})')
            logging.info('No Final Push created')
	
    finally:
        # Leave group and commit final offsets
        consumer.close()
