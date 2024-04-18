# Reddit_Worldnews_Tracking
A project to pull in and ingest reddit worldnews new articles!

# Project Description
Reddit is the host of thousands of apps that enable users to see information in a convenient format to what it is that they are interested in. 

-We're pulling in the newest articles so a user can see news as soon as it's available. 

-The project will keep track also of the hourly trend of how many articles are produced in reddit/worldnews to showcase if there is any outlier behavior in the aggregate number of articles. Over time, trends and spikes could be seen as data is aggregated.

## Noteworthy Features

This code includes proper and continual logging as a feature of the consumer code. These logs get stored on a regular updated basis, with info and error messages. When a failure occurs, before the process terminates, the logs are updated to ensure that all errors are logged properly.

## Applicability

There are many practical applications that this process will mimick. For example, network traffic patterns can be used to determine anomalies and potentially shut down malicious traffic before it becomes a problem, or determine how to scale up and scale down the process based on the amount of traffic a website is experiencing, along with personal interest in the latest of news articles.

## Technologies Used

This project is an exercise in using Apache Kafka and Confluent technology, along with Python, VMs, GitHub, BigQuery, and Looker Studio and is a streaming data project. 

## Components of Project

This uses a data warehouse, includes multiple transformations and ingestion steps with a raw and refined dataset in BigQuery, in the cloud using streaming data with Kafka/Confluent and instructions for reproducing are below.

Potential future updates include containerizing the whole process, adding in airflow for orchestration, changing from Kafka to the reddit package in Python for costs savings, and creating proper unit tests for the process.

# Data Architecture

![Architecture Image](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/architecture_diagram.png)

## Architecture Steps

More detail in later sections, but this is the basic architecture. *This project is created entirely in the cloud.*

### 1. Reddit API

Interface with the Reddit API to get data relating to the worldnews subreddit. The Reddit API allows anyone with the initiative to glean lots of information from Reddit, from the hottest articles, comment details, or new articles (as used here). It's a simple, user friendly API that has a dedicated Python package for it (not used here, as the purpose of this project was to use Kafka).

### 2. Confluent Cluster

Create a confluent produce, cluster, and topic for the worldnews 'new' subscription to see what articles are new every 10 minutes. 

Confluent and Kafka provide a robust, easy to use way to ingest data in real-time with Confluent dealing with the cluster maintenance and creation while also providing helpful producer and consumer connectors. Most recently, Apache Flink capabilities are available in Confluent for immediate data transformation and analysis. 

See this helpful image from Confluent about tbe benefits of Confluent and Kafka.

![Confluent_Kafka_Propertes](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/kafka-intro.webp)

### 3. Python Consumer

Create a consumer that reads in messages, collects only the relevant data (article name, timestamp, id, and removed_by status) and pushes results to parquet files on a regular basis. 

It is expensive to write data to BigQuery in small batches. This can be done if necessary (and is easier) but in this case I wrote an intermediate step to parquet files in a google storage bucket.

This script performs multiple *transformations* on the data to select the data of interest and ensure that we are not bringing in duplicate data, as each API call will return the same article multiple times depending on how frequent the calls are and how many articles are produced.

### 4. Python BQ Upload Script (collectparquet.py)

This Script takes the parquet files daily, or all at once (I have both options created) and pushes them to BigQuery, a *data warehouse*. There are two tables in the dataset Reddit_Refined. The base metadata table and aggregated time series statistics for trend analysis.

### 5. Time Series Aggregator

Once a day, this script will take the basic data from each article and aggregate how many articles were posted on an hourly basis to the worldnews subreddit.

### 6. Looker Studio

Highlights every 15 minutes (assuming that data was being pushed that often, which it isn't due to expense, but the principal stands) and highlights the most recent articles *and* the overall trend of number of articles per hour since the data has been being collected for *two tiles* of information. 

Here's a snapshot of that vizualization:

![Create_API_Pic](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/DataVizualization.png)


# How to Recreate this Project

## 1. Create a Reddit API account. 

Go to https://www.reddit.com/wiki/api/ and follow instructions for creating a reddit account. Then you can go here https://old.reddit.com/prefs/apps/. 

Choose the option of identifying yourself as a developer for a 'script'. 

![Create_API_Pic](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/create_app_reddit.png)

Create and app and credentials that will be passed to the producer in Confluent so that it can talk to the Reddit API properly. Here's an example of what the credentials look like:

![Credential_API_Pic](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/reddit_api_credentials.png)

## 2. Confluent Cluster Creation

#### Sign up at https://www.confluent.io/ for an account that includes a free trial for you to start with. 

Then, create a basic cluster on your cloud provider of choice and region (does not matter where, but I chose GCP for consistency). 

![Cluster_Creation_Pic](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/Create_Cluster_Confluent.png)

#### Then create an Kafka API Key and then your 'HTTP Source' type Connector.

Here's how you should have your connector set up - note that the html request I am sending has a limit of 50 articles per API request:

![Connector_Creation_Pic](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/Connector_Settings.png)

#### Create a topic and connect it to your Connector. 

You can choose one partition because we are using a simple process with only one consumer and one producer.

Start the connector running and it should start collecting data. Note that it sometimes returns a failure intermittently due to too many requests to the Reddit API, but this is infrequent.

## 3. Create your VM and associated Software

Have a VM set up with Anaconda Python, VS Code with the SSH extension installed, and th-Create a VM in GCP

#### Create an SSH key on your personal computer in a terminal.
#### Share the public key with the VM by going to metadata settings and adding the public key.
#### Create a config file to use to tunnel into the VM in VS Code## 4. Start up the consumer in your virtual machine.
#### Create a role that allows you to pass data to and from Google Storage and BigQuery and download the json credentials to an appropriate place in your VM.

## 4. Start the consumer in the VM

#### After downloading all files in the project to your VM, run the following command:

nohup python consumer.py configuration.ini &

This will run the consumer in the background and start pulling data into parquet files in a google storage bucket. Some important notes:

** Change the name of the bucket to your bucket **
** Change the path to your google credential key **
** Initialize prior_ids/prior_ids.json file in your bucket with an empty dataset **

## 5. Run collectparquet.py

After at least one parquet file is available, run this script to initialize the base table in BigQuery. **You will have to change the name of the project_id to your project_id and target the google credentials to where yours are located**.

## Run transform_time_series.py

This will take the reddit_raw dataset and transform it into an aggregated time series for analysis on an hourly basis. **You will have to change the name of the project_id to your project_id and target the google credentials to where yours are located**.

This script will create or append to an already created table.

## 6. Create Looker Studio Vizualizations

#### Configure and connect Looker Studio to BigQuery 
#### Bring in the two data tables Article_Data and Aricle_Data_Time_Series
#### Create a line graph and sort the articles by recency. 
#### Update the frequency in the settings to every 15 minutes for refreshing the data.


# Relevant Features of Code

## Consumer.py

### Logging

There is logging and try/except blocks throughout the code that allow for troubleshooting, including info and error logging into google cloud storage.

The logs are datetime stamped so that they are easier to determine if they are relevant to a troubleshooting investigation.

Currently retrieved data is also dumped into bigquery before a failure in the code shuts the script down.

### Data Redundancy Check

The prior_ids stored in google cloud bucket ensure that we don't ingest into parquet files the same news article twice. There is a small chance of failure here if the process fails and the priod_id's are not updated in time.

### Transformations

The Reddit API brings in hundreds of properties per article, and 50 articles per pull. That's a lot of data we don't need. All we need is the id, title, if it has been removed, and timestamp. 

#### Pulled out the relevant data from each topic message
#### Ensure that we haven't seen this article ID before
#### Use a counter to ensure we have enough data that it's worth storing in a parquet file
The requirement is for 10 API pings to occur, which occur every 10 minutes, or store results every 100 minutes into a google bucket.

## collectparquet.py

### Downloads data with set prefix

This prefix pulls in *all* data that is relevant and available in parquet files.

### Duplicate protection

Since article title updates are pretty rare, this is a simple and mostly effective way to ensure we don't dump the same news article twice, just in case our redundancy efforts in the consumer failed (say, through a consumer restart).


### push_incremental.py

This code will take a daily specific date and only pull in parquet data from that date. This would be the python script to run daily to update the bigquery article_data table.

This code is very similar to the code we see in collectparquet, except the filter is different (includes the date).

### transform_time_series.py

Also a simple script that aggregates the number of columns per hour from the Article_Data table. For this to run daily, a time filter should be put on the BigQuery query based on the current datetime.

### deleteparquet.py

While there are some lifecycle rules that could do this in cloud storage, I opted to create a script that gets rid of parquet files more than 10 days old. After awhile, the number of parquet files can become staggering and add to costs.
