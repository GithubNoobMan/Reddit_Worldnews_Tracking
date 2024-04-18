# Reddit_Worldnews_Tracking
A project to pull in and ingest reddit worldnews new articles

# Project Description
Reddit is the host of thousands of apps that enable users to see information in a convenient format to what it is that they are interested in. In this case, we're pulling in the newest articles so that a user can see news as soon as it's available. The project will keep track also of the hourly trend of how many articles are produced in the subreddit of worldnews to showcase if there is any outlier behavior in terms of the aggregate number of articles that are being produced. Over time, trends and spikes could be seen as data is aggregated over an extended period of time (years, probably).

There are many practical applications that this process will mimick. For example, network traffic patterns can be used to determine anomalies and potentially shut down malicious traffic before it becomes a problem, or determine how to scale up and scale down the process based on the amount of traffic a website is experiencing, along with personal interest in the latest of news articles.

This project is an exercise in using Apache Kafka and Confluent technology, along with Python, VMs, GitHub, BigQuery, and Looker Studio and is a streaming data project.

# Data Architecture

![Example Image](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/architecture_diagram.png)

## Architecture Steps

More detail will follow, but this is the basic steps. This project is created entirely in the cloud.

### 1. Reddit API

Interface with the Reddit API to get data relating to the worldnews subreddit.

### 2. Confluent Cluster

Create a confluent produce, cluster, and topic for the worldnews 'new' subscription to see what articles are new every 10 minutes.

### 3. Python Consumer

Create a consumer that reads in messages, collects only the relevant data (article name, timestamp, id, and removed_by status) and pushes results to parquet files on a regular basis. This is because it is expensive to write data to BigQuery in small batches. This can be done if necessary (and is easier) but in this case I wrote an intermediate step to parquet files in a google storage bucket.

This script performs multiple *transformations* on the data to select the data of interest and ensure that we are not bringing in duplicate data, as each API call will return the same article multiple times depending on how frequent the calls are and how many articles are produced.

### 4. Python BQ Upload Script

This Script takes the parquet files daily, or all at once (I have both options created) and pushes them to BigQuery, a *data warehouse*. 

### 5. Time Series Aggregator

Once a day, this script will take the basic data from each article and aggregate how many articles were posted on an hourly basis to the worldnews subreddit.



