# Reddit_Worldnews_Tracking
A project to pull in and ingest reddit worldnews new articles

# Project Description
Reddit is the host of thousands of apps that enable users to see information in a convenient format to what it is that they are interested in. In this case, we're pulling in the newest articles so that a user can see news as soon as it's available. The project will keep track also of the hourly trend of how many articles are produced in the subreddit of worldnews to showcase if there is any outlier behavior in terms of the aggregate number of articles that are being produced. Over time, trends and spikes could be seen as data is aggregated over an extended period of time (years, probably).

There are many practical applications that this process will mimick. For example, network traffic patterns can be used to determine anomalies and potentially shut down malicious traffic before it becomes a problem, or determine how to scale up and scale down the process based on the amount of traffic a website is experiencing, along with personal interest in the latest of news articles.

This project is an exercise in using Apache Kafka and Confluent technology, along with Python, VMs, GitHub, BigQuery, and Looker Studio. 

# Data Architecture

![Example Image](https://github.com/GithubNoobMan/Reddit_Worldnews_Tracking/blob/main/images/architecture_diagram.png)

