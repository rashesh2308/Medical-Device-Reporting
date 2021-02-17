# Medical-Device-Reporting-Kafka-Elasticsearch-SparkStreaming-Cassandra

In this tutorial, you’ll be learning how to create an application using Elasticsearch, Apache Kafka for Streaming Data.
The tutorial will focus more on deployment rather than analysis of the data.

I have deployed the project using:
· Docker
· Apache Kafka
· Elasticsearch
· Amazon S3
· Cassandra
· Spark Streaming

## Table of Contents:
1. Getting the Data
2. Storing the Data
3. Conversion of static data to streaming data
4. Architecture
5. Deploying

**SETUP:**

I used Docker to initially setup Kafka, Cassandra and Elasticsearch. I’ve used single node clusters for simplicity. Attached yaml file, Cassandra is using ports “9092:9092” ,”2181:2181", “9042:9042”.  

Versions:  
Docker — 19.03 (without WSL backend)  
Kafka — 2.2  
Cassandra — 3.11.4  
ElasticSearch — 7.10.0  
SparkStreaming — 3.0.0  
I decided to keep Spark on local rather than docker. In order to run SparkStreaming, we have to include external dependencies  
“spark-streaming-kafka-0–10_2.12–3.0.0.jar,   
spark-sql-kafka-0–10_2.12–3.0.0.jar,  
kafka-clients-2.5.0.jar,  
commons-pool2–2.8.0.jar,  
spark-token-provider-kafka-0–10_2.12–3.0.0.jar”  

**Architecture:**

![alt text](https://miro.medium.com/max/4800/1*SIiXl951qWcdZ0Sva2qHDg.jpeg)

**Getting the data:**

**What is the Enforcement Report?**

All recalls monitored by FDA are included in the Enforcement Report once they are classified and may be listed prior to classification when FDA determines the firm’s removal or correction of a marketed product(s) meets the definition of a recall.

Once FDA completes the hazard assessment, the Enforcement Report entry will be updated with the recall classification (Class I, II or III) indicating the hazard posed by the recalled product(s).

The dataset is available at [FDA](https://open.fda.gov/data/downloads/) website.
To get the data from API, I used request package.
url=requests.get(‘https://download.open.fda.gov/device/enforcement/device-enforcement-0001-of-0001.json.zip')

**Note:** The data is in json format which is static data. I have converted into the streaming data. The process for it is explained below.

**Storing the Data:**

When the homepage of the website is loaded it will trigger the API call, and infinite loop where every loop iteration will be delayed by 3600 seconds. While in loop it will refresh the S3 content and re-trigger the Kafka.

Since, the data is in json format, the data is converted in DataFrame after extracting zip file. This DataFrame is exported in csv and then the file is uploaded on S3.

At the same time reusing the DataFrame, each row of the df is sent to Kafka producer at port 9092.
Using the concept of multithreading, two threads have been created. 1) Consumer 2) Analytics

1. Consumer:

Here, I couldn’t pass the whole json from producer to consumer since it is a large file so, while the infinite loop is running, first thread is activated and using KafkaConsumer each row is inserted into enforcement table.

I sent the consumer data into the Cassandra database and have made all the columns as string datatype.

**ElaticSearch**

Elasticsearch is a search engine library which provides a distributed, multitienant-capable full-text search engine with an HTTP web interface and schema-free JSON document. Elasticsearch can be used to search all kinds of documents. It provides scalable search, has near real-time search, and supports multitenancy.

Since there’s a delay while loading in the Cassandra, if the thread is still inserting in database, Elasticsearch will take the data from Amazon S3, otherwise it will take the data from Cassandra database.

For simplicity, I’ve kept just one column — recall_number. ElasticSearch would be useful for Autocompletion as well as Searching.

**Insertion:**

I have used Elasticsearch helpers.bulk method to insert all the data into the Elasticsearch Database.

![alt text](https://miro.medium.com/max/4800/1*MDbhV1SZo0GZMVttVYEerw.png)

**Retrieval:**

For autocomplete dropdown list I am sending all the hits of ‘recall_number’ inside the ‘_source’ to html and for the recall_number submission we are iterating through the hits and getting all the data. Though it is not the optimal method to iterate using ‘For’ loop but it is a great starting point to learn Elasticsearch.

![alt text](https://miro.medium.com/max/4800/1*TVk2nd7a7Q0RdjYMX3xgcQ.png)

**Elastic Search Output:**

![alt text](https://miro.medium.com/max/4800/1*5Hp827ifGCvHRE8tvupVog.png)

**Spark Streaming:**

Spark Streaming provides an abstraction on the name of DStream which is a continuous stream of data. DStreams can be created using input sources or applying functions on existing DStreasms.

We are trying to capture the consumer output and making batches using the spark streaming via in memory sink method. I have applied aggregation after making batches and generated matplotlib graphs image and saved it inside the static folder and its get replaced by another image when next batch comes inside the spark.

The easiest way to get started with SparkStreaming is [LinkedinLearning](https://www.linkedin.com/learning/apache-spark-essential-training/introduction-to-streaming-analytics?u=2343682)

**Output:**

![alt text](https://miro.medium.com/max/1400/1*asZ4781RKYtYI5L8vSEp3g.png)


 Note: Use your own S3 Credentials. Docker-compose.yml should be run in order to run all microservices.

