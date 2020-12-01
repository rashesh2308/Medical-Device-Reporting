import zipfile
import flask
from flask import request
import numpy as np
from flask import Flask
from io import BytesIO, StringIO
import pandas as pd
import time as t
import findspark
from pyspark.sql.types import StructType, StringType, StructField
import matplotlib.pyplot as plt
findspark.init('C:\opt\spark-3.0.0-bin-hadoop2.7')
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
import smart_open
from boto.s3.key import Key
import boto
from cassandra.cluster import Cluster
import threading
from kafka import KafkaConsumer
from kafka import KafkaProducer, KafkaClient
import json, requests
from elasticsearch import Elasticsearch, helpers
cluster = Cluster(['127.0.0.1'], connect_timeout=100)
session = cluster.connect("Devices")
app = Flask(__name__)
Flag = 0
Flags=False
def producer():
    global Flags
    t.sleep(100)
    url = '192.168.56.1:9092'
    kafka = KafkaClient(bootstrap_servers=url)
    producer = KafkaProducer(bootstrap_servers=["192.168.56.1:9092"])
    counter =1
    while counter:

        url = requests.get('https://download.open.fda.gov/device/enforcement/device-enforcement-0001-of-0001.json.zip')

        d = None
        data = None
        with zipfile.ZipFile(BytesIO(url.content), "r") as z:
            for filename in z.namelist():
                print(filename)
                with z.open(filename) as f:
                    data = f.read()
                    data = json.loads(data)

        data = pd.DataFrame(data['results'])
        data = data.replace({np.NaN:None})
        data = data.drop('openfda',1)
        data.insert(0, 'ID', range(0, len(data)))
        data['ID'] = data['ID'].astype('str')
        # upload to S3
        conn = boto.connect_s3('AKIAQ4UBQVDV3RRYSD5Y', 'x5K/PgZwoDY8O/N+QVg99Lm2TnnkgB4mp981wGy4')
        bucket = conn.get_bucket('device-enforcement')
        upload = Key(bucket)

        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        filename = 'dataset.csv'
        upload.key = filename
        upload.set_contents_from_string(csv_buffer.getvalue())

        # start consumer
        t2 = threading.Thread(target=consumer)
        t2.start()
        t3 = threading.Thread(target=Analytics1)
        t3.start()
        for index,rows in data.iterrows():
            x = rows.to_dict()
            producer.send("devices",json.dumps(x).encode('utf-8'))
            Flags = True

        t.sleep(3600)

    kafka.close()
def consumer():
    global Flag
    Flag = 1


    query = """ truncate table enforcement"""
    session.execute(query)
    kafka_brokers_list = ["192.168.56.1:9092"]  # We can put multiple brokers here.

    # Kafka Consumer
    consumer = KafkaConsumer("devices", bootstrap_servers=kafka_brokers_list)
    for message in consumer:
        item = json.loads(message.value.decode('utf8'))


        query = """INSERT INTO enforcement(id, address_1, address_2, center_classification_date, city,classification, code_info, country, distribution_pattern,
       event_id, initial_firm_notification, more_code_info, 
       postal_code, product_description, product_quantity,
       product_type, reason_for_recall, recall_initiation_date,
       recall_number, recalling_firm, report_date, state, status,
       termination_date, voluntary_mandated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


        session.execute(query, (
            item['ID'], item['address_1'], item['address_2'], item['center_classification_date'], item['city'], item['classification'], item['code_info'], item['country'],
            item['distribution_pattern'], item['event_id'], item['initial_firm_notification'], item['more_code_info'],
            item['postal_code'], item['product_description'], item['product_quantity'],
            item['product_type'], item['reason_for_recall'], item['recall_initiation_date'],
            item['recall_number'], item['recalling_firm'], item['report_date'], item['state'], item['status'],
            item['termination_date'], str(item['voluntary_mandated'])))
    Flag = 0



def Analytics1():
    while (1):
        global Flags
        if Flags == True:
            spark = SparkSession \
                .builder \
                .config("spark.jars",
                        "spark-streaming-kafka-0-10_2.12-3.0.0.jar,spark-sql-kafka-0-10_2.12-3.0.0.jar,kafka-clients-2.5.0.jar,commons-pool2-2.8.0.jar,spark-token-provider-kafka-0-10_2.12-3.0.0.jar") \
                .appName("Q1") \
                .getOrCreate()
            data_spark_schema = StructType([
                StructField("classification", StringType(), True),
                StructField("status", StringType(), True),
                StructField("initial_firm_notification", StringType(), True)])

            streamingInputDF = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "192.168.56.1:9092") \
                .option("subscribe", "devices") \
                .load()
            stream_records = streamingInputDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as devices") \
                .select(psf.from_json("devices", data_spark_schema).alias("devices"))
            stream_records.printSchema()
            stream_records = stream_records.select("devices.*").drop("devices")

            z= stream_records.writeStream.outputMode("append").format("memory").queryName("view").trigger(processingTime=' 2 seconds').start()
            while(z.isActive):
                t.sleep(15)
                x = spark.sql("""select classification, count(classification) from view group by classification""")
                x = x.toPandas()
                x.plot(x='classification', y='count(classification)', kind='bar')
                plt.savefig('C:\\Users\\balla\\PycharmProjects\\ORIM1\\untitled\\static\\output.png')
            break

@app.route('/analytics')
def Analytics():

    return flask.render_template('Analytics.html',name='Classification1')


@app.route('/',methods=["GET","POST"])
def hello_world():
    global Flag
    global Flags
    es = Elasticsearch(['localhost'], port=9200)
    t1 = threading.Thread(target=producer)
    t1.start()
    if Flag == 1:
        es.indices.delete(index='devices', ignore=[400, 404])
        es.indices.create(index='devices', body={})

        path = 's3://{}:{}@{}/{}'.format('AKIAQ4UBQVDV3RRYSD5Y', 'x5K/PgZwoDY8O/N+QVg99Lm2TnnkgB4mp981wGy4', 'device-enforcement', 'dataset.csv')
        df = pd.read_csv(smart_open.open(path))
        df = df.replace({np.NaN: None})

        documents = df.to_dict(orient='records')
        helpers.bulk(es, documents, index='devices', doc_type='records', raise_on_error=True)
    else:
        es.indices.delete(index='devices', ignore=[400, 404])
        es.indices.create(index='devices', body={})
        query = """ select * from enforcement"""
        df=pd.DataFrame(list(session.execute(query)))
        df = df.replace({np.NaN: None})
        documents = df.to_dict(orient='records')

        helpers.bulk(es, documents, index='devices', doc_type='records', raise_on_error=True)




    return flask.render_template('Index.html')
@app.route('/search',methods=["GET","POST"])
def search():
    es = Elasticsearch(['localhost'], port=9200)
    documents = es.search(index='devices', body={}, size=50)['hits']['hits']
    recall = [hit["_source"]["recall_number"] for hit in documents]
    recall = list(set(recall))
    if request.method == "POST":
        keyword = request.form.get('keyword')
        li =[]
        result = [hit["_source"] for hit in documents]

        for x in result:

            if x["recall_number"] == keyword:
                li.append(x)
        result = li

    else:

        result = [hit["_source"] for hit in documents]





    return flask.render_template('search.html', result=result, response= recall)

if __name__ == '__main__':
    app.run()
