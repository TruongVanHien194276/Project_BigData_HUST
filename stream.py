from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, quarter, dayofmonth, day
from pyspark import *
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col
import pandas
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

es = Elasticsearch('http://localhost:9200')
print(es.ping())

link_data = 'hdfs://localhost:9000/AAPL.csv'

sparkSession = SparkSession.builder.appName("BigData").getOrCreate()

df = sparkSession.read.csv(link_data, header = True, inferSchema=True)

# Them cac cot ngay, thang, nam, quy, ti le thay doi
df = df.withColumn('Year', year(df.Date))
df = df.withColumn('Month', month(df.Date))
df = df.withColumn('Day', day(df.Date))
df = df.withColumn('Quarter', quarter(df.Date))
df = df.withColumn('Change', (df.Close - df.Open)/df.Open*100)

df = df.toPandas()
print("Xong Spark")

for i, row in df.iterrows():
    doc = {
            'Date': row['Date'],
            'Open': row['Open'],
            'High': row['High'],
            'Low': row['Low'],
            'Close': row['Close'],
            'Adj Close': row['Adj Close'],
            'Volume': row['Volume'],
            'Change': row['Change'],
            'Year': row['Year'],
            'Month': row['Month'],
            'Day': row['Day'],
            'Quarter': row['Quarter']
        }
    es.index(index='data_apple', id=i, document=doc)
    # print("Xong " + str(i))
print("Xong Elasticsearch")

print(es.indices.get_alias().keys())