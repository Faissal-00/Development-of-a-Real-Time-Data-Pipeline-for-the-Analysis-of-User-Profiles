#!/usr/bin/env python
# coding: utf-8

# In[3]:


#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, concat_ws, current_date, datediff, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cassandra.cluster import Cluster

# Initialize Spark session
spark = SparkSession.builder\
        .appName("UserProfileAnalysis")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

# Define Kafka source configuration
kafka_source = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_profiles") \
    .load()

# Define the schema for parsing JSON data
json_schema = StructType([
    StructField("gender", StringType()),
    StructField("name", StructType([
        StructField("title", StringType()),
        StructField("first", StringType()),
        StructField("last", StringType())
    ])),
    StructField("location", StructType([
        StructField("street", StructType([
            StructField("number", IntegerType()),
            StructField("name", StringType())
        ])),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("postcode", IntegerType())
    ])),
    StructField("email", StringType()),
    StructField("login", StructType([
        StructField("uuid", StringType()),
        StructField("username", StringType()),
    ])),
    StructField("dob", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("registered", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("phone", StringType()),
    StructField("nat", StringType())
])

# Parse JSON data using the defined schema
parsedStreamDF = kafka_source.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), json_schema).alias("data")) \
    .select("data.*")

# 1. Build Full Name
parsedStreamDF = parsedStreamDF.withColumn("full_name", concat_ws(" ", "name.title", "name.first", "name.last"))

# 2. Calculate Age
parsedStreamDF = parsedStreamDF.withColumn("calculated_age", year(current_date()) - year(to_date("dob.date")))

# 3. Build Complete Address
parsedStreamDF = parsedStreamDF.withColumn("complete_address", concat_ws(", ", "location.street.number", "location.street.name", "location.city", "location.state", "location.country", "location.postcode"))

# 4. Delete the old columns
parsedStreamDF = parsedStreamDF.drop("name.title", "name.first", "name.last", "location.street.number", "location.street.name", "location.city", "location.state", "location.country", "location.postcode")

# # Define the Cassandra cluster
cluster = Cluster(['localhost'],port=9042)
session = cluster.connect()

# print(session)
# Define the keyspace name
keyspace = "user_profiles"

# Define the table name
table_name = "Users"

# Function to save DataFrame to Cassandra
def save_to_cassandra(df, keyspace, table_name):
    # Save the DataFrame to Cassandra
    df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .options(keyspace=keyspace)\
        .option(table=table_name) \
        .save()

# Define the streaming query
query = parsedStreamDF.writeStream.format('console').start()
query.awaitTermination()

