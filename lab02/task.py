#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql.functions import from_json


topic_in = "maria_sokolova"
topic_out = topic_in + "_lab02_out"
# ! use your own IP
kafka_bootstrap = "10.0.0.8:6667"

spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')


schema = StructType(
   fields = [
      StructField("timestamp", LongType(), True),
      StructField("referer", StringType(), True),
      StructField("location", StringType(), True),
      StructField("remoteHost", StringType(), True),
      StructField("partyId", StringType(), True),
      StructField("sessionId", StringType(), True),
      StructField("pageViewId", StringType(), True),
      StructField("eventType", StringType(), True),
      StructField("item_id", StringType(), True),
      StructField("item_price",IntegerType(), True),
      StructField("item_url", StringType(), True),
      StructField("basket_price",StringType(), True),
      StructField("detectedDuplicate", BooleanType(), True),
      StructField("detectedCorruption", BooleanType(), True),
      StructField("firstInSession", BooleanType(), True),
      StructField("userAgentName", StringType(), True),
])

## Считываем и распаковываем json-сообщения
st = spark \
  .readStream \
  .format("kafka") \
  .option("checkpointLocation", "tmp/lab02/checkpoint-read")\
  .option("kafka.bootstrap.servers", kafka_bootstrap ) \
  .option("subscribe", topic_in) \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value as string)")\
  .select(from_json("value", schema).alias("value"))\
  .select(F.col("value.timestamp").alias("timestamp_unix")\
  ,F.col("value.partyId").alias("partyId")\
  ,F.col("value.sessionId").alias("sessionId")\
  ,F.col("value.eventType").alias("eventType")\
  ,F.col("value.item_price").alias("item_price")\
  ,F.col("value.detectedDuplicate").alias("detectedDuplicate")\
  ,F.col("value.detectedCorruption").alias("detectedCorruption")\
  )\


## Формируем выходной датафрейм.
out_df = st.filter("detectedDuplicate='false' and detectedCorruption='false'")
from pyspark.sql.functions import from_unixtime
timestamp = from_unixtime(out_df["timestamp_unix"]/1000)
out_df = out_df.withColumn("timestamp",timestamp)



out_columns = list(out_df.columns)

query = out_df\
    .select(F.to_json(F.struct(*out_columns)).alias("value"))\
    .writeStream \
    .outputMode("update")\
    .format("kafka") \
    .option("checkpointLocation", "/tmp/checkpoint-write")\
    .option("kafka.bootstrap.servers", kafka_bootstrap ) \
    .option("topic", topic_out) \
    .start()

query.awaitTermination()
