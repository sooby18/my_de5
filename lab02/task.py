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

#https://spark.apache.org/docs/2.3.0/structured-streaming-programming-guide.html
start_ts = F.window(out_df["timestamp"],"10 minutes","10 minutes")
out_df = out_df.withColumn("start_ts",F.unix_timestamp(start_ts.getField("start"))).withColumn("end_ts",F.unix_timestamp(start_ts.getField("end")))

#out_df = out_df.filter("start_ts > '2018-12-18 10:00:00'")

out_df = out_df.groupBy("start_ts","end_ts")\
        .agg(\
          F.sum(F.when(F.col("eventType")=="itemBuyEvent",F.col("item_price"))).alias("revenue")\
         ,F.approx_count_distinct(F.col("partyId")).alias("visitors")\
         ,F.approx_count_distinct(F.when(F.col("eventType")=="itemBuyEvent",F.col("sessionId"))).alias("purchases")\
            )

out_df = out_df.withColumn("aov",F.col("revenue")/F.col("purchases"))
            

#out_df = out_df.groupBy("start_ts")\
#                            .agg(\
#                                 F.countDistinct("partyId").alias("visitors")\
#                                 )\
#                                 .join(\
#                            out_df.filter("eventType='itemBuyEvent'").groupBy("start_ts")\
#                            .agg(F.max("end_ts").alias("end_ts"),\
#                                 F.sum("item_price").alias("revenue"),\
#                                 F.countDistinct("sessionId").alias("purchases"),\
#                                 F.sum("item_price")/F.countDistinct("sessionId")\
#                                 )\
#                                 ,"start_ts", "left")


out_columns = list(out_df.columns)
#out_columns = ["start_ts","end_ts","visitors","revenue","purchases","aov"]

query = out_df\
    .select(F.to_json(F.struct(*out_columns)).alias("value"))\
    .writeStream \
    .outputMode("update")\
    .format("kafka") \
    .option("checkpointLocation", "/tmp/checkpoint-write")\
    .option("kafka.bootstrap.servers", kafka_bootstrap ) \
    .option("topic", topic_out) \
    .start()\

query.awaitTermination()
