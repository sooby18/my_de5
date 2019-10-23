#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.sql.functions import explode

from pyspark.sql.functions import from_json

import re
from urllib.parse import urlparse
from urllib.request import urlretrieve, unquote

#Преобразование url
def url2domain(url):
    url = re.sub('(http(s)*://)+', 'http://', url)
    parsed_url = urlparse(unquote(url.strip()))
    if parsed_url.scheme not in ['http','https']: return None
    netloc = re.search("(?:www\.)?(.*)", parsed_url.netloc).group(1)
    if netloc is not None: return str(netloc.encode('utf8')).strip()
    return None

#Загрузка модели
from pyspark.ml import PipelineModel
model_reloaded =  PipelineModel.load("lab04_model")


topic_in = "maria_sokolova_lab04_in"
topic_out = "maria_sokolova_lab04_out"
# ! use your own IP
kafka_bootstrap = "10.0.0.8:6667"

spark = SparkSession.builder.appName("MLStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#Структура входящих данных
schema = StructType(
   fields = [
      StructField("uid", StringType(), True),
      StructField("visits", ArrayType(
          StructType([
               StructField("timestamp", LongType(), True),        
               StructField("url", StringType(), True)
               ])
      ),True)
])

## Считываем и распаковываем json-сообщения
st = spark \
  .readStream \
  .format("kafka") \
  .option("checkpointLocation", "/tmp/checkpoint-read")\
  .option("kafka.bootstrap.servers", kafka_bootstrap ) \
  .option("subscribe", topic_in) \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value as string)")\
  .select(from_json("value", schema).alias("value"))\
  .select(F.col("value.uid").alias("uid")\
  ,F.col("value.visits").alias("visits")\
  )\

## Формируем выходной датафрейм.

#UDF
url2domain_udf = F.udf(lambda xx: [ url2domain(x) for x in xx],
                   ArrayType(StringType()))
#Преобразование массивов url в датафрейме
df = st.withColumn("urls",url2domain_udf(st["visits"].getField("url")))

#Выбор полей для скармливания в модель
df = df.select(["uid", "urls"])

#Применение модели к датафрейму
df_trans = model_reloaded.transform(df)

#Выбор полей для выходного датафрейма
out_df = df_trans.select(["uid","gender_age"])

out_columns = list(out_df.columns)

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
