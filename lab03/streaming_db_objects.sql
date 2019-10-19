create table tst_kafka_queue \
(\
     timestamp UInt64,\
     referer String,\
     location String,\
     remoteHost String,\
     partyId String,\
     sessionId String,\
     pageViewId String,\
     eventType String,\
     item_id String,\
     item_price Int64,\
     item_url String,\
     basket_price Float64,\
     detectedDuplicate UInt8,\
     detectedCorruption UInt8,\
     firstInSession UInt8,\
     userAgentName String\
)\
engine=Kafka() \
SETTINGS \
    kafka_broker_list = 'de-5-maria-sokolova-cn1:6667',
    kafka_topic_list = 'maria_sokolova',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow'


CREATE MATERIALIZED VIEW tst_kafka_from_queue_to_data TO tst_kafka_data 
  AS SELECT 
     toDateTime(timestamp/1000) as timestamp,
     referer,\
     location,\
     remoteHost,\
     partyId,\
     sessionId,\
     pageViewId,\
     eventType,\
     item_id,\
     item_price,\
     item_url,\
     basket_price,\
     detectedDuplicate,\
     detectedCorruption,\
     firstInSession,\
     userAgentName\

 FROM tst_kafka_queue



