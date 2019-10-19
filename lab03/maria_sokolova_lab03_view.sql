-- забэкапить результаты lab01
create table backup_tst_kafka_data \
(\
     timestamp DateTime,\
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
ENGINE = MergeTree()\
order by timestamp

insert into backup_tst_kafka_data select * from tst_kafka_data

-- материализовать вью с агрегатными результатами lab01
create table  lab01_agg
( ts_start DateTime,
  ts_end DateTime,
  revenue Integer,
  buyers Integer,
  visitors Integer,
  purchases Integer,
  aov Float32
)
engine=MergeTree()
order by ts_start

insert into lab01_agg select * from maria_sokolova_lab01_agg_hourly;

-- вью для lab03
create view maria_sokolova_lab03_view
as
SELECT 
    sum(revenue) AS revenue, 
    sum(purchases) AS purchases, 
    revenue/purchases AS aov
FROM 
(
select distinct
  ts_start
, ts_end
, revenue
, purchases
, aov
from
(
(select * from lab01_agg
 union all
 SELECT
toStartOfHour(timestamp) as ts_start
,ts_start + interval 1 hour - interval 1 second as ts_end
,toInt32(sumIf(item_price,eventType='itemBuyEvent')) as revenue
,toInt32(uniq(partyId)) AS visitors
,toInt32(uniqIf(partyId,eventType='itemBuyEvent')) as buyers
,toInt32(uniqIf(sessionId,eventType='itemBuyEvent')) as purchases
,toFloat32(revenue/purchases) as aov
FROM tst_kafka_data
where detectedDuplicate=0 and detectedCorruption=0
group by ts_start
order by ts_start
)
)
)

