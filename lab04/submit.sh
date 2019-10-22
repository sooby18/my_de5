#!/bin/bash
export HDP_VERSION="current"

PYSPARK_PYTHON=python3 spark-submit \
    --conf spark.streaming.batch.duration=5 --conf spark.ui.port=4041\
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 \
    task.py
