#!/bin/bash

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  --conf spark.driver.host=127.0.0.1 \
  --jars ./jars/postgresql-42.7.5.jar \
  stream-processing/flight_stream.py
