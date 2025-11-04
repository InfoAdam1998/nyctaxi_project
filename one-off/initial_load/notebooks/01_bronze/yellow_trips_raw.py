# Databricks notebook source
from pyspark.sql.functions import current_timestamp

path = "/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*"
df = spark.read.format("parquet").load(path)

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")