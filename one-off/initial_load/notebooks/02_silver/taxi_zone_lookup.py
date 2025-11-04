# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/"
df = spark.read.format("csv").option("header", True).load(path)

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")