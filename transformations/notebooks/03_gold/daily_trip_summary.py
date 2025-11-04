# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round, col
from datetime import date
from dateutil.relativedelta import relativedelta

two_months_ago_start = date.today().replace(day=1) - relativedelta(month=2)

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}'")

# COMMAND ----------

df = (
    df.withColumn("pickup_date", col("tpep_pickup_datetime").cast("date"))
      .groupBy("pickup_date")
      .agg(
          count("*").alias("total_trips"),
          round(avg("passenger_count"), 1).alias("average_passengers"),
          round(avg("trip_distance"), 1).alias("average_distance"),
          round(avg("fare_amount"), 2).alias("average_fare_per_trip"),
          max("fare_amount").alias("max_fare"),
          min("fare_amount").alias("min_fare"),
          round(sum("total_amount"), 2).alias("total_revenue")
      )
)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")