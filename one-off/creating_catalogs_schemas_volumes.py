# Databricks notebook source
# Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS nyctaxi MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragehdtmmarygv33e.dfs.core.windows.net/1788087520141883'")

# COMMAND ----------

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold")

# COMMAND ----------

# Create volume
spark.sql("CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources")