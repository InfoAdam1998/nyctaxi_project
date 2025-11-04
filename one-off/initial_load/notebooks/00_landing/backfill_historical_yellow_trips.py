# Databricks notebook source
import urllib.request 
import shutil
import os

# COMMAND ----------

dates_to_process = ["2024-12", "2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]

for date in dates_to_process:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"

    response = urllib.request.urlopen(url)

    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}"

    os.makedirs(dir_path, exist_ok=True)

    local_path = f"{dir_path}/yellow_tripdata_{date}.parquet"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)