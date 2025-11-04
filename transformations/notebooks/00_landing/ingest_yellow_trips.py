# Databricks notebook source
import urllib.request 
import shutil
import os
from datetime import datetime, date, timezone
from dateutil.relativedelta import relativedelta

two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try:
    # Check if the files already exists
    dbutils.fs.ls(dir_path)

    # If the file already exist, then set continue downstream to no
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        response = urllib.request.urlopen(url)

        os.makedirs(dir_path, exist_ok=True)

        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)

        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File downloaded successfully")
    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File downloaded failed {str(e)}")