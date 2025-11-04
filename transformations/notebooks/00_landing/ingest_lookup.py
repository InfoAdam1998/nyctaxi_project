# Databricks notebook source
import urllib.request 
import shutil
import os

try:
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    response = urllib.request.urlopen(url)

    dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"

    os.makedirs(dir_path, exist_ok=True)

    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uploaded")
except Exceptio as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File downloaded failed {str(e)}")