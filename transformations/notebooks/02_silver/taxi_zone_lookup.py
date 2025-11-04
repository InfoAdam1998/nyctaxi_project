# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime
from delta.tables import DeltaTable

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

end_timestamp = datetime.now()

# Load the SCD2 Delta Table
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

# Pass 1: Close any active row whos tracked attributes chaned.
dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date is NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)" 
    ).\
    whenMatchedUpdate(
        set = {"t.end_date": lit(end_timestamp).cast(TimestampType())}
    ).\
    execute()

# COMMAND ----------

# Pass 2: Insert new current versions

# get the lists of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}' ").select("location_id").collect()]

if len(insert_id_list) == 0:
    print("No new records to insert")
else:
    dt.alias("t").\
        merge(
            source = df.alias("s"),
            condition = f"s.location_id not in ({', '.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values = {
                "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone:": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType())
            }
        ).\
            execute()

# COMMAND ----------

# Pass 3: Insert brand-new keys (no historical row in target)
dt.alias("t") \
    .merge(
        source=df.alias("s"),
        condition="t.location_id = s.location_id"
    ) \
    .whenNotMatchedInsert(
        values={
            "location_id": "s.location_id",
            "borough": "s.borough",
            "zone": "s.zone",
            "service_zone": "s.service_zone",
            "effective_date": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
        }
    ) \
    .execute()
