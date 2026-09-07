# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import the important Library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Create Catalog, Scheam and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path='/Volumes/workspace/default/cricket_api_project'


# COMMAND ----------

# DBTITLE 1,Calling Cricket API
API_KEY='8617b221-27d1-43a0-82f2-3708fe9bfddd'
api_url=f"https://api.cricapi.com/v1/series?apikey={API_KEY}&offset=0"

response= requests.get(api_url)
response.raise_for_status()

api_data= response.json()
print(api_data.keys())

print(json.dumps(api_data,indent=2)[:2000])



# COMMAND ----------

# DBTITLE 1,Save RAW API Response in the Volume
raw_file_path=f'{base_path}/current_matches_raw.json'

with open(raw_file_path,'w') as file:
    json.dump(api_data,file)
print("RAW API data is save at the :", raw_file_path)

# COMMAND ----------

# DBTITLE 1,CREATE Broze Layer Dataframe or Table
bronze_data=[{
    "source_api": api_url,
    "raw_json": json.dumps(api_data),
    "ingestion_time": None
}]

bronze_schema=StructType([
    StructField("source_api",StringType()),
    StructField("raw_json",StringType()),
    StructField("ingestion_time",TimestampType())
])

bronze_df=spark.createDataFrame(bronze_data,bronze_schema) \
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)



# COMMAND ----------

# DBTITLE 1,SAVE the Bronze Table
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")
print("Bronze Table is created sucessfully")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cricket_bronze_current_matches