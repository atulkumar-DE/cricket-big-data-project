# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importing Libraries
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Read the bronze Table
bronze_df=spark.table('workspace.default.cricket_bronze_current_matches')

raw_json=bronze_df.select('raw_json').collect()[0]['raw_json']
api_data=json.loads(raw_json)

matches=api_data.get("data",[])

print("Total Matches found ", len(matches))
print(matches[0] if len(matches)>0 else "No Matches Found")

# COMMAND ----------

# DBTITLE 1,Extract only the useful fields
silver_rows=[]

for match in matches:
    # Fixed: removed trailing comma to make teams a list, not a tuple
    teams=match.get("teams",[])
    score=match.get("score",[])

    # Fixed: check len(teams) instead of len(score)
    team_1=teams[0] if len(teams)>0 else None
    team_2=teams[1] if len(teams)>1 else None

    score_1=None
    score_2=None

    if len(score)>0:
        s1=score[0]
        #Formating in it real score display examp 180/5 in 20 overs
        score_1=f"{s1.get('r',0)}/{s1.get('w',0)} in {s1.get('o',0)} overs"
    if len(score)>1:
        s2=score[1]
        score_2=f"{s2.get('r',0)}/{s2.get('w',0)} in {s2.get('o',0)} overs"

    silver_rows.append(
        {
            "match_id":match.get("id"),  # Fixed: 'id' not 'match_id'
            "match_name":match.get("name"),  # Fixed: 'name' not 'match_name'
            "match_type":match.get("matchType"),
            "status":match.get("status"),
            "venue":match.get("venue"),
            "match_date":match.get("startDate"),  # Fixed: 'startDate' not 'date'
            "date_time_gmt":match.get("dateTimeGMT"),
            "team_1":team_1,
            "team_2":team_2,
            "score_1":score_1,
            "score_2":score_2,
            "match_started":match.get("dateTimeGMT"),
            "match_ended":match.get("dateTimeGMT")
        }
    )
print("Silver rows Prepaired: ", len(silver_rows))


# COMMAND ----------

# DBTITLE 1,Create a structured Schema for Silver rows
silver_schema=StructType([
    StructField("match_id", StringType(), True),
    StructField("match_name", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("date_time_gmt", StringType(), True),
    StructField("team_1", StringType(), True),
    StructField("team_2", StringType(), True),
    StructField("score_1", StringType(), True),
    StructField("score_2", StringType(), True),
    StructField("match_started", StringType(), True),
    StructField("match_ended", StringType(), True)
])
silver_df=spark.createDataFrame(silver_rows,silver_schema)\
    .withColumn("match_date",expr("try_to_date(match_date, 'yyyy-MM-dd')"))\
        .withColumn("loaded_at",current_timestamp())

display(silver_df)


# COMMAND ----------

silver_df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable("workspace.default.cricket_silver_current_matches") 

print("Silver Table created successfully")   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cricket_silver_current_matches