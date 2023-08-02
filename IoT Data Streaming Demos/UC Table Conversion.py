# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Converts all tables in this demo from hive to UC tables

# COMMAND ----------


from pyspark.sql.functions import *

df_benchmark_tables = spark.sql("SHOW TABLES IN hive_metastore.streamingdemos").collect()

spark.sql("CREATE DATABASE IF NOT EXISTS main.streamingdemos")

print("Migrating Tables...")

for i in df_benchmark_tables:
  try:
    tbl = str(i[0]) + "." + str(i[1])
    print(tbl)
    spark.sql(f"CREATE OR REPLACE TABLE main.{tbl} AS SELECt * FROM hive_metastore.{tbl}")
  except Exception as e:
    print("Table not a view... moving on" + str(e))
