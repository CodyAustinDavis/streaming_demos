# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE gold_airtempanalysis 
USING DELTA 
AS (

  SELECT *,
  avg(`SensorValue`) OVER (
          ORDER BY `MeasurementDateTime`
          ROWS BETWEEN
            30 PRECEDING AND
            CURRENT ROW
        ) AS TempShortMovingAverage,

  avg(`SensorValue`) OVER (
          ORDER BY `MeasurementDateTime`
          ROWS BETWEEN
            365 PRECEDING AND
            CURRENT ROW
        ) AS TempLongMovingAverage
FROM streamingdemos.silver_allsensors
WHERE SensorMeasurement = 'average_temperature'
)
""")

# COMMAND ----------

spark.sql("""SELECT *,
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          30 PRECEDING AND
          CURRENT ROW
      ) AS WaterTempShortMovingAverage,
      
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          365 PRECEDING AND
          CURRENT ROW
      ) AS WaterTempLongMovingAverage
FROM streamingdemos.silver_allsensors
WHERE SensorMeasurement = 'h2o_quality';
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE streamingdemos.gold_waterqualityanalysis
USING DELTA
AS (SELECT *,
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          30 PRECEDING AND
          CURRENT ROW
      ) AS WaterTempShortMovingAverage,
      
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          365 PRECEDING AND
          CURRENT ROW
      ) AS WaterTempLongMovingAverage
FROM streamingdemos.silver_allsensors
WHERE SensorMeasurement = 'h2o_quality'
)
""")
