# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h1> Silver to Gold: Analytics of IoT stream tables </h1>
# MAGIC
# MAGIC This notebook uses any preferred analyst language to quickly get insights from the tables Data Engineering just streamed in!
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS streamingdemos;
# MAGIC USE streamingdemos;

# COMMAND ----------

# MAGIC %md 
# MAGIC <b> Analyze Air Temp Trends </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS streamingdemos.air_temp_ma_live
# MAGIC AS
# MAGIC SELECT *,
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           30 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS TempShortMovingAverage,
# MAGIC       
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           365 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS TempLongMovingAverage
# MAGIC FROM streamingdemos.silver_airtempsensor;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABle  streamingdemos.gold_airtempanalysis 
# MAGIC USING DELTA 
# MAGIC AS (
# MAGIC
# MAGIC   SELECT *,
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             30 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS TempShortMovingAverage,
# MAGIC
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             365 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS TempLongMovingAverage
# MAGIC   FROM streamingdemos.silver_airtempsensor
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Check View Directly in notebook
# MAGIC %sql 
# MAGIC
# MAGIC SELECT *,
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           30 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS WaterTempShortMovingAverage,
# MAGIC       
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           365 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS WaterTempLongMovingAverage
# MAGIC FROM streamingdemos.silver_waterqualitysensor;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE streamingdemos.gold_waterqualityanalysis
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT *,
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             30 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS WaterTempShortMovingAverage,
# MAGIC
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             365 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS WaterTempLongMovingAverage
# MAGIC   FROM streamingdemos.silver_waterqualitysensor
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW streamingdemos.water_quality_forecast
# MAGIC AS (
# MAGIC WITH water_forecast AS (
# MAGIC SELECt ds AS MeasurementDateTime, 'forecast' AS ValueType,  yhat AS SensorValue
# MAGIC FROM streamingdemos.silver_hourly_forecast
# MAGIC WHERE SensorMeasurement = 'h2o_quality'
# MAGIC ),
# MAGIC combined_model AS (
# MAGIC SELECT MeasurementDateTime, 'actuals' AS ValueType, WaterTempShortMovingAverage AS SensorValue FROm streamingdemos.gold_waterqualityanalysis
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC SELECT MeasurementDateTime, ValueType, SensorValue FROM water_forecast
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM combined_model
# MAGIC WHERE SensorValue >= 0 ORDER BY MeasurementDateTime DESC 
# MAGIC LIMIT 1000
# MAGIC )
