-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC df_benchmark_tables = spark.sql("SHOW TABLES IN main.streamingdemos").filter(col("tableName").like("benchmark%")).collect()
-- MAGIC
-- MAGIC print("Dropping Benchmarking Tables...")
-- MAGIC
-- MAGIC for i in df_benchmark_tables:
-- MAGIC   tbl = str(i[0]) + "." + str(i[1])
-- MAGIC   print(tbl)
-- MAGIC   spark.sql(f"DROP TABLE IF EXISTS main.{tbl}")

-- COMMAND ----------

-- DBTITLE 1,Spark - Intel

CLEAR CACHE;
USE CATALOG main;

CREATE OR REPLACE TABLE streamingdemos.benchmark_spark_intel_water_quality_ma_live
AS
SELECT *,
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
FROM streamingdemos.silver_waterqualitysensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_spark_intel_air_temp_ma_live
AS
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
FROM streamingdemos.silver_airtempsensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_spark_intel_gold_airtempanalysis 
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
  FROM streamingdemos.silver_airtempsensor
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_spark_intel_gold_airtempanalysis 
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
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_spark_intel_gold_waterqualityanalysis
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
);


CREATE OR REPLACE TABLE main.streamingdemos.benchmark_spark_intel_gold_aggregates
AS 
(
WITH r1_aggs AS (
SELECt s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime) AS MDate, MIN(s.SensorValue) AS Lowest, MAX(s.SensorValue) AS Highest, AVG(s.SensorValue) AS Midpoint,
COUNT(0) AS RecordsInDay,
COUNT(DISTINCT CASE WHEN wd.Id IS NOT NULL THEN wd.Id END) AS CountOfVerifiedWaterDepthRecords,
COUNT(DISTINCT CASE WHEN wq.Id IS NOT NULL THEN wq.Id END) AS CountOfVerifiedWaterQualityRecords,
COUNT(DISTINCT CASE WHEN wt.Id IS NOT NULL THEN wt.Id END) AS CountOfVerifiedWaterTempRecords,
COUNT(DISTINCT CASE WHEN att.Id IS NOT NULL THEN att.Id END) AS CountOfVerifiedAirTempRecords
 FROM main.streamingdemos.silver_allsensors s
 LEFT JOIN main.streamingdemos.silver_waterdepthsensor wd ON wd.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_feet'
 LEFT JOIN main.streamingdemos.silver_waterqualitysensor wq ON wq.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_quality'
 LEFT JOIN main.streamingdemos.silver_watertempsensor wt ON wt.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_temperature'
 LEFT JOIN main.streamingdemos.silver_airtempsensor att ON att.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'average_temperature'
 GROUP BY s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime)
 ORDER BY s.SensorLocation, s.SensorMeasurement, MDate
),
step2 AS (
SELECT spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,
CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords,
MIN(Lowest) AS Lowest,
AVG(Midpoint) AS Midpoint,
MAX(Highest) AS Highest,
CASE WHEN MAX(f.ValueType) IS NOT NULL THEN 1 ELSE 0 END AS HasForecast,
AVG(f.SensorValue) AS AvgDailyForecast 
FROM r1_aggs as spine
LEFT JOIN main.streamingdemos.water_quality_forecast f ON date_trunc('day', f.MeasurementDateTime) = spine.MDate AND spine.SensorMeasurement = 'h2o_quality'
GROUP BY spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords
)
SELECt DISTINCT * FROM step2
)

-- COMMAND ----------

-- DBTITLE 1,Photon - Intel

CLEAR CACHE;
USE CATALOG main;

CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_intel_water_quality_ma_live
AS
SELECT *,
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
FROM streamingdemos.silver_waterqualitysensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_intel_air_temp_ma_live
AS
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
FROM streamingdemos.silver_airtempsensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_intel_gold_airtempanalysis 
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
  FROM streamingdemos.silver_airtempsensor
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_intel_gold_airtempanalysis 
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
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_intel_gold_waterqualityanalysis
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
);


CREATE OR REPLACE TABLE main.streamingdemos.benchmark_photon_intel_gold_aggregates
AS 
(
WITH r1_aggs AS (
SELECt s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime) AS MDate, MIN(s.SensorValue) AS Lowest, MAX(s.SensorValue) AS Highest, AVG(s.SensorValue) AS Midpoint,
COUNT(0) AS RecordsInDay,
COUNT(DISTINCT CASE WHEN wd.Id IS NOT NULL THEN wd.Id END) AS CountOfVerifiedWaterDepthRecords,
COUNT(DISTINCT CASE WHEN wq.Id IS NOT NULL THEN wq.Id END) AS CountOfVerifiedWaterQualityRecords,
COUNT(DISTINCT CASE WHEN wt.Id IS NOT NULL THEN wt.Id END) AS CountOfVerifiedWaterTempRecords,
COUNT(DISTINCT CASE WHEN att.Id IS NOT NULL THEN att.Id END) AS CountOfVerifiedAirTempRecords
 FROM main.streamingdemos.silver_allsensors s
 LEFT JOIN main.streamingdemos.silver_waterdepthsensor wd ON wd.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_feet'
 LEFT JOIN main.streamingdemos.silver_waterqualitysensor wq ON wq.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_quality'
 LEFT JOIN main.streamingdemos.silver_watertempsensor wt ON wt.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_temperature'
 LEFT JOIN main.streamingdemos.silver_airtempsensor att ON att.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'average_temperature'
 GROUP BY s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime)
 ORDER BY s.SensorLocation, s.SensorMeasurement, MDate
),
step2 AS (
SELECT spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,
CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords,
MIN(Lowest) AS Lowest,
AVG(Midpoint) AS Midpoint,
MAX(Highest) AS Highest,
CASE WHEN MAX(f.ValueType) IS NOT NULL THEN 1 ELSE 0 END AS HasForecast,
AVG(f.SensorValue) AS AvgDailyForecast 
FROM r1_aggs as spine
LEFT JOIN main.streamingdemos.water_quality_forecast f ON date_trunc('day', f.MeasurementDateTime) = spine.MDate AND spine.SensorMeasurement = 'h2o_quality'
GROUP BY spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords
)
SELECt DISTINCT * FROM step2
)


-- COMMAND ----------

-- DBTITLE 1,Photon - Graviton

CLEAR CACHE;
USE CATALOG main;

CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_graviton_water_quality_ma_live
AS
SELECT *,
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
FROM streamingdemos.silver_waterqualitysensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_graviton_air_temp_ma_live
AS
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
FROM streamingdemos.silver_airtempsensor;


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_graviton_gold_airtempanalysis 
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
  FROM streamingdemos.silver_airtempsensor
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_graviton_gold_airtempanalysis 
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
);


CREATE OR REPLACE TABLE streamingdemos.benchmark_photon_graviton_gold_waterqualityanalysis
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
);


CREATE OR REPLACE TABLE main.streamingdemos.benchmark_photon_graviton_gold_aggregates
AS 
(
WITH r1_aggs AS (
SELECt s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime) AS MDate, MIN(s.SensorValue) AS Lowest, MAX(s.SensorValue) AS Highest, AVG(s.SensorValue) AS Midpoint,
COUNT(0) AS RecordsInDay,
COUNT(DISTINCT CASE WHEN wd.Id IS NOT NULL THEN wd.Id END) AS CountOfVerifiedWaterDepthRecords,
COUNT(DISTINCT CASE WHEN wq.Id IS NOT NULL THEN wq.Id END) AS CountOfVerifiedWaterQualityRecords,
COUNT(DISTINCT CASE WHEN wt.Id IS NOT NULL THEN wt.Id END) AS CountOfVerifiedWaterTempRecords,
COUNT(DISTINCT CASE WHEN att.Id IS NOT NULL THEN att.Id END) AS CountOfVerifiedAirTempRecords
 FROM main.streamingdemos.silver_allsensors s
 LEFT JOIN main.streamingdemos.silver_waterdepthsensor wd ON wd.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_feet'
 LEFT JOIN main.streamingdemos.silver_waterqualitysensor wq ON wq.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_quality'
 LEFT JOIN main.streamingdemos.silver_watertempsensor wt ON wt.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'h2o_temperature'
 LEFT JOIN main.streamingdemos.silver_airtempsensor att ON att.MeasurementDateTime = s.MeasurementDateTime AND s.SensorMeasurement = 'average_temperature'
 GROUP BY s.SensorLocation, s.SensorMeasurement, date_trunc('day', s.MeasurementDateTime)
 ORDER BY s.SensorLocation, s.SensorMeasurement, MDate
),
step2 AS (
SELECT spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,
CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords,
MIN(Lowest) AS Lowest,
AVG(Midpoint) AS Midpoint,
MAX(Highest) AS Highest,
CASE WHEN MAX(f.ValueType) IS NOT NULL THEN 1 ELSE 0 END AS HasForecast,
AVG(f.SensorValue) AS AvgDailyForecast 
FROM r1_aggs as spine
LEFT JOIN main.streamingdemos.water_quality_forecast f ON date_trunc('day', f.MeasurementDateTime) = spine.MDate AND spine.SensorMeasurement = 'h2o_quality'
GROUP BY spine.SensorLocation, spine.MDate, spine.SensorMeasurement, spine.RecordsInDay,CountOfVerifiedWaterDepthRecords,
CountOfVerifiedWaterQualityRecords,
CountOfVerifiedWaterTempRecords,
CountOfVerifiedAirTempRecords
)
SELECt DISTINCT * FROM step2
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Benchmarks on HMS: 
-- MAGIC
-- MAGIC 1. Spark - 1.67 min, Photon/Intel - 1.27 min, Photon/Graviton - 1.20 min
-- MAGIC
-- MAGIC
-- MAGIC ## Benchmarks on UC: 
-- MAGIC
-- MAGIC 1. Spark - 32.61 seconds, Photon/Intel - 16.15 seconds
