# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid 
import os
import boto3

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS streamingdemos""")

# COMMAND ----------

##### Get Parameters for job

try:
  start_over = os.environ["START_OVER"]
except: 
  start_over = os.environ["START_OVER"] = "No"


try:
  bucket = os.environ["BUCKET"]
except:
  bucket = "codydemos"
## Set up source and checkpoints

file_source_location = f"s3://{bucket}/data_sources/"
checkpoint_location = f"s3://{bucket}/checkpoints/emr_checkpoints/raw_to_bronze/"
target_location = f"s3://{bucket}/emr/delta_tables/"

print("Now running Weather Data Streaming Service...")
print(f"...from source location {file_source_location}")
print(f"Start Over? : {start_over}")
print(f"Streaming tables to target location: {target_location}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema: Can be defined in separate directly and managed in Git for advanced schema evolution with Delta

# COMMAND ----------

weatherInputSensorSchema = StructType([StructField("Skip", StringType(), True),
                                      StructField("SkipResult", StringType(), True),
                                      StructField("SkipTable", StringType(), True),
                                      StructField("WindowAverageStartDateTime", TimestampType(), True),
                                      StructField("WindowAverageStopDateTime", TimestampType(), True),
                                      StructField("MeasurementDateTime", TimestampType(), True),
                                      StructField("SensorValue", DecimalType(), True),
                                      StructField("SensorUnitDescription", StringType(), True),
                                      StructField("SensorMeasurement", StringType(), True),
                                      StructField("SensorLocation", StringType(), True),
                                      StructField("Id", StringType(), True)]
                                     )

# COMMAND ----------

# DBTITLE 1,Read Stream
df_raw = (spark
     .readStream
     .format("csv")
     .schema(weatherInputSensorSchema)
     .load(file_source_location)
     .withColumn("Id", uuidUdf())
     .withColumn("InputFileName", input_file_name())
    )

# COMMAND ----------

##### Do ETL/Modelling as needed for products

##### In this example we read 1 source of sensor data, and create a model of 5 delta tables that drive an analytics dashboard
df_cleaned = (df_raw
              .filter((col("WindowAverageStartDateTime").isNotNull()) & (col("SensorValue").isNotNull())) 
              .drop("Skip", "SkipResult", "SkipTable", "WindowAverageStartDateTime", "WindowAverageStopDateTime")
             )

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def ModelSourceSensorData(microBatchDf, BatchId):
  
  #### If we want to make incremental, then we can insert merge statements here!!
  #print(BatchId)
  
  df_airTemp = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("average_temperature"))
                ## Do any ETL
                  )
  
  df_waterQuality = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_quality"))
                  )
  
  df_waterPH = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_pH"))
                  )
  
  df_waterTemp = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_temperature"))
                  )
  
  df_waterDepth = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_depth"))
                  )
  
  ### Apply schemas
  
  ## Look up schema registry, check to see if the events in each subtype are equal to the most recently registered schema, Register new schema
  
  
  ##### Write to sink location
  microBatchDf.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("path", f"{target_location}bronze_allsensors/").mode("overwrite").saveAsTable("streamingdemos.Bronze_AllSensors")
  df_waterDepth.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("path", f"{target_location}bronze_waterdepthsensor").mode("overwrite").saveAsTable("streamingdemos.Bronze_WaterDepthSensor")
  df_airTemp.write.format("delta").mode("overwrite").option("path", f"{target_location}bronze_airtempsensor").saveAsTable("streamingdemos.Bronze_AverageAirTemperatureSensor")
  df_waterQuality.write.format("delta").mode("overwrite").option("path", f"{target_location}bronze_waterqualitysensor").saveAsTable("streamingdemos.Bronze_WaterQualitySensor")
  df_waterPH.write.format("delta").mode("overwrite").option("path", f"{target_location}bronze_waterphsensor").saveAsTable("streamingdemos.Bronze_WaterPhSensor")
  df_waterTemp.write.format("delta").mode("overwrite").option("path",f"{target_location}bronze_watertemperaturesensor").saveAsTable("streamingdemos.Bronze_WaterTemperatureSensor")
  
  return

# COMMAND ----------

if start_over.lower() == "yes":
    
  s3 = boto3.resource('s3')
  my_bucket = s3.Bucket(bucket)
  my_bucket.objects.filter(Prefix="checkpoints/emr_checkpoints/raw_to_bronze/").delete()

# COMMAND ----------

# DBTITLE 1,Write Stream
#### Actually execute stream or file run with same logic!

  ### Using For each batch - microBatchMode
(df_cleaned
    .writeStream
    .trigger(once=True) #1. Continous - 5 min, ProcessingTime='1 second', availableNow=True
    .option("checkpointLocation", checkpoint_location)
    .foreachBatch(ModelSourceSensorData)
    .start()
)
