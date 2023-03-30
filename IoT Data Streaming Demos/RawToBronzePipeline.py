# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline </h1>
# MAGIC 
# MAGIC <h2> Structured Streaming With Sensor Data </h2>
# MAGIC 
# MAGIC <li> Structured Streaming can use the same code, whether streaming or performing ad-hoc analysis. </li>
# MAGIC <li> Read a table, perform modelling, report on data in real time. </li>
# MAGIC <li> Debug and Develop with same code. </li>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Current Stage: Raw --> Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid 

# COMMAND ----------


spark.sql("""CREATE DATABASE IF NOT EXISTS streamingdemos""")
spark.sql("""USE streamingdemos""")

# COMMAND ----------

##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

dbutils.widgets.dropdown("Start Over", "No", ["Yes", "No"])
start_over = dbutils.widgets.get("Start Over")

## Set up source and checkpoints
file_source_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemo/" #s3://codyaustindavisdemos/Demo/sales/"
checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/RawToBronze/"

print("Now running Weather Data Streaming Service...")
print(f"...from source location {file_source_location}")
print(f"Run Mode: {runMode}")
print(f"Start Over? : {start_over}")

if runMode == "Static":
  print(f"Running file: {fileName}")

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

# MAGIC %md
# MAGIC 
# MAGIC ##### Read file: Same output for either stream or static mode

# COMMAND ----------

##### Read file: Same output for either stream or static mode

if runMode == "Static":
  
  df_raw = (spark
         .read
         .option("header", "true")
         .option("inferSchema", "true")
         .schema(weatherInputSensorSchema) #infer
         .format("csv")
         .load(file_source_location + fileName)
         .withColumn("Id", uuidUdf())
        )
  
elif runMode == "Stream":
  
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

#display(df_cleaned)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Stream one source to multiple sinks!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://image.slidesharecdn.com/tathagatadas-191104200312/95/designing-etl-pipelines-with-structured-streaming-and-delta-lakehow-to-architect-things-right-40-638.jpg?cb=1572897862" >

# COMMAND ----------

# DBTITLE 1,Create a global temp view to be used later in the job
df_raw_sensors = (spark
       .read
       .option("header", "true")
       .option("inferSchema", "true")
       .schema(weatherInputSensorSchema) #infer
       .format("csv")
       .load(file_source_location + fileName)
       .withColumn("Id", uuidUdf())
       .filter((col("WindowAverageStartDateTime").isNotNull()) & (col("SensorValue").isNotNull())) 
       .drop("Skip", "SkipResult", "SkipTable", "WindowAverageStartDateTime", "WindowAverageStopDateTime")
       .select("SensorMeasurement")
       .distinct()
      )
  
df_raw_sensors.createOrReplaceGlobalTempView("unique_sensors")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Perform Advacned ETL Logic inside the forEachBatch function

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
  microBatchDf.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("path", "/data/streamingdemos/bronze_allsensors").mode("overwrite").saveAsTable("streamingdemos.bronze_allsensors")
  df_waterDepth.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("path", "/data/streamingdemos/bronze_waterdepthsensor").mode("overwrite").saveAsTable("streamingdemos.Bronze_WaterDepthSensor")
  df_airTemp.write.format("delta").mode("overwrite").option("path", "/data/streamingdemos/bronze_airtempsensor").saveAsTable("streamingdemos.Bronze_AverageAirTemperatureSensor")
  df_waterQuality.write.format("delta").mode("overwrite").option("path", "/data/streamingdemos/bronze_waterqualitysensor").saveAsTable("streamingdemos.Bronze_WaterQualitySensor")
  df_waterPH.write.format("delta").mode("overwrite").option("path", "/data/streamingdemos/bronze_waterphsensor").saveAsTable("streamingdemos.Bronze_WaterPhSensor")
  df_waterTemp.write.format("delta").mode("overwrite").option("path","/data/streamingdemos/bronze_watertemperaturesensor").saveAsTable("streamingdemos.Bronze_WaterTemperatureSensor")
  
  return

# COMMAND ----------

if start_over == "Yes":
  dbutils.fs.rm(checkpoint_location, recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_allsensors", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_waterdepthsensor", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_airtempsensor", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_waterqualitysensor", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_waterphsensor", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_watertemperaturesensor", recurse=True)
  dbutils.fs.rm("/data/streamingdemos/bronze_fullstreamfromkafka", recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream
#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  ModelSourceSensorData(df_cleaned, 1)
  
elif runMode == "Stream":
  ### Using For each batch - microBatchMode
   (df_cleaned
     .writeStream
     .trigger(once=True) #1. Continous - 5 min, ProcessingTime='1 second', availableNow=True
     .option("checkpointLocation", checkpoint_location)
     .foreachBatch(ModelSourceSensorData)
     .start()
   )

# COMMAND ----------

# DBTITLE 1,Standard Streaming
"""

df_raw = (spark
   .readStream
   .format("csv")
   .option("header", "true")
   .schema(weatherInputSensorSchema)
   .load(file_source_location)
   .withColumn("Id", uuidUdf())
   .withColumn("InputFileName", input_file_name())
  )

(df_raw
 .writeStream
.trigger(once=True) #processingTime = '1m', continuous='30 seconds'
.option("checkpointLocation", checkpoint_location)
.option("path", "/data/codydemos/bronze_fullstreamfromkafka")
.table("streamingdemos.BronzeFullStreamFromKafka")
)
"""
