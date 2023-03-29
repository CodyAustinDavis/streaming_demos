# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf
from delta.tables import *
import os
import boto

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS streamingdemos;""")
spark.sql("""USE streamingdemos;""")

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
checkpoint_location = f"s3://{bucket}/checkpoints/emr_checkpoints/bronze_to_silver/"
target_location = f"s3://{bucket}/emr/delta_tables/"

print("Now running Weather Data Streaming Service...")
print(f"...from source location {file_source_location}")
print(f"Start Over? : {start_over}")
print(f"Streaming tables to target location: {target_location}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

##### Read file: Same output for either stream or static mode

### Stream # 5 for All Sensors

file_source_location_all_sensors = file_source_location + "bronze_allsensors/"
file_sink_location_all_sensors = file_source_location +"silver_allsensors/"
checkpoint_location_all_sensors = checkpoint_location + "AllSensors/"

stream_df = (spark
    .readStream
    .format("delta")
    .load(file_source_location_all_sensors)
  )

# COMMAND ----------

if start_over.lower() == "yes":
    
  print("Truncating bronze--> silver checkpoint for all sensors table.")
  s3 = boto3.resource('s3')
  my_bucket = s3.Bucket(bucket)
  my_bucket.objects.filter(Prefix="checkpoints/emr_checkpoints/bronze_to_silver/").delete()

# COMMAND ----------

### Define silver table schema

silverAllSensorSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                    StructField("SensorValue", DecimalType(), True),
                                    StructField("SensorUnitDescription", StringType(), True),
                                    StructField("SensorMeasurement", StringType(), True),
                                    StructField("SensorLocation", StringType(), True),
                                    StructField("Id", StringType(), False)]
                                     )


### create silver table if not exists

isAllSensorsTempThere = DeltaTable.isDeltaTable(spark, file_sink_location_all_sensors)
print(f"Silver Table for All Sensors Exists: {isAllSensorsTempThere}")

if isAllSensorsTempThere == False:
  emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), silverAllSensorSchema)
  (emptyDF
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("SensorMeasurement")
    .option("path", file_sink_location_all_sensors)
    .saveAsTable("streamingdemos.silver_allsensors")
  )
  
  print("Created Empty Silver All Sensor Table for All Sensors Table!")

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def streamController(microBatchDf, BatchId):
  
  silverDeltaTable = DeltaTable.forPath(spark, file_sink_location_all_sensors)
  
  
  (silverDeltaTable.alias("t")
  .merge(
    microBatchDf.alias("s"),
    "t.Id = s.Id"
        )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
  )
  
  
  ##
  return

# COMMAND ----------

#### Actually execute stream or file run with same logic!

(stream_df
  .writeStream
  .trigger(once=True)
  .option("checkpointLocation", checkpoint_location_all_sensors)
  .foreachBatch(streamController)
  .start()
)

# COMMAND ----------

spark.sql("""OPTIMIZE streamingdemos.silver_allsensors
ZORDER BY (MeasurementDateTime)
""")
