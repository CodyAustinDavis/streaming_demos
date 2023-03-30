# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.table("streamingdemos.silver_allsensors")

# COMMAND ----------

# DBTITLE 1,Run Predictions on Silver Table
import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/60926433e49b4f20892b4ca5240688ff/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')


# Predict on a Spark DataFrame.
df_final = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))

# COMMAND ----------

# DBTITLE 1,Write Predictions
df_final.write.saveAsTable("streamingdemos.sensor_predictions")
