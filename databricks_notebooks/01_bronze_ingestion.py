# Databricks Notebook source
# COMMAND ----------
# 01_bronze_ingestion.py
# Ingesta en tiempo real desde S3 a Delta Lake (Capa Bronze) con Auto Loader

# COMMAND ----------
# 1. Configuracion de parametros y rutas
# (En produccion Databricks, las credenciales se manejan mediante Secrets Scope o IAM Instance Profile)

s3_bucket = "ecommerce-streaming-jordi-2026"
source_path = f"s3a://{s3_bucket}/raw_landing/ecommerce_stream/*/*/*/*/*.json"
checkpoint_path = f"s3a://{s3_bucket}/checkpoints/bronze_ecommerce"
bronze_delta_path = f"s3a://{s3_bucket}/delta/bronze_ecommerce_events"

# COMMAND ----------
# 2. Lectura en Streaming usando Databricks Auto Loader (cloudFiles)
# Auto Loader infiere y evoluciona el esquema de forma automatica y eficiente.

df_stream_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
)

# COMMAND ----------
# 3. Anadir metadatos de auditoria de ingesta
from pyspark.sql.functions import current_timestamp, input_file_name

df_bronze = (
    df_stream_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------
# 4. Escritura en Streaming hacia Delta Lake Bronze Table
query = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True) # Trigger para procesamiento eficiente de micro-lotes
    .start(bronze_delta_path)
)

query.awaitTermination()

# COMMAND ----------
# 5. Verificacion de registros en la tabla Bronze
display(spark.read.format("delta").load(bronze_delta_path))