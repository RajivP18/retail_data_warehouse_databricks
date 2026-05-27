# Databricks notebook source
# MAGIC %md
# MAGIC # CREATING THE DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_bronze;
# MAGIC CREATE DATABASE IF NOT EXISTS retail_silver;
# MAGIC CREATE DATABASE IF NOT EXISTS retail_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC # READING THE RAW DATA FROM WORKSPACE

# COMMAND ----------

df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("multiLine", "true").option("quote", '"').option("escape", '"').load("file:/Workspace/Users/rajivpillalamarri@gmail.com/Retail Data Warehouse/raw_retail.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # ADDING A METADATA COLUMN THAT RECORDS WHEN THE DATA WAS INGESTED INTO BRONZR LAYER AND CLEANING COLUM NAMES TO CREATE TABLE

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_raw = df_raw.withColumn(
    "ingestion_time",
    current_timestamp()
)

# COMMAND ----------

# Clean column names
clean_columns = [col.replace(" ", "_").lower() for col in df_raw.columns]

df_raw = df_raw.toDF(*clean_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING THE TABLE AND LOADING IT INTO RETAIL_RAW SCHEMA

# COMMAND ----------

df_raw.write.format("delta").mode("overwrite").saveAsTable("retail_bronze.orders_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING ANOTHER TABLE FROM API

# COMMAND ----------

import requests
import pandas as pd

url = "https://open.er-api.com/v6/latest/USD"

response = requests.get(url)
data = response.json()

rates_df = pd.DataFrame(
    data['rates'].items(),
    columns=['currency', 'rate']
)

spark_rates = spark.createDataFrame(rates_df)

spark_rates.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_bronze.currency_rates")

# COMMAND ----------

spark.table("retail_bronze.orders_raw").show(5)

# COMMAND ----------

spark.table("retail_bronze.orders_raw").printSchema()

# COMMAND ----------

