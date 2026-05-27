# Databricks notebook source
# MAGIC %md
# MAGIC # LOADING THE DATA BROM BRONZE LAYER

# COMMAND ----------

bronze_df = spark.table("retail_bronze.orders_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEAING THE DATA (DROPPING NULLS AND DUPLICATES)

# COMMAND ----------

from pyspark.sql.functions import *

silver_df = bronze_df.dropna()

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # CONVERTING DATA TYPES

# COMMAND ----------

from pyspark.sql.functions import col

bronze_df = spark.table("retail_bronze.orders_raw")

silver_df = bronze_df \
    .withColumn("sales", col("sales").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("discount", col("discount").cast("double")) \
    .withColumn("profit", col("profit").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC # STANDAEDIZE COLUMN NAMES

# COMMAND ----------

silver_df = silver_df.withColumnRenamed(
    "Customer ID",
    "customer_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SAVING THE DATA TO SILVER LAYER

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("retail_silver.orders_clean")

# COMMAND ----------

silver_df.display()