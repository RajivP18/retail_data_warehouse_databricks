# Databricks notebook source
# MAGIC %md
# MAGIC # READING SILVER DATA

# COMMAND ----------

silver_df = spark.table("retail_silver.orders_clean")

silver_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE CUSTOMER DIMENSION

# COMMAND ----------

dim_customer = silver_df.select(
    "customer_id",
    "customer_name",
    "segment",
    "country",
    "city",
    "state",
    "postal_code",
    "region"
).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE PRODUCT DIMENSION

# COMMAND ----------

dim_product = silver_df.select(
    "product_id",
    "category",
    "sub_category",
    "product_name"
).dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DATE DIMENSION

# COMMAND ----------

from pyspark.sql.functions import *

dim_date = silver_df.select(
    "order_date"
).distinct()

dim_date = dim_date.withColumn(
    "year", year(col("order_date"))
).withColumn(
    "month", month(col("order_date"))
).withColumn(
    "quarter", quarter(col("order_date"))
).withColumn(
    "day", dayofmonth(col("order_date"))
).withColumn(
    "week_of_year", weekofyear(col("order_date"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING FACT TABLE

# COMMAND ----------

fact_sales = silver_df.select(
    "row_id",
    "order_id",
    "order_date",
    "ship_date",
    "customer_id",
    "product_id",
    "sales",
    "quantity",
    "discount",
    "profit"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SAVE GOLD TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC 01 CUSTOMER TABLE

# COMMAND ----------

dim_customer.write.format("delta").mode("overwrite").saveAsTable("retail_gold.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC 02 PRODUCT TABLE

# COMMAND ----------

from pyspark.sql.functions import col

dim_product = silver_df.select(
    "product_id",
    "category",
    col("`sub-category`").alias("sub_category"),
    "product_name"
).dropDuplicates()

dim_product.write.format("delta").mode("overwrite").saveAsTable("retail_gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC 03 DATE TABLE

# COMMAND ----------

dim_date.write.format("delta").mode("overwrite").saveAsTable("retail_gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 04 FACT TABLE

# COMMAND ----------

fact_sales.write.format("delta").mode("overwrite").saveAsTable("retail_gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC # VERIFYING GOLD TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

spark.table("retail_gold.fact_sales").show(5)

spark.table("retail_gold.dim_customer").show(5)

spark.table("retail_gold.dim_product").show(5)  