# Databricks notebook source
# MAGIC %md
# MAGIC # TOTAL REVENUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(sales), 2) AS total_revenue
# MAGIC FROM retail_gold.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC # PROFIT BY REGION

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.region,
# MAGIC     ROUND(SUM(f.profit), 2) AS total_profit
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_customer c
# MAGIC ON f.customer_id = c.customer_id
# MAGIC GROUP BY c.region
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # TOP PRODUCT CATEGORIES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.region,
# MAGIC     ROUND(SUM(f.profit), 2) AS total_profit
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_customer c
# MAGIC ON f.customer_id = c.customer_id
# MAGIC GROUP BY c.region
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # MONTHLY REVENUE TREND

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     ROUND(SUM(f.sales), 2) AS monthly_revenue
# MAGIC FROM gold.fact_sales f
# MAGIC JOIN gold.dim_date d
# MAGIC ON f.order_date = d.order_date
# MAGIC GROUP BY d.year, d.month
# MAGIC ORDER BY d.year, d.month;