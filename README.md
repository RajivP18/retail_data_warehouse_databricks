# Retail Data Warehouse using Medallion Architecture in Databricks

## Project Overview

This project demonstrates the implementation of an end-to-end Retail Data Warehouse using Medallion Architecture in Databricks Community Edition. The pipeline ingests raw retail data from CSV files, processes and transforms the data using PySpark, stores the data in Delta Lake tables, and builds business intelligence dashboards for analytics.

The project follows the industry-standard Bronze → Silver → Gold layered architecture and uses Star Schema dimensional modeling for analytical reporting.

---

# Architecture Overview

```text
Raw CSV File
      ↓
Retail Bronze Layer
(retail_bronze.orders_raw)
      ↓
Retail Silver Layer
(retail_silver.orders_clean)
      ↓
Retail Gold Layer
(retail_gold.fact_sales)
(retail_gold.dim_customer)
(retail_gold.dim_product)
(retail_gold.dim_date)
      ↓
SQL Analytics & Dashboards
```

---

# Medallion Architecture

## 1. Bronze Layer (Raw Layer)

### Schema

```text
retail_bronze
```

### Purpose

The Bronze layer stores raw ingested data exactly as received from the source system.

### Features

- Raw CSV ingestion
- Metadata tracking
- No transformations
- Delta table storage

### Table

```text
retail_bronze.orders_raw
```

---

## 2. Silver Layer (Cleaned Layer)

### Schema

```text
retail_silver
```

### Purpose

The Silver layer cleans and standardizes the raw data.

### Features

- Null handling
- Data type casting
- Column standardization
- Deduplication
- Data quality checks

### Table

```text
retail_silver.orders_clean
```

---

## 3. Gold Layer (Business Layer)

### Schema

```text
retail_gold
```

### Purpose

The Gold layer stores business-ready analytical tables using Star Schema design.

### Tables

```text
retail_gold.fact_sales
retail_gold.dim_customer
retail_gold.dim_product
retail_gold.dim_date
```

---

# Tech Stack

| Technology | Purpose |
|---|---|
| Databricks Community Edition | Cloud Data Platform |
| PySpark | Data Processing |
| Delta Lake | Data Storage |
| Spark SQL | Analytics |
| Python | ETL Development |
| GitHub | Version Control |
| Databricks Dashboards | Visualization |

---

# Project Folder Structure

```text
retail-data-warehouse-databricks/
│
├── notebooks_and_dashboards/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_warehouse.py
│   |── DASHBOARD 1 — KPI CARDS
│   ├── DASHBOARD 2 — MONTHLY REVENUE TREND
│   ├── DASHBOARD 3 — PROFIT BY REGION
│   ├── DASHBOARD 4 — SALES BY CATEGORY
│   ├── DASHBOARD 5 — TOP 10 PRODUCTS
|
├── Architecture/
│   ├── architecture_diagram.png
│
├── data/
│   └── raw_retail.csv
│
├── sql/
│   └── 04_analytics.sql
│
└── README.md
```

---

# Dataset

## Source

Retail Superstore Dataset (CSV)

### File Used

```text
raw_retail.csv
```

---

# Database Schemas

The following schemas/databases were created:

```sql
CREATE DATABASE IF NOT EXISTS retail_bronze;
CREATE DATABASE IF NOT EXISTS retail_silver;
CREATE DATABASE IF NOT EXISTS retail_gold;
```

---

# Step-by-Step Implementation

# Step 1 — Upload Dataset

Upload the CSV file into Databricks Workspace.

## Path

```text
Workspace → Users → Your Folder → raw_retail.csv
```

---

# Step 2 — Bronze Layer Implementation

## Objective

Ingest raw CSV data into Delta Lake.

## Notebook

```text
01_bronze_ingestion
```

## Read CSV File

```python
from pyspark.sql.functions import current_timestamp

# Read raw CSV

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .load("file:/Workspace/Users/your_email/raw_retail.csv")
```

---

## Add Metadata Columns

```python
from pyspark.sql.functions import current_timestamp

# Add ingestion timestamp

df_raw = df_raw.withColumn(
    "ingestion_time",
    current_timestamp()
)
```

---

## Standardize Column Names

```python
clean_columns = [
    c.replace(" ", "_")
     .replace("-", "_")
     .lower()
    for c in df_raw.columns
]

# Rename columns

df_raw = df_raw.toDF(*clean_columns)
```

---

## Save Bronze Table

```python
# Save Bronze Delta Table

df_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_bronze.orders_raw")
```

---

## Bronze Layer Screenshot

Save screenshot here:

```text
screenshots/bronze_layer.png
```

Suggested screenshot:
- Bronze table preview
- Schema
- Delta table creation

---

# Step 3 — Silver Layer Implementation

## Objective

Clean and transform raw data.

## Notebook

```text
02_silver_transformation
```

## Read Bronze Table

```python
bronze_df = spark.table("retail_bronze.orders_raw")
```

---

## Data Cleaning

```python
from pyspark.sql.functions import col

silver_df = bronze_df.dropna()

silver_df = silver_df.dropDuplicates()
```

---

## Type Casting

```python
silver_df = silver_df \
    .withColumn("sales", col("sales").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("discount", col("discount").cast("double")) \
    .withColumn("profit", col("profit").cast("double"))
```

---

## Save Silver Table

```python
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_silver.orders_clean")
```

---

## Silver Layer Screenshot

Save screenshot here:

```text
screenshots/silver_layer.png
```

Suggested screenshot:
- Cleaned data preview
- Data types
- Silver Delta table

---

# Step 4 — Gold Layer Implementation

## Objective

Build Star Schema warehouse tables.

## Notebook

```text
03_gold_warehouse
```

---

# Create Customer Dimension

```python
# Customer Dimension

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
```

---

# Create Product Dimension

```python
# Product Dimension

dim_product = silver_df.select(
    "product_id",
    "category",
    "sub_category",
    "product_name"
).dropDuplicates()
```

---

# Create Date Dimension

```python
from pyspark.sql.functions import *

# Date Dimension

dim_date = silver_df.select(
    "order_date"
).distinct()

# Add derived columns

dim_date = dim_date.withColumn(
    "year", year(col("order_date"))
).withColumn(
    "month", month(col("order_date"))
).withColumn(
    "quarter", quarter(col("order_date"))
).withColumn(
    "day", dayofmonth(col("order_date"))
)
```

---

# Create Fact Table

```python
# Fact Table

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
```

---

# Save Gold Tables

```python
# Save Customer Dimension

dim_customer.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_gold.dim_customer")

# Save Product Dimension

dim_product.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_gold.dim_product")

# Save Date Dimension

dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_gold.dim_date")

# Save Fact Table

fact_sales.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_gold.fact_sales")
```

---

## Gold Layer Screenshot

Save screenshot here:

```text
screenshots/gold_layer.png
```

Suggested screenshot:
- Gold tables
- Star schema tables
- Fact and dimension tables

---

# Star Schema Design

```text
                dim_customer
                      |
                      |
 dim_date ---- fact_sales ---- dim_product
```

---

# Step 5 — SQL Analytics

## Notebook

```text
04_analytics
```

---

# KPI Queries

## Total Revenue

```sql
SELECT ROUND(SUM(sales), 2) AS total_revenue
FROM retail_gold.fact_sales;
```

---

## Monthly Revenue Trend

```sql
SELECT
    CONCAT(d.year, '-', LPAD(d.month, 2, '0')) AS year_month,
    ROUND(SUM(f.sales), 2) AS monthly_revenue
FROM retail_gold.fact_sales f
JOIN retail_gold.dim_date d
ON f.order_date = d.order_date
GROUP BY d.year, d.month
ORDER BY year_month;
```

---

## Profit by Region

```sql
SELECT
    c.region,
    ROUND(SUM(f.profit), 2) AS total_profit
FROM retail_gold.fact_sales f
JOIN retail_gold.dim_customer c
ON f.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_profit DESC;
```

---

## Sales by Category

```sql
SELECT
    p.category,
    ROUND(SUM(f.sales), 2) AS revenue
FROM retail_gold.fact_sales f
JOIN retail_gold.dim_product p
ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;
```

---

## Top 10 Products

```sql
SELECT
    p.product_name,
    ROUND(SUM(f.sales), 2) AS revenue
FROM retail_gold.fact_sales f
JOIN retail_gold.dim_product p
ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;
```

---

# Step 6 — Dashboard Creation

## Dashboards Built

### 1. KPI Dashboard

- Total Revenue
- Total Profit
- Total Orders
- Total Customers

### 2. Monthly Revenue Trend

Visualization Type:

```text
Line Chart
```

### 3. Profit by Region

Visualization Type:

```text
Bar Chart
```

### 4. Sales by Category

Visualization Type:

```text
Donut Chart
```

### 5. Top 10 Products

Visualization Type:

```text
Horizontal Bar Chart
```

---

# Dashboard Screenshots

## Save Screenshots Here

```text
screenshots/dashboard_kpis.png
screenshots/monthly_revenue_trend.png
screenshots/profit_by_region.png
screenshots/sales_by_category.png
screenshots/top_products.png
```

---

# Key Features

- Medallion Architecture
- Delta Lake Storage
- ETL Pipeline
- PySpark Transformations
- Star Schema Modeling
- Spark SQL Analytics
- Business Intelligence Dashboards
- Metadata Tracking
- Data Cleaning & Validation

---

# Concepts Covered

- Data Engineering
- Data Warehousing
- ETL Pipelines
- Medallion Architecture
- Delta Lake
- Spark SQL
- PySpark
- Dimensional Modeling
- Business Intelligence
- Dashboarding
- Data Cleaning
- Lakehouse Architecture

---

# Future Enhancements

Possible future improvements:

- Incremental Loading
- Streaming Data Pipelines
- Apache Airflow Integration
- Slowly Changing Dimensions (SCD Type 2)
- Power BI Integration
- CI/CD Deployment
- Data Quality Framework
- Auto Loader Integration

---

# How to Run the Project

## Step 1

Clone repository:

```bash
git clone https://github.com/your-username/retail-data-warehouse-databricks.git
```

---

## Step 2

Open Databricks Community Edition.

---

## Step 3

Upload:

```text
raw_retail.csv
```

into Workspace.

---

## Step 4

Run notebooks in order:

```text
01_bronze_ingestion
02_silver_transformation
03_gold_warehouse
04_analytics
```

---

# Project Outcome

This project successfully demonstrates the implementation of a modern Retail Lakehouse Data Warehouse using Databricks and Medallion Architecture. The solution automates ETL workflows, structures data using Star Schema modeling, and provides business analytics through interactive dashboards.

---

# Author

Rajiv Pillalamarri
---

# License

This project is for educational and portfolio purposes.
