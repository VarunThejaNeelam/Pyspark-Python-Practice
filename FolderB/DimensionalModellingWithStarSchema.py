# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# 1. Product Dimension Table
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True)
])

product_data = [
    (1, "Laptop", "Electronics"),
    (2, "Phone", "Electronics"),
    (3, "Shirt", "Apparel"),
    (4, "Jacket", "Apparel")
]

product_df = spark.createDataFrame(product_data, product_schema)

# Show the Product Dimension table
product_df.show()
product_df.write.format('csv').option('header',True).save("dbfs:/FileStore/raw_layer/products_dimension")

# 2. Customer Dimension Table
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True)
])

customer_data = [
    (101, "John", "Doe", "New York", "NY", "USA"),
    (102, "Alice", "Smith", "Los Angeles", "CA", "USA"),
    (103, "Bob", "Johnson", "London", "NA", "UK")
]

customer_df = spark.createDataFrame(customer_data, customer_schema)
# Show the Customer Dimension table
customer_df.show()
customer_df.write.format('csv').option('header',True).save("dbfs:/FileStore/raw_layer/customers_dimension")

# 3. Date Dimension Table
date_schema = StructType([
    StructField("date_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("month", StringType(), True),
    StructField("quarter", StringType(), True),
    StructField("year", IntegerType(), True)
])

date_data = [
    ("20220101", "2022-01-01", "January", "Q1", 2022),
    ("20220102", "2022-01-02", "January", "Q1", 2022),
    ("20220103", "2022-01-03", "January", "Q1", 2022),
    ("20220201", "2022-02-01", "February", "Q1", 2022)
]

date_df = spark.createDataFrame(date_data, date_schema)

# Show the Date Dimension table
date_df = date_df.withColumn("date", to_date(col("date")))
date_df.show()
date_df.write.format('csv').option('header',True).save("dbfs:/FileStore/raw_layer/dates_dimension")

# 4. Sales Fact Table
sales_schema = StructType([
    StructField("sales_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("date_id", StringType(), True),
    StructField("amount", FloatType(), True)
])

sales_data = [
    (1001, 101, 1, "20220101", 1200.00),
    (1002, 102, 2, "20220102", 800.00),
    (1003, 103, 3, "20220201", 50.00),
    (1004, 101, 4, "20220201", 200.00)
]

sales_df = spark.createDataFrame(sales_data, sales_schema)

# Show the Sales Fact table
sales_df.show()
sales_df.write.format('csv').option('header',True).save("dbfs:/FileStore/raw_layer/sales_fact")

# 5. Order Fact Table
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("date_id", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

order_data = [
    (5001, 101, 1, "20220101", 1),
    (5002, 102, 2, "20220102", 2),
    (5003, 103, 3, "20220201", 5),
    (5004, 101, 4, "20220201", 3)
]

order_df = spark.createDataFrame(order_data, order_schema)

# Show the Order Fact table
order_df.show()
order_df.write.format('csv').option('header',True).save("dbfs:/FileStore/raw_layer/orders_fact")

# COMMAND ----------

# Reading the files from paths
products_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/rawlayer/products_dimension")
customers_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/rawlayer/customers_dimension")
dates_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/rawlayer/dates_dimension")
sales_transactions_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/rawlayer/sales_fact")
orders_transactions_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/rawlayer/orders_fact")

# Joining customers nd products with sales transactions to aggregate total transaction amount for each product 
sales_transaction_with_details = sales_transactions_df.join(customers_df, on="customer_id", how="inner") \
    .join(products_df, on="product_id", how="inner")
  

# Aggregating total transactions amount per product
fact_salestransactions_df = sales_transaction_with_details.groupBy("customer_id", "product_id", "product_name").agg(
    sum(amount).alias("total_transactions_amount_per_product")
)    
fact_salestransactions_df.write.format('delta').option('header',True).save("dbfs:/FileStore/rawlayer/fact_salestransactions")

# Joining customers and products with orders transactions to aggregate total quantity for each product
orders_transactions_with_details = orders_transactions_df.join(customers_df, on="customer_id", how="inner") \
    .join(products_df, on="product_id", how="inner")

# Aggregating total quantity for each product for all dates
fact_orderstransactions_df = orders_transactions_with_details.groupBy("customer_id", "product_id", "product_name").agg(
    sum(quantity).alias("total_quantity_per_product")
) 

fact_orderstransactions_df.write.format('delta').option('header',True).save("dbfs:/FileStore/rawlayer/fact_orderstransactions")

# COMMAND ----------

# Handling scd type2 on customers historical data by using incoming data
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True)
])

customer_data = [
    (101, "John", "Doe", "Torronto", "NY", "USA"),
    (104, "Varun", "Theja", "Gudur", "AP", "INDIA"),
    (103, "Bob", "Johnson", "New York", "NA", "UK")
]
source_df = spark.createDataFrame(customer_data, customer_schema) 

joined_df = source_df.alias("source").join(customers_df.alias("target"), source_df["customer_id"] == customers_df["customer_id"] "left")

joined_df = joined_df.select(
    col("source.customer_id").alias("source_customer_id"),
    col("source.first_name").alias("source_first_name"),
    col("source.last_name").alias("source_last_name"),
    col("source.city").alias("source_city"),
    col("source.state").alias("source_state"),
    col("source.country").alias("source_country"),
    col("target.customer_id").alias("target_customer_id"),
    col("target.first_name").alias("target_first_name"),
    col("target.last_name").alias("target_last_name"),
    col("target.city").alias("target_city"),
    col("target.state").alias("target_state"),
    col("target.country").alias("target_country"),
)

filtered_df = joined_df.filter(
   (col("source_customer_id") == col("target_customer_id"))&
    ((col("source_city") != col("target_city")) | (col("source_state") != col("target_state")) | (col("source_country") != col("target_country"))) | (col("target_customer_id").isNull())
)

merge_df = filtered_df.withColumn(
    "merge_key",
    col("source_customer_id")
)

dummy_df = filtered_df.filter("target_customer_id is not null")

dummy_df = dummy_df.withColumn("merge_key", lit(None))

scd_df = merge_df.union(dummy_df)

delta_ins = DeltaTable.forPath(spark, "dbfs:/FileStore/rawlayer/customers_dimension")

delta_ins.alias("target").merge(
    scd_df.alias("source"),
    "target.customer_id = source.merge_key"
).whenMatchedUpdate(
    "target.city != source.source_city or target.state != source.source_state or target.country != source.source_country"
    set={
        "target.city":"source.source_city",
        "target.state":"source.source_state",
        "target.country":"source.source_country"
    }
).whenNotMatchedInsert(
    values={
        "customer_id":"source_customer_id",
        "first_name":"source_first_name",
        "last_name":"source_last_name",
        "city":"source_city",
        "state":"source_state",
        "country":"source_country"
    }
).execute()