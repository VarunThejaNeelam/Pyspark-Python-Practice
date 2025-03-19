# Databricks notebook source
from pyspark.sql.functions import col
import random

# Create a large dataset
data = []
for i in range(1, 10001):  # Generate 10,000 rows
    data.append((
        i,
        random.choice(["USA", "Canada", "UK", "India"]),  # Random country
        random.choice(["Electronics", "Clothing", "Furniture", "Groceries"]),  # Random category
        random.randint(1, 1000),  # Random sales amount
        random.randint(2023, 2025),  # Random year
        random.randint(1, 12)  # Random month
    ))

# Define schema and create DataFrame
columns = ["transaction_id", "country", "category", "sales_amount", "year", "month"]
df = spark.createDataFrame(data, schema=columns)

# Save the DataFrame with both partitioning and bucketing
output_table = "partitioned_bucketed_table"

df.write \
    .bucketBy(10, "category") \
    .partitionBy("year", "month") \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "dbfs:/FileStore/partitioned_bucketed_table") \
    .saveAsTable(output_table)

print("Data written successfully as a partitioned and bucketed table!")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/partitioned_bucketed_table/year=2023/month=1/part-00007-tid-3002595500381437439-51c59140-306b-4bcd-8e53-55652d85ef37-200-3_00008.c000.snappy.parquet")
df.display()

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/partitioned_bucketed_table/year=2023/month=1/part-00007-tid-3002595500381437439-51c59140-306b-4bcd-8e53-55652d85ef37-200-1_00001.c000.snappy.parquet")
df.display()