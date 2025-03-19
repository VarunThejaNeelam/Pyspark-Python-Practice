# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# Initial data for customer_dim Delta table
existing_data = [
    (1, "John Doe", "123 Elm St", "2024-01-01", None, True),
    (2, "Jane Smith", "456 Oak Ave", "2024-01-01", None, True),
    (3, "Alice Lee", "789 Pine Blvd", "2024-01-01", None, True)
]

existed_schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("address",StringType(),True),
    StructField("start_date",StringType(),True),
    StructField("end_date",StringType(),True),
    StructField("is_active",BooleanType(),True)
])

existed_df = spark.createDataFrame(existing_data,existed_schema)

# Save the initial data to a Delta table (adjust path as needed)
existed_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/delta_customers_dim")

# Simulated source data with updates
source_data = [
    (1, "John Doe", "321 Maple St", "2024-06-01"),  # Updated address for existing customer
    (2, "Jane Smith", "456 Oak Ave", "2024-06-01"),  # No change
    (3, "Alice Lee", "789 Pine Blvd", "2024-06-01"),  # No change
    (4, "Bob Johnson", "101 Birch Rd", "2024-06-01")  # New customer
]

source_schema =  StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("address",StringType(),True),
    StructField("start_date",StringType(),True)
])

source_df = spark.createDataFrame(source_data, source_schema)

# Display the DataFrames
print("Initial Data:")
existed_df.show()

print("Source Data:")
source_df.show()

# COMMAND ----------

joined_df=source_df.alias("source").join(
    existed_df.alias("target"),
    source_df["customer_id"] == existed_df["customer_id"],
    "left"
)
# Select the desired columns
result_df = joined_df.select(
    col("source.customer_id").alias("source_customer_id"),
    col("source.name").alias("source_name"),
    col("source.address").alias("source_address"),
    col("source.start_date").alias("source_start_date"),
    col("target.customer_id").alias("target_customer_id"),
    col("target.name").alias("target_name"),
    col("target.address").alias("target_address"),
    col("target.start_date").alias("target_start_date"),
    col("target.end_date").alias("target_end_date"),
    col("target.is_active").alias("target_is_active")
)

result_df.display()

# COMMAND ----------

filtered_df=result_df.filter(
    (col("source_customer_id") == col("target_customer_id"))&
    (col("source_address") != col("target_address")) | (col("target_customer_id").isNull()))
filtered_df.display()    

# COMMAND ----------

merge_df=filtered_df.withColumn("mergekey",col("source_customer_id"))

merge_df.display()

# COMMAND ----------

dummy_df=filtered_df.filter("target_customer_id is not null")

dummy_df=dummy_df.withColumn("mergekey",lit(None))
dummy_df.display()

# COMMAND ----------

scd_df=merge_df.union(dummy_df)
scd_df.display()

# COMMAND ----------

delta_ins=DeltaTable.forPath(spark,"dbfs:/FileStore/tables/delta_customers_dim")
delta_ins.alias("target").merge(
    scd_df.alias("source"),
    "target.customer_id = source.mergekey and target.is_active = True "
).whenMatchedUpdate(
    set={
        "end_date":"source.source_start_date",
        "is_active":lit(False)
    }
).whenNotMatchedInsert(
    values={
        "customer_id":"source.source_customer_id",
        "name":"source.source_name",
        "address":"source.source_address",
        "start_date":"source.source_start_date",
        "end_date":lit(None),
        "is_active":lit(True)
    }
).execute()

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/delta_customers_dim")
df.display()

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True)
])

# Initial data
initial_data = [
    (1, 101, 2, 20.0),
    (2, 102, 1, 15.0),
    (3, 103, 5, 50.0),
    (4, 104, 3, 30.0)
]

# Create DataFrame
sales_df = spark.createDataFrame(initial_data, schema)

# Save as Delta table
sales_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/sales_data")

# COMMAND ----------

delta_table=DeltaTable.forPath(spark,"dbfs:/FileStore/tables/sales_data")
delta_table.update(
    condition="product_id = 101",
    set={
        "price":"25"
    }
)
display(delta_table.toDF())


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data

# COMMAND ----------

#spark.sql("SELECT * FROM sales_data").show()

#insert the new data into delta table

data=[
    (5, 105, 4, 40.0),
    (6, 106, 5, 35.0)
]
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True)
])
df = spark.createDataFrame(data,schema)
df.write.insertInto("sales_data",overwrite=False)

# COMMAND ----------

delta_table.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into sales_data values(7, 107, 2, 10.0);
# MAGIC insert into sales_data values(8, 108, 2, 15.0);
# MAGIC insert into sales_data values(9, 109, 3,18.0);

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/sales_data/_delta_log

# COMMAND ----------

df=spark.read.format("parquet").load("dbfs:/FileStore/tables/sales_data/_delta_log/00000000000000000010.checkpoint.parquet")
df.display()