# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window

# COMMAND ----------

# Define schema
target_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("address", StringType(), False),
    StructField("valid_from", StringType(), False),
    StructField("valid_to", StringType(), True),
    StructField("is_active", IntegerType(), False)
])

# Create target DataFrame
target_data = [
    (101, "Alice", "alice@example.com", "123 Main St, NY", "2024-01-10 08:00:00", "2024-02-05 09:45:00", 0),
    (101, "Alice", "alice@example.com", "222 Oak St, NY", "2024-02-05 09:45:00", None, 1),
    (102, "Bob", "bob@example.com", "456 Elm St, CA", "2024-01-12 10:15:00", "2024-03-01 14:20:00", 0),
    (102, "Bob", "bob@example.com", "777 Maple St, CA", "2024-03-01 14:20:00", None, 1),
    (103, "Carol", "carol@example.com", "789 Pine St, TX", "2024-01-15 12:30:00", None, 1)
]

target_df = spark.createDataFrame(target_data, schema=target_schema)
target_customers_df = target_df.withColumn("valid_from", col("valid_from").cast("timestamp"))
target_customers_df = target_df.withColumn("valid_to", col("valid_to").cast("timestamp"))

target_customers_df.write.option('header', True).format('delta').save("dbfs:/FileStore/tables/target_customers")


# COMMAND ----------

# Define schema
incoming_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("address", StringType(), False),
    StructField("update_timestamp", StringType(), False)
])

# Create incoming DataFrame
incoming_data = [
    (101, "Alice", "alice@example.com", "555 Birch St, NY", "2024-03-10 11:00:00"),  # Address change
    (104, "Varun", "varun@example.com", "Gudur", "2024-03-01 14:20:00"),    # New record
    (103, "Carol", "carol@example.com", "999 Cedar St, TX", "2024-04-15 09:30:00")  # Address change
]

incoming_df = spark.createDataFrame(incoming_data, schema=incoming_schema)
incoming_df = incoming_df.withColumn("update_timestamp", col("update_timestamp").cast("timestamp"))
target_customers_df.show()
incoming_df.show()
 

# COMMAND ----------

joined_df = incoming_df.alias("source").join(
    target_customers_df.alias("target"),
    (col("source.customer_id") == col("target.customer_id")) & 
    (col("target.valid_to").isNull()),  # Ensure only active records are joined
    "left"
)
joined_df = joined_df.select(
    col("source.customer_id").alias("source_customer_id"),
    col("source.name").alias("source_name"),
    col("source.email").alias("source_email"),
    col("source.address").alias("source_address"),
    col("source.update_timestamp").alias("source_update_timestamp"),
    col("target.customer_id").alias("target_customer_id"),
    col("target.name").alias("target_name"),
    col("target.email").alias("target_email"),
    col("target.address").alias("target_address"),
    col("target.valid_from").alias("target_valid_from"),
    col("target.valid_to").alias("target_valid_to"),
    col("target.is_active").alias("target_is_active")
)

joined_df.display()

# COMMAND ----------

filtered_df = joined_df.filter(
    (col("source_customer_id") == col("target_customer_id"))&
    (col("source_address") != col("target_address")) |
    col("target_customer_id").isNull()
)
filtered_df.display()

# COMMAND ----------

merge_df = filtered_df.withColumn("merge_key", col("source_customer_id"))
merge_df.display()

# COMMAND ----------

dummy_df = filtered_df.filter(col("target_customer_id").isNotNull())
dummy_df = dummy_df.withColumn("merge_key", lit(None))
dummy_df.display()

# COMMAND ----------

scd_df = merge_df.union(dummy_df)
scd_df.display()

# COMMAND ----------

delta_ins = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/target_customers")
# Merge statement
delta_ins.alias("target").merge(
    scd_df.alias("source"),
    condition = "target.customer_id = source.merge_key and target.is_active = 1"
).whenMatchedUpdate(
    set={
        "valid_to":"source_update_timestamp",
        "is_active":lit(0)
    }
).whenNotMatchedInsert(
    values={
        "customer_id":"source_customer_id",
        "name":"source_name",
        "email":"source_email",
        "address":"source_address",
        "valid_from":"source_update_timestamp",
        "valid_to":lit(None),
        "is_active":lit(1)
    }
).execute()


# COMMAND ----------

df = spark.read.format('delta').load("dbfs:/FileStore/tables/target_customers")
df.show()