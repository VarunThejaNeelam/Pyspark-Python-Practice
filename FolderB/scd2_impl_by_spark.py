# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from datetime import date  # Import the date function

# COMMAND ----------

# Create the schema
schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('city', StringType(), True),
    StructField('effective_date', StringType(), True)  # Initially keep as StringType
])

# Create the source data with date strings
source_data = [
    (1, "varun", "Banglore", '2024-10-01'),
    (2, "venu", "sydhapuram", '2024-10-01'),
    (3, "suresh", "kavali", '2024-10-01')
]

# Create the DataFrame
source_df = spark.createDataFrame(source_data, schema)

# Convert 'effective_date' from StringType to DateType
source_df = source_df.withColumn("effective_date", to_date(source_df["effective_date"], "yyyy-MM-dd"))

source_df.write.format("delta").saveAsTable("source_information")
# Show the DataFrame
source_df.show()

# COMMAND ----------

# Create target DataFrame (historical data)
target_data = [
    (1, 'varun', 'Gudur', date(2023,10,1), None, True),
    (2, 'venu', 'Sydhapuram', date(2023,10,1), None, True)
]
# Define schema
schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('city', StringType(), True),
    StructField('effective_date', DateType(), True),
    StructField('expiry_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])
target_df=spark.createDataFrame(target_data,schema)
target_df.write.format('delta').saveAsTable("target_information")
target_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW merged_info AS
# MAGIC SELECT 
# MAGIC     source.customer_id,
# MAGIC     source.name AS new_name,
# MAGIC     source.city AS new_city,
# MAGIC     target.name AS old_name,
# MAGIC     target.city AS old_city,
# MAGIC     target.effective_date,
# MAGIC     target.expiry_date,
# MAGIC     target.is_current
# MAGIC FROM source_information AS source
# MAGIC LEFT JOIN target_information AS target
# MAGIC ON source.customer_id = target.customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update current records in the target that have changed values
# MAGIC MERGE INTO target_information AS target
# MAGIC USING merged_info AS merged
# MAGIC ON target.customer_id = merged.customer_id
# MAGIC WHEN MATCHED AND target.is_current = TRUE 
# MAGIC   AND (target.name != merged.new_name OR target.city != merged.new_city) THEN
# MAGIC   UPDATE SET 
# MAGIC     target.is_current = FALSE,
# MAGIC     target.expiry_date = CURRENT_DATE();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new records
# MAGIC INSERT INTO target_information (customer_id, name, city, effective_date, expiry_date, is_current)
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     new_name AS name,
# MAGIC     new_city AS city,
# MAGIC     CURRENT_DATE() AS effective_date,
# MAGIC     NULL AS expiry_date,
# MAGIC     TRUE AS is_current
# MAGIC FROM merged_info
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 
# MAGIC     FROM target_information AS target
# MAGIC     WHERE target.customer_id = merged_info.customer_id
# MAGIC       AND target.is_current = TRUE
# MAGIC       AND (target.name = merged_info.new_name AND target.city = merged_info.new_city)
# MAGIC );
# MAGIC

# COMMAND ----------

# Select final result from target table
final_result = spark.sql("SELECT * FROM target_information")
final_result.show(truncate=False)


# COMMAND ----------

# Step 1: Update existing records to inactive
update_query = """
MERGE INTO target_information AS target
USING source_information AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND target.is_current = TRUE 
  AND (target.name != source.name OR target.city != source.city) THEN
  UPDATE SET 
    target.is_current = FALSE,
    target.expiry_date = CURRENT_DATE()
"""
spark.sql(update_query)

# Step 2: Insert new records (including updates)
insert_query = """
MERGE INTO target_information AS target
USING source_information AS source
ON target.customer_id = source.customer_id
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, city, effective_date, expiry_date, is_current)
  VALUES (source.customer_id, source.name, source.city, CURRENT_DATE(), NULL, TRUE)
"""
spark.sql(insert_query)


# COMMAND ----------

# Select final result from target table
final_result = spark.sql("SELECT * FROM target_information")
final_result.show(truncate=False)

# COMMAND ----------

# Sample source data (raw data)
raw_data = [
    (1, "John Doe", "456 Oak St, NY", "Active", "2023-06-02"),
    (3, "Alice Lee", "654 Birch St, TX", "Active", "2023-09-02"),
    (5, "varun", "gudur 13/159", "Active", "2023-01-01")
]

# Define the schema for the SCD2 table
schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("start_date", StringType(), nullable=False),
])

# Create DataFrame for raw data
raw_df = spark.createDataFrame(raw_data, schema)

# Show the raw data
raw_df.display()

# Create initial dimension table (Target Table) with previous records
dim_data = [
    (1, "John Doe", "123 Elm St, NY", "Inactive", "2023-01-01", None, True),
    (2, "Jane Smith", "789 Pine St, CA", "Active", "2023-01-01", None, True),
    (3, "Alice Lee", "321 Maple St, TX", "Inactive", "2023-01-01", None, True),
    (4, "Bob Brown", "987 Cedar St, FL", "Active", "2023-01-01", None, True)
]

#Define the schema
schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("start_date", StringType(), nullable=False),
    StructField("end_date", StringType(), nullable=True),
    StructField("is_current", BooleanType(), nullable=False)
])
#create a dataframe
dim_df = spark.createDataFrame(dim_data, schema)

dim_df = dim_df.withColumn("start_date", to_date(col("start_date"))) \
               .withColumn("end_date", to_date(col("end_date")))

#dim_df.write.format("delta").save("dbfs:/FileStore/tables/dim_customer_target2")

# Show the dimension table (Target Table)
dim_df.display()

# Fix the join to avoid ambiguity
joined_df = raw_df.alias("source").join(
    dim_df.alias("target"),
    raw_df["customer_id"] == dim_df["customer_id"],
    "left"
)
joined_df.display()

# COMMAND ----------

#Finding new records 
new_records=joined_df.filter(col("target.customer_id").isNull())
new_records.display()

#Records which is updated in raw data
update_records=joined_df.filter(
    (col("target.customer_id")==col("source.customer_id"))&
    (col("target.address") != col("source.address")) | (col("target.status") != col("source.status")))
update_records.display()


# COMMAND ----------

#updating records which is already there in target table as in active
updates_closure_df=update_records.select(
    col("target.customer_id"),
    col("target.name"),
    col("target.address"),
    col("target.status"),
    col("target.start_date"),
    col("source.start_date").alias("end_date"),
    lit(False).alias("is_current")
)

update_records_as_new=update_records.select(
    col("source.customer_id"),
    col("source.name"),
    col("source.address"),
    col("source.status"),
    col("source.start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

insert_new_records=new_records.select(
    col("source.customer_id"),
    col("source.name"),
    col("source.address"),
    col("source.status"),
    col("source.start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

final_df=updates_closure_df.union(update_records_as_new).union(insert_new_records)
#writing dataframe to path
final_df.write.format("delta").mode("append").save("dbfs:/FileStore/tables/scd2_dim_customer_target")

df=spark.read.format("delta").load("dbfs:/FileStore/tables/scd2_dim_customer_target")

# COMMAND ----------

metadata_df=spark.read.format("json").load("dbfs:/FileStore/tables/scd2_dim_customer/_delta_log/00000000000000000000.json")
metadata_df.display() 