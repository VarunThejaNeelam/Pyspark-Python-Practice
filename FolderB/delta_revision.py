# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee_details(
# MAGIC   employee_id int,
# MAGIC   employee_name varchar(20),
# MAGIC   salary int,
# MAGIC   city varchar(20)
# MAGIC )
# MAGIC using DELTA
# MAGIC location "dbfs:/FileStore/tables/delta_employee"

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,"employee_details")
delta_instance.toDF().show()

# COMMAND ----------

data=[
    (1,"varun",30000,"Gudur"),
    (2,"suresh",40000,"kavali"),
    (3,"venu",50000,"sydhapur")
]
schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("employee_name",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("city",StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.write.insertInto('employee_details',overwrite=False)

# COMMAND ----------

df_log=spark.read.format('json').load("dbfs:/FileStore/tables/delta_employee/_delta_log/00000000000000000001.json")
df_log.display()

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,"employee_details")
delta_instance.toDF().show()

# COMMAND ----------

data=[
    (3,"venu",50000,"sydhapuram"),
    (4,"lokesh",70000,"nellore")
]
df_source=spark.createDataFrame(data,schema)
df_source.display()

# COMMAND ----------

#Merge operation
delta_instance.alias("target_delta").merge(
    df_source.alias("new_delta"),
    condition="target_delta.employee_id=new_delta.employee_id"
).whenMatchedUpdate(
    set={
        "employee_name":"new_delta.employee_name",
        "salary":"new_delta.salary",
        "city":"new_delta.city"
    }
).whenNotMatchedInsert(
    values={
        "employee_id":"new_delta.employee_id",
        "employee_name":"new_delta.employee_name",
        "salary":"new_delta.salary",
        "city":"new_delta.city"
    }
).execute()


# COMMAND ----------

delta_df=spark.read.format('delta').load("dbfs:/FileStore/tables/delta_employee")
delta_df.display()

# COMMAND ----------

df_source.createOrReplaceTempView("source_emp")

# COMMAND ----------

---
%sql
merge into employee_details as target
using source_emp as source
on="target.employee_id=source.employee_id"
when matched then
update set={
  "employee_name":"source.employee_name",
  "salary":"source.salary",
  "city":"source.city"
}
when not matched then
insert(target.employee_id,target.employee_name,target.salary,target.city)values(source.employee_id,source.employee_name,source.salary,source.city)
---

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details

# COMMAND ----------

delta_instance.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_details version as of 4

# COMMAND ----------

df_log=spark.read.format('json').load("dbfs:/FileStore/tables/delta_employee/_delta_log/00000000000000000004.json")
df_log.display()

# COMMAND ----------

# Create the schema for the inventory Delta table
inventory_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# Create the inventory DataFrame (Delta Table data)
inventory_data = [
    (101, 50, 20.00),
    (102, 30, 15.50),
    (103, 10, 25.00),
    (104, 5, 10.00)
]

inventory_df = spark.createDataFrame(data=inventory_data, schema=inventory_schema)

# Write the inventory DataFrame to Delta table (assuming a Delta table path)
inventory_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/delta_inventory")

# Create the schema for the incoming DataFrame
incoming_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("action", StringType(), True)
])

# Create the incoming DataFrame (New Data)
incoming_data = [
    (101, 50, 22.00, "update"),  # Update price for existing product
    (105, 40, 18.00, "insert"),  # Insert new product
    (103, 0, None, "delete")     # Delete product
]

incoming_df = spark.createDataFrame(data=incoming_data, schema=incoming_schema)

# Show the data for both DataFrames
print("Inventory Delta Table Data:")
inventory_df.show()

print("Incoming Data:")
incoming_df.show()

# COMMAND ----------

# Load the Delta table
delta_table = DeltaTable.forPath(spark, "dbfs:/FileStore/tables/delta_inventory")

# Perform the MERGE operation
delta_table.alias("target").merge(
    incoming_df.alias("source"),
    "target.product_id = source.product_id"
).whenMatchedUpdate(
    condition="source.action = 'update'",
    set={
        "quantity": "source.quantity",
        "price": "source.price"
    }
).whenMatchedDelete(
    condition="source.action = 'delete'"
).whenNotMatchedInsert(
    values={
        "product_id": "source.product_id",
        "quantity": "source.quantity",
        "price": "source.price"
    }
).execute()


# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/delta_inventory")
df.display()