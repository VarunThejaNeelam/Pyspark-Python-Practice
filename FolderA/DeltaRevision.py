# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_details(
# MAGIC   employee_id integer,
# MAGIC   name string,
# MAGIC   department string,
# MAGIC   salary float
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/employee_details"

# COMMAND ----------

spark.sql("select * from employee_details")

# COMMAND ----------

data=[
    (1,"varun","Dev",50000.00),
    (2,"venu","Ml",45000.00),
    (3,"suresh","Ai",70000.00),
    (4,"lokesh","Sales",60000.00),
    (5,"vinay","Marketing",55000.00)
]
schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("department",StringType(),True),
    StructField("salary",FloatType(),True)
])

delta_df=spark.createDataFrame(data,schema)
delta_df.display()

# COMMAND ----------

delta_df.write.insertInto("employee_details",overwrite=False)

# COMMAND ----------

spark.sql("select * from employee_details").show()

# COMMAND ----------

data=[
    (6,"vamsi","Dev",50000.00),
    (7,"srinu","Ml",45000.00)
]
schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("department",StringType(),True),
    StructField("salary",FloatType(),True)
])
df=spark.createDataFrame(data,schema)
df.write.insertInto("employee_details",overwrite=False)

# COMMAND ----------

spark.sql("select * from employee_details").show()

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,"employee_details")

# COMMAND ----------

delta_instance.toDF().display()

# COMMAND ----------

delta_instance.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details version as of 2

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,"employee_details")

# COMMAND ----------

# Update: Increase salary by 10% for employees in the Sales department
delta_instance.update(
    condition="department='sales'"
    set{"salary":"salary*1.10"}
)
# Delete: Remove employees from the HR department
delta_instance.delete(
    condition="department='Ml'"
)

# COMMAND ----------

data=[
    (3,"suresh","Ai",75000.00),
    (1,"varun","BigData",45000.00)
]
schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("department",StringType(),True),
    StructField("salary",FloatType(),True)
])
upcoming_df=spark.createDataFrame(data,schema)
upcoming_df.show()

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,"employee_details")

delta_instance.alias("target").merge(
    upcoming_df.alias("source"),condition="target.employee_id=source.employee_id"
).whenMatchedUpdate(
    condition="target.salary!=source.salary",
    set={"salary":"source.salary"}
).whenNotMatchedInsert(
    values={
        "target.employee_id":"source.employee_id",
        "target.name":"source.name",
        "target.department":"source.department",
        "target.salary":"source.salary"
    }
).execute()

# COMMAND ----------

"""
Partitioning and Z-Ordering
Task:
Create a Delta table with partitioning on the department column.
Insert multiple records into the table.
Optimize the table using Z-order indexing on the salary column.
Hint:
Specify the partition column when creating the Delta table and use ZORDER BY for optimization."""

# COMMAND ----------

# Sample data
data = [
    (1, "Alice", "HR", 50000.00),
    (2, "Bob", "IT", 70000.00),
    (3, "Charlie", "HR", 60000.00),
    (4, "Diana", "Sales", 75000.00),
    (5, "Eve", "IT", 80000.00),
    (6, "Frank", "Sales", 65000.00)
]

# Schema definition
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", FloatType(), True)
])

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Write the DataFrame to Delta table with partitioning on 'department'
delta_table_path = "dbfs:/FileStore/tables/partitioned_employees_details"
df.write.format("delta").partitionBy("department").mode("overwrite").save(delta_table_path)

# Optimize the table using Z-order indexing on 'salary'
DeltaTable.forPath(spark, delta_table_path).optimize().zorderBy("salary")