# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 1: Create a Delta Table

# COMMAND ----------

data=[(1,'varun',25),(2,'venu',25),(3,'suresh',27),(4,'lokesh',26)]
schema=StructType([
           StructField('emp_id',IntegerType(),True),
           StructField('emp_name',StringType(),True),
           StructField('age',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from employee_info

# COMMAND ----------

delta_instance=DeltaTable.forName(spark,'employee_info')
#update the record
delta_instance.update(
       condition="emp_id=3",
       set={"age":"24"}
)
#delete the record
delta_instance.delete(
         condition="emp_id=1"
)
#insert the record

data=[(5,'manoj',26)]
df_new=spark.createDataFrame(data,schema)
df_new.write.insertInto('employee_info',overwrite=False)

# COMMAND ----------

# upsert operation
data = [(3, "suresh", 26), (1, "varun", 25)]
df_newdata = spark.createDataFrame(data, schema)
# performing merge operation

delta_instance.alias("target_delta").merge(
    df_newdata.alias("new_delta"), condition="target_delta.emp_id=new_delta.emp_id"
).whenMatchedUpdate(
    set={"emp_name": "new_delta.emp_name", "age": "new_delta.age"}
).whenNotMatchedInsert(
    values={
        "emp_id": "new_delta.emp_id",
        "emp_name": "new_delta.emp_name",
        "age": "new_delta.age",
    }
).execute()

# COMMAND ----------

df_merge=spark.read.format('delta').table("employee_info")
df_merge.display()

# COMMAND ----------

data=[(1,'varun',25),(2,'venu',25),(3,'suresh',27),(4,'lokesh',26)]
schema=StructType([
           StructField('emp_id',IntegerType(),True),
           StructField('emp_name',StringType(),True),
           StructField('age',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)


# COMMAND ----------

df.write.format('delta').saveAsTable('employee_details')

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 2: Schema Evolution

# COMMAND ----------

new_data=[(7,'vinay',27,30000)]
new_schema=StructType([
           StructField('emp_id',IntegerType(),True),
           StructField('emp_name',StringType(),True),
           StructField('age',IntegerType(),True),
           StructField('salary',IntegerType(),True)
])

# COMMAND ----------

# Load the existing Delta table data
existing_data = spark.table('employee_details')

# New data to append
new_data = [(7, 'vinay', 27, 30000)]

# Define schema for new data
new_schema = StructType([
    StructField('emp_id', IntegerType(), True),
    StructField('emp_name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('salary', IntegerType(), True)
])

# Create a DataFrame for new data
df_new_data = spark.createDataFrame(new_data, new_schema)

# Check if new data already exists in the Delta table (by emp_id)
df_to_append = df_new_data.join(existing_data, ['emp_id'], 'leftanti')

# Append only new data that doesn't exist in the Delta table
if df_to_append.count() > 0:
    df_to_append.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable('employee_details')
else:
    print("No new records to append")


# COMMAND ----------

#df_new_data=spark.createDataFrame(new_data,new_schema)

# Remove duplicates based on 'emp_id' (or another unique identifier)
df_no_duplicates = delta_instance.toDF().dropDuplicates(['emp_id'])

df_no_duplicates.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable('employee_details')

# Refresh the table metadata to reflect schema changes
spark.catalog.refreshTable('employee_details')

delta_instance=DeltaTable.forName(spark,'employee_details')

delta_instance.toDF().printSchema()
delta_instance.toDF().show()

# COMMAND ----------

delta_instance.update(condition="emp_id>1 and emp_id<=4",set={"salary":'50000'})

# COMMAND ----------

delta_instance.toDF().show()

# COMMAND ----------

df_his=delta_instance.history()
df_his.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details version As Of 0

# COMMAND ----------

