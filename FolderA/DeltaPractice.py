# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

data=[(1,'Varun','Gudur','India'),(2,'Suresh',"Kavali",'India'),(3,'Venu','NewYork','USA')]
schema=StructType([
          StructField('emp_id',IntegerType(),True),
          StructField('name',StringType(),True),
          StructField('city',StringType(),True),
          StructField('country',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tar_employee(
# MAGIC          emp_id INT,
# MAGIC          name STRING,
# MAGIC          city STRING,
# MAGIC          country STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/FileStore/tables/delta_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into tar_employee values(1,'Varun','Nellore','India')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tar_employee

# COMMAND ----------

df.createOrReplaceTempView("source_employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  source_employee

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1 %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into tar_employee as target using source_employee as source on target.emp_id == source.emp_id
# MAGIC when matched then
# MAGIC update
# MAGIC set
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country
# MAGIC   when not matched then
# MAGIC insert(emp_id,name,city,country) values(emp_id, name, city, country)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE audit_log(
# MAGIC              operation STRING,
# MAGIC              updated_time TIMESTAMP,
# MAGIC              user_name STRING,
# MAGIC              notebook_name STRING,
# MAGIC              numTargetRowsUpdated INT,
# MAGIC              numTargetRowsInserted INT,
# MAGIC              numTargetRowsDeleted INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM audit_log

# COMMAND ----------

delta_df=DeltaTable.forPath(spark,"/FileStore/tables/delta_merge")
last_df=delta_df.history(1)
display(last_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Explode operation metrics 

# COMMAND ----------

exploded_df=last_df.select(last_df.operation,explode(last_df.operationMetrics))
explode_df_select=exploded_df.select(exploded_df.operation,exploded_df.key,exploded_df.value.cast('int'))
explode_df_select.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Pivot for converting rows to columns 

# COMMAND ----------

pivot_df=explode_df_select.groupBy('operation').pivot("key").sum("value")
pivot_df.display()

# COMMAND ----------

pivot_df_select=pivot_df.select(pivot_df.operation,pivot_df.numTargetRowsUpdated,pivot_df.numTargetRowsInserted,pivot_df.numTargetRowsDeleted)
pivot_df_select.display()

# COMMAND ----------

audit_df=pivot_df_select.withColumn("user_name",lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).withColumn("notebook_name",lit(dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn("updated_time",lit(current_timestamp()))
audit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Rearranging columns according to my audit log table

# COMMAND ----------

audit_df_select=audit_df.select(audit_df.operation,audit_df.updated_time,audit_df.user_name,audit_df.notebook_name,audit_df.numTargetRowsUpdated,audit_df.numTargetRowsInserted,audit_df.numTargetRowsDeleted)
audit_df_select.display()

# COMMAND ----------

audit_df_select.createOrReplaceTempView("audit")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into audit_log
# MAGIC select* from audit

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from audit_log

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/weather/')