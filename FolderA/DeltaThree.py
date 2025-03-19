# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE student_details(
# MAGIC   student_id INT,
# MAGIC   student_name STRING,
# MAGIC   subject STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC using delta
# MAGIC location "dbfs:/FileStore/tables/delta_student";
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into student_details values(4,'lokesh','maths',87);
# MAGIC insert into student_details values(5,'vinay','science',85);
# MAGIC insert into student_details values(6,'manoj','english',89);
# MAGIC insert into student_details values(7,'sathu','chemistry',97);
# MAGIC insert into student_details values(8,'venky','biology',78);
# MAGIC insert into student_details values(9,'deepak','mechanics',87);
# MAGIC insert into student_details values(10,'banu','maths',87);

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from student_details

# COMMAND ----------

#loading delta log file 
df_log=spark.read.format('json').load("dbfs:/FileStore/tables/delta_student/_delta_log/00000000000000000009.json")
df_log.display()

# COMMAND ----------

data=[(1,'varun','maths',99),(2,'venu','physics',95),(3,'suresh','computers',90)]
schema=StructType([
            StructField('student_id',IntegerType(),True),
            StructField('student_name',StringType(),True),
            StructField('subject',StringType(),True),
            StructField('marks',IntegerType(),True)
])

# COMMAND ----------

df_new=spark.createDataFrame(data,schema)

# COMMAND ----------

df_new.write.insertInto('student_details',overwrite=False)

# COMMAND ----------

#loading the delta table 
df=spark.read.format('delta').load("dbfs:/FileStore/tables/delta_student")
df.display()

# COMMAND ----------

delta_ins=DeltaTable.forPath(spark,"dbfs:/FileStore/tables/delta_student")

# delta_ins.update(
#     condition="student_id = 6", 
#     set={"marks":"100"}
# )

# COMMAND ----------

delta_ins.delete(
    condition="student_id = 1"
)
delta_ins.toDF().show()

# COMMAND ----------

df_checkpoint=spark.read.format('parquet').load("dbfs:/FileStore/tables/delta_student/_delta_log/00000000000000000010.checkpoint.parquet")
df_checkpoint.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into student_details values(1,'varun','maths',100);

# COMMAND ----------

df_js=spark.read.format('json').load("dbfs:/FileStore/tables/delta_student/_delta_log/00000000000000000006.json")
df_js.display()

# COMMAND ----------

delta_ins.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE student_details 
# MAGIC using delta 
# MAGIC LOCATION 'dbfs:/FileStore/tables/delta_student'

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from student_details

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM student_details version AS of 5

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize student_details
# MAGIC zorder by (student_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from student_details where student_id<5

# COMMAND ----------

df_optimize=spark.read.format('json').load("dbfs:/FileStore/tables/delta_student/_delta_log/00000000000000000012.json")
df_optimize.display()

# COMMAND ----------

#checking history after doing optimize on delta table 
delta_ins.history(11).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=true