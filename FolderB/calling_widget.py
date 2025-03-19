# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

temp_file_path=dbutils.notebook.run("/Users/varunteja584@gmail.com/FolderB/widget_practice",120,{"file_path":"dbfs:/FileStore/tables/emp-1.csv"})

df_result=spark.read.format('csv').load("temp_file_path")
df_result.display()