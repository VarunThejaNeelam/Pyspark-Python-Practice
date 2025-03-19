# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("file_path","")

# COMMAND ----------

file_path=dbutils.widgets.get("file_path")

file_path = dbutils.widgets.get("file_path")
df = spark.read.option('header', 'true').csv(file_path)
df.display()