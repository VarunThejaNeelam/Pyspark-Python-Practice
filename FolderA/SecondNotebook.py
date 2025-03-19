# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

data=[(1,'Varun','Mechanical'),(2,'Suresh','Ca'),(3,'Venu','Computers')]
schema=StructType([
    StructField('StudentID',IntegerType(),True),
    StructField('StudentName',StringType(),True),
    StructField('Major',StringType(),True)]
)

# COMMAND ----------

df=spark.createDataFrame(data,schema)
print('Original dataframe')
df.show()

# COMMAND ----------

#Replace column with using replace method in dataframe
df_replaced= df.replace('Ca','Charted Accountant',subset=['Major'])
print('Replaced datframe by replace method')
df_replaced.show()

# COMMAND ----------

#Changing column withcolumn and when
df_transformed_multiple = df.withColumn(
        "StudentName",
        when (col("StudentName")=='Varun','Varun Theja')
       .when(col("StudentName")=='Suresh','Suresh Reddy')
       . otherwise(col("StudentName"))
)
print("Replacing columns withcolumn method")
df_transformed_multiple.show()

# COMMAND ----------

#Third way using sql
df.createOrReplaceTempView("students")
df_sql_transformed=spark.sql("""
            SELECT
                StudentID,
                CASE 
                    WHEN StudentName='Varun' THEN 'Varun Theja'
                    WHEN StudentName='Suresh' THEN 'Suresh Reddy'
                    WHEN StudentName='Venu' THEN 'Venu Parichuri'
                    ELSE StudentName
                END AS StudentName,
                CASE 
                    WHEN Major='Ca' THEN 'Chartered Accountant'
                    ELSE Major
                    END AS Major
             FROM students       
""")
print("After sql transformations")
df_sql_transformed.show()

# COMMAND ----------

df_sql_transformed.write.format('parquet').option('header',True).mode('overwrite').saveAsTable('StudentsData')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM studentsdata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

df=spark.read.format('parquet').option('header',True).load('dbfs:/user/hive/warehouse/studentsdata')
df.display()

# COMMAND ----------

#Register as a table 
df.createOrReplaceTempView("studentsdata_temp")

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/studentsdata',True)

# COMMAND ----------

data=[('Varun',25,'Gudur'),('Suresh',26,'Kavali'),('Venu',25,'Sydhapuram')]

schema=StructType([
    StructField('Name',StringType(),True),
    StructField('Age',IntegerType(),True),
    StructField('City',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df_filtered=df.filter(df.Age>=25)
df_filtered.show()
#This can achieve by where method also 
#df_filtered = df.where("Age > 30")
#df_filtered.show()

# COMMAND ----------

#Adding Modifying columns using withcolumn
df_modified=df.withColumn(
    "AgeCategory",
    when (df.Age>25,"Adult")
    .when (df.Age<=25,"Young")
    .otherwise("Senior")
)
df_modified.show()

# COMMAND ----------

