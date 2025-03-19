# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.format('csv').option('header','true').load("dbfs:/FileStore/tables/emp-1.csv")
df.show()

# COMMAND ----------

data=[(1,'Varun',101,95),(2,'Suresh',101,97),(3,'Lokesh',102,94),(4,'Venu',102,98),(5,'Manoj',101,100)]
schema=StructType([
    StructField('StudentID',IntegerType(),True),
    StructField('StudentName',StringType(),True),
    StructField('SubjectID',IntegerType(),True),
    StructField('Marks',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_selected_columns=df.select('StudentName','SubjectID','Marks',row_number().over(Window.partitionBy('SubjectID').orderBy(desc("Marks"))).alias("row_number"))
df_selected_columns.show()

# COMMAND ----------

w=Window.partitionBy("SubjectID").orderBy(desc("Marks"))

# COMMAND ----------

df_with=df.withColumn("rank",rank().over(w))\
          .withColumn("dense_rank",dense_rank().over(w))

df_with.show()           

# COMMAND ----------

df_agg=df.groupBy("SubjectID").agg(
              max("Marks").alias("maximum_marks"),
              min("Marks").alias("minimum_marks"),
              avg("Marks").alias("avg_marks")
)
df_agg.show()

# COMMAND ----------

data=[(1,'Varun',101,95),(2,'Suresh',101,97),(3,'Lokesh',102,94),(4,'Venu',102,98),(5,'Manoj',101,100)]
schema=StructType([
    StructField('StudentID',IntegerType(),True),
    StructField('StudentName',StringType(),True),
    StructField('SubjectID',IntegerType(),True),
    StructField('Marks',IntegerType(),True)
])

df_student=spark.createDataFrame(data,schema)
df_student.display()

# COMMAND ----------

data=[(101,1,'vedha_vyasa_em_school'),
      (102,2,"vivekanandha_school"),
      (101,3,"vedha_vyasa_em_school"),
      (102,4,"vivekanandha_school"),
      (101,5,"vedha_vyasa_em_school"),
      (103,6,"netaji_school")]
schema=StructType([
     StructField("SchoolID",IntegerType(),True),
     StructField("StudentID",IntegerType(),True),
     StructField("SchoolName",StringType(),True)
])
df_school=spark.createDataFrame(data,schema)
df_school.display()


# COMMAND ----------

#joining both students and school tables
df_inner=df_student.join(df_school,df_student.StudentID==df_school.StudentID,how="inner")\
            .select(df_student.StudentName,df_student.Marks,df_school.SchoolName)
df_inner.show()

# COMMAND ----------

df_outer=df_student.join(df_school,df_student.StudentID==df_school.StudentID,how="outer")\
            .select(df_student.StudentName,df_student.Marks,df_school.SchoolName)
df_outer.show()

# COMMAND ----------

 df_new_column=df_student.withColumn(
     "student_grades",
     when(col("Marks") == 100, "Excellent")
    .when((col("Marks") > 96) & (col("Marks") <= 99), "VeryGood")
    .when(col("Marks") >= 95, "Good")
    .otherwise("NotGood")
 )
 df_new_column.display()

# COMMAND ----------

