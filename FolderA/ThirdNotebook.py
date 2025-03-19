# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum as _sum

# COMMAND ----------

data=[(1,'Varun',101,95),(2,'Suresh',101,97),(3,'Lokesh',102,94),(4,'Venu',102,98),(5,'Manoj',101,100)]
schema=StructType([
    StructField('StudentID',IntegerType(),True),
    StructField('StudentName',StringType(),True),
    StructField('SubjectID',IntegerType(),True),
    StructField('Percentage',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df_renamed=df.withColumnRenamed("Percentage","Marks")
df_renamed.show()

# COMMAND ----------

df_updated=df_renamed.withColumn(
               "Marks",
                when(col("StudentID")==2,95)
               .otherwise(col("Marks"))
)
df_updated.show()

# COMMAND ----------

w=Window.partitionBy("SubjectID").orderBy(desc("Marks"))

# COMMAND ----------

df_window_operation=df_updated.withColumn("row_number",row_number().over(w))\
                              .withColumn("rank",rank().over(w))\
                              .withColumn("dense_rank",dense_rank().over(w))

df_window_operation.show()                              
                              

# COMMAND ----------

df_win=df_renamed.select('StudentID','StudentName','SubjectID','Marks',row_number().over(Window.partitionBy("SubjectID").orderBy(desc("Marks"))).alias("row_number"))
df_win.show()


# COMMAND ----------

#Grouping by subjectId and performing operations
df_aggregated=df_renamed.groupBy("SubjectID").agg(
                        sum("Marks").alias("total_marks"),
                        avg("Marks").alias("average_marks"),
                        max("Marks").alias("maximum_marks")
)
df_aggregated.show()          

# COMMAND ----------


data=[(1,"Varun"),(2,"Venu"),(4,"Suresh")]
schema=StructType([
    StructField("EmpID",IntegerType(),True),
    StructField("Name",StringType(),True)
])


# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

data=[(1,"Development"),(4,"Marketing"),(3,"Design")]
schema=StructType([
    StructField("ID",IntegerType(),True),
    StructField("Department",StringType(),True)
])

# COMMAND ----------

df2=spark.createDataFrame(data,schema)


# COMMAND ----------

#joining dataframes
inner_join=df.join(df2,col("EmpID")==col("ID"),how="inner") \
    .select("EmpID","Name","Department")
inner_join.show()

# COMMAND ----------

left_join=df.join(df2,col("EmpID")==col("ID"),how="left") \
    .select("EmpID","Name","Department")
left_join.show()

# COMMAND ----------

full_outer_join=df.join(df2,col("EmpID")==col("ID"),how="full_outer") \
    .select("EmpID","Name","Department")
full_outer_join.show()

# COMMAND ----------

data=[(1,50000),(4,70000)]
schema=StructType([
    StructField('EmpID',IntegerType(),True),
    StructField('Salary',StringType(),True)
])

# COMMAND ----------

df3=spark.createDataFrame(data,schema)
df3.show()

# COMMAND ----------

inner_join=df.join(df2,col("EmpID")==col("ID"),how="inner").join(df3,df.EmpID==df3.EmpID,"inner")\
    .select(df.EmpID,"Name","Department",df3.Salary) 
inner_join.show()

# COMMAND ----------

left_join=df.join(df2,col("EmpID")==col("ID"),how="left").join(df3,df.EmpID==df3.EmpID,"left")\
    .select(df.EmpID,"Name","Department",df3.Salary) 
left_join.show()

# COMMAND ----------

full_outer_join=df.join(df2,col("EmpID")==col("ID"),how="full_outer").join(df3,df.EmpID==df3.EmpID,"full_outer")\
    .select(df.EmpID,"Name","Department",df3.Salary) 
full_outer_join.show()

# COMMAND ----------



# COMMAND ----------

def generate_squares(n):
    squares = []  # Initialize an empty list to store squares
    for i in range(1, n + 1):
        square = i * i  # Calculate square of the number
        squares.append(square)  # Add the square to the list
    return squares
generate_squares(10)
         

# COMMAND ----------

def count_digits(num):
    count=0;
    while(num>0):
        count +=1
        num=num/10
    return count         
