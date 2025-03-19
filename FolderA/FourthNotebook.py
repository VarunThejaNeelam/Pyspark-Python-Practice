# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from pyspark.sql.functions import date_format, col

# COMMAND ----------

data=[(1,'Varun',30000),(2,'Suresh',40000),(3,'Venu',50000)]
schema=StructType([
       StructField('EmployeeID',IntegerType(),True),
       StructField('EmployeeName',StringType(),True),
       StructField('Salary',IntegerType(),True)
])


# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df_modified=df.withColumn(
    "Salary",
    when(col("Salary")==30000,70000)
    .otherwise(col("Salary"))
)
df_modified.show()

# COMMAND ----------

df_delete=df_modified.filter(df_modified.EmployeeID!=3)
df_delete.show()

# COMMAND ----------

new_row_data=[(3,'Venu',55000)]
new_row_added_df=spark.createDataFrame(new_row_data,schema)
new_row_added_df.show()

# COMMAND ----------

df_with_new_row=df_delete.union(new_row_added_df)
df_with_new_row.show()

# COMMAND ----------

#Select certain columns by using select method
data=[('Laptop',2,300),('Tv',3,200)]
schema=StructType([
        StructField('Item',StringType(),True),
        StructField('quantity',IntegerType(),True),
        StructField('unitprice',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_calculated_columns=df.select(
           "Item",
           "quantity",
           "unitprice",
           (col("quantity") * col("unitprice")).alias("TotalPrice")
)
#Dataframe with calculated columns 
df_calculated_columns.show()

# COMMAND ----------

#Applying filters to retrieve rows
data=[(1,101,10000),(2,102,15000),(3,103,25000)]
schema=StructType([
        StructField('CustomerID',IntegerType(),True),
        StructField('orderID',IntegerType(),True),
        StructField('amount',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_filtered=df.filter((df.amount>10000)&(df.CustomerID>2))
df_filtered.show()

# COMMAND ----------

#Grouping and aggregations operations on dataframe
data=[(1,101,3),(2,101,2),(3,102,2),(4,103,1),(5,103,2)]
schema=StructType([
          StructField('SaleID',IntegerType(),True),
          StructField('ProductID',IntegerType(),True),
          StructField('quantity',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_aggregations=df.groupBy("ProductID").agg(
       sum("quantity").alias("total_sales"),
       max("quantity").alias("max_sales")
)
df_aggregations.show()

# COMMAND ----------

# Joining multiple dataframes
data = [(1, "Varun", 101,95), (2, "Suresh", 102,100), (3, "Venu", 101,100),(4,'Lokesh',102,100)]
schema = StructType(
    [
        StructField("StudentID", IntegerType(), True),
        StructField("StudentName", StringType(), True),
        StructField("SubjectID", IntegerType(), True),
        StructField('Marks',IntegerType(),True)
    ]
)

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

data=[(101,'Maths'),(102,'Physics')]
schema=StructType([
       StructField('ID',IntegerType(),True),
       StructField('subject_name',StringType(),True)
])

# COMMAND ----------

df_sub=spark.createDataFrame(data ,schema)
df_sub.show()

# COMMAND ----------

w=Window.partitionBy("SubjectID").orderBy(desc("Marks"))

# COMMAND ----------

joined_df = (
    df.join(df_sub, col("SubjectID") == col("ID"), how="inner")
    .withColumn("row_number", row_number().over(w))
    .withColumn("rank", rank().over(w))
    .withColumn("dense_rank", dense_rank().over(w))
    .select(df.StudentID, "StudentName", "SubjectID", "Marks", df_sub.subject_name,"row_number","rank","dense_rank")
)
          
joined_df.show()

# COMMAND ----------

data = [
    ("2023-09-01", "2024-01-01 12:30:00", "2023-12-25"),
    ("2023-06-15", "2024-07-15 08:45:00", "2024-05-05"),
    ("2023-03-30", "2023-11-20 18:00:00", "2024-02-14")
]
schema = StructType([
    StructField("start_date", StringType(), True),
    StructField("end_timestamp", StringType(), True),  # Change to TimestampType if needed
    StructField("holiday", StringType(), True)
])


# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

new_row = spark.createDataFrame([("2023-04-12", None, "2024-03-10")],schema)
new_df_with_newrow=df.union(new_row)
new_df_with_newrow.show()

# COMMAND ----------

df_with_coalesce = new_df_with_newrow.withColumn("preferred_date", coalesce(new_df_with_newrow["end_timestamp"], new_df_with_newrow["holiday"]))
df_with_coalesce.show()                                                 

# COMMAND ----------

new_df_with_newrow.filter(new_df_with_newrow["end_timestamp"].isNotNull()).show()

# COMMAND ----------

new_df_with_newrow.na.fill({"end_timestamp":0}).show()

# COMMAND ----------

df_format=df.select("start_date","end_timestamp","holiday",date_format(col("holiday"),"dd-MM-yyyy").alias("formatted_date"))       
df_format.show()

# COMMAND ----------

df.select(datediff(col("end_timestamp"),col("start_date")).alias("date_difference")).show()

# COMMAND ----------

df.select(date_add(col("start_date"), 10).alias("day_added")).show()

# COMMAND ----------

df.select(year(col("holiday")).alias("year")).show()

# COMMAND ----------

df.select("start_date",to_date("end_timestamp").alias("end_date"),"holiday").show()

# COMMAND ----------

myrdd=spark.sparkContext.parallelize([1,2,3,4])
map_fun=myrdd.map(lambda x:x*2)
# Action to collect the result and display it
result=map_fun.collect()
print(f"Result of map function:{result}")

# COMMAND ----------

rdd=spark.sparkContext.parallelize([2,3,4,5])
first_three=rdd.take(3)
print(first_three)

# COMMAND ----------

rdd=spark.sparkContext.parallelize([2,3,4,5])
mul_rdd=rdd.reduce(lambda x,y:x*y)
print(mul_rdd)

# COMMAND ----------

rdd=spark.sparkContext.parallelize(range(1,10+1))
rdd.collect()

# COMMAND ----------

firstname="Varun"
dbutils.notebook.exit(firstname)

# COMMAND ----------

if "firstname" in locals():
    lastname = "Theja"
    print(lastname)