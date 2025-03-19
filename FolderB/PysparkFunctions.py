# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# Sample data
data = [
    ("HR", 2021, 4000),
    ("HR", 2022, 4500),
    ("IT",2021,8000),
    ("Finance", 2021, 5000),
    ("Finance", 2022, 5500),
    ("IT", 2021, 6000),
    ("IT", 2022, 6500)
]
schema=StructType([
          StructField('department',StringType(),True),
          StructField('year',IntegerType(),True),
          StructField('salary',IntegerType(),True),
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

#pivot the data by year
df_pivot=df.groupBy('department').pivot('year').sum('salary')
df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Exercise 1: Total Sales by Product and Year

# COMMAND ----------

data=[
     ('Laptop',2021,100),
     ('Tv',2021,150),
     ('Phone',2021,50),
     ('Laptop',2022,100),
     ('Tv',2022,150),
     ('Phone',2022,300)
]
schema=StructType([
        StructField('Product',StringType(),True),
        StructField('Year',IntegerType(),True),
        StructField('Sales',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df_pivoted=df.groupBy('Product').pivot('Year').sum('Sales')
df_pivoted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Top Earners in Each Department:

# COMMAND ----------

data = [
    ("Alice", 12000, "HR"),
    ("Bob", 8000, "Finance"),
    ("Charlie", 15000, "IT"),
    ("David", 5000, "HR"),
    ("Eva", 20000, "Finance"),
    ("Frank", 30000, "IT"),
    ("Grace", 12000, "IT")
]
schema=StructType([
         StructField('name',StringType(),True),
         StructField('salary',IntegerType(),True),
         StructField('department',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

subquery=df.groupBy('department').agg(max('salary').alias('max_salary')).select('department','max_salary')
subquery.show()

# COMMAND ----------

df_joined=df.join(subquery,on="department",how="inner")

# COMMAND ----------

df_with_row_num=df_joined.withColumn(
    "row_number",
    row_number().over(Window.partitionBy('department').orderBy(desc('salary')))          
)
df_with_row_num.display()

# COMMAND ----------

df_filtered=df_with_row_num.filter(col('row_number')==1)
df_filtered.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Employees with Below Average Salary:

# COMMAND ----------

data = [
    ("Alice", 12000, "HR"),
    ("Bob", 8000, "Finance"),
    ("Charlie", 15000, "IT"),
    ("David", 5000, "HR"),
    ("Eva", 20000, "Finance"),
    ("Frank", 30000, "IT"),
    ("Grace", 12000, "IT")
]
schema=StructType([
         StructField('name',StringType(),True),
         StructField('salary',IntegerType(),True),
         StructField('department',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

subquery=df.groupBy('department').agg(avg('salary').alias('avg_salary')).select('department','avg_salary')
subquery.show()

# COMMAND ----------

df_joined=df.join(subquery,on="department",how="inner").filter(col('salary')<col('avg_salary'))
df_joined.display()

# COMMAND ----------

#Reading table from mysql
df=spark.read.format('jdbc').options(
    url="jdbc:mysql://localhost:3306/flm",
    dbtable="flm.student",
    user="root",
    password="varun321",
    fetchsize=1000 
).load()
df.show()

# COMMAND ----------

#Flatenning json data
data=[{
    "name": "Alice",
    "details": {
        "age": 28,
        "location": {
            "city": "Los Angeles",
            "zipcode": 900
        }
    },
    "contacts": [
        {"type": "email", "value": "alice@example.com"},
        {"type": "phone", "value": "1234567890"}
    ],
    "hobbies": ["reading", "traveling"]
}]

# COMMAND ----------

schema=StructType([
             StructField('name',StringType(),True),
             StructField('details',StructType([
                 StructField('age',IntegerType(),True),
                 StructField('location',StructType([
                     StructField('city',StringType(),True),
                     StructField('zipcode',IntegerType(),True)
                 ]),True)
             ]),True),
             StructField('contacts',ArrayType(StructType([
                    StructField('type',StringType(),True),
                    StructField('value', StringType(), True) 
             ])),True),
             StructField('hobbies',ArrayType(StringType()),True)

])

df=spark.createDataFrame(data,schema)

df_flattened = df.select(
    col("name"),
    col("details.age").alias("age"),
    col("details.location.city").alias("city"),
    col("details.location.zipcode").alias("zipcode"),
    explode(col("contacts")).alias("contact"),
    col("hobbies")
)

# Step 3: Select the individual columns, including the exploded 'contacts'
df_final = df_flattened.select(
    col("name"),
    col("age"),
    col("city"),
    col("zipcode"),
    col("contact.type").alias("contact_type"),
    col("contact.value").alias("contact_value"),
    col("hobbies")
)
df_final.display()

# COMMAND ----------

df_final.write.mode("overwrite").option("compression","gzip").option("dateFormat", "yyyy-MM-dd").json("/FileStore/data/json")

# COMMAND ----------

#Handling null values
data=[('varun','maths',93,'pass',None),('suresh','physics',85,None,88),('venu','mechanics',98,'pass',90),('lokesh','science',87,None,88)]
schema=['name','subject','marks','status','attendence']

# COMMAND ----------

df=spark.createDataFrame(data=data,schema=schema)
df.display()

# COMMAND ----------

df_filtered=df.filter((col("status").isNotNull()) & (col('subject')=='maths'))
df_filtered.show()

# COMMAND ----------

df_filtered_two=df.filter(col("attendence").isNotNull())
df_filtered_two.display()

# COMMAND ----------

df_union=df_filtered.union(df_filtered_two)
df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Handling null values with drop and fill functions

# COMMAND ----------

data=[('varun','maths',93,'pass',None),
      ('suresh','physics',85,None,88),
      ('venu','mechanics',98,'pass',90),
      ('lokesh','science',87,None,88),
      ('john','computers',None,None,99),
      (None,None,None,None,None)]
schema=['name','subject','marks','status','attendence']

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df.dropna(subset=["marks"]).display()

# COMMAND ----------

# df.fillna(value=0 ,subset=["marks","attendence"]) \
#     .fillna(value="unknown",subset=["name","subject","status"]).display()

df.fillna({"name":"no_name","subject":"Sanskrit","marks":0,"status":"NA","attendence":50}).display()

# COMMAND ----------

data=[
    ('varun',9,8),
    ('suresh',8,7),
    ('venu',7,8),
    ('lokesh',9,8),
    ('varun',8,7),
    ('varun',6,5),
    ('suresh',9,7),
    ('suresh',5,4),
    ('venu',8,9),
    ('lokesh',7,6)
]
schema=["name","score_1","score_2"]

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

df_arr=df.groupBy('name').agg(collect_list('score_1').alias('Arrays_score1'),collect_list('score_2').alias('Arrays_score2'))
df_arr.display()

# COMMAND ----------

#Using array_zip function
df_arr_zip=df_arr.withColumn("arrays_zipped_value",expr("arrays_zip(Arrays_score1, Arrays_score2)"))
df_arr_zip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 1: Data Ingestion and Transformation
# MAGIC Task: Ingest a CSV file (e.g., sales data) from Azure Blob Storage into a DataFrame and perform transformations.
# MAGIC Ingest the file into a PySpark DataFrame.
# MAGIC Add a new column total_sale by multiplying quantity and price.
# MAGIC Filter rows where total_sale > 5000.
# MAGIC Write the transformed DataFrame to a Delta table.
# MAGIC Goal: Practice loading data from external sources, basic transformations, and writing data to Delta tables.

# COMMAND ----------

source_dir = "dbfs:/FileStore/data/json/product.csv"
destination_dir = "dbfs:/FileStore/data/"

# Moving the entire directory recursively
dbutils.fs.mv(source_dir, destination_dir, recurse=True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/data/product.csv",True)

# COMMAND ----------

df = spark.read.option('header', 'true').csv('dbfs:/FileStore/data/product_details.csv')
df.display()

# COMMAND ----------

df = df.withColumnRenamed("price ", "price")



# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn('total_sales',col('quantity')*col('price'))
df.show()

# COMMAND ----------

df_filtered=df.filter(col("total_sales")>600)
df_filtered.show()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/delta_product")

# COMMAND ----------

df_filtered.write.format('delta').option('header','true').save("dbfs:/FileStore/tables/delta_product")

# COMMAND ----------

df2=spark.read.json("dbfs:/FileStore/tables/delta_product/_delta_log/00000000000000000000.json")
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 2: Join Operations and Aggregations
# MAGIC Task: You have two DataFrames, one with customer information and another with their purchase history.
# MAGIC Perform an inner join on customer_id.
# MAGIC Group the joined data by region and calculate the total amount spent by customers in each region.
# MAGIC Sort the results by total amount spent in descending order.
# MAGIC Goal: Practice joining tables and performing aggregations with groupBy and agg.

# COMMAND ----------

data=[
    (1,'Varun','India'),
    (2,'Suresh','USA'),
    (3,'Venu','Australia'),
    (4,'Lokesh','England')
]
schema=StructType([
    StructField('customer_id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('region',StringType(),True),
])
customers_df=spark.createDataFrame(data,schema)
customers_df.show()

# COMMAND ----------

data=[
    (101,200,1),
    (102,400,2),
    (102,500,1),
    (101,300,2),
    (103,100,4),
    (103,700,3),
    (104,500,3)
]
schema=StructType([
    StructField('purchase_id',IntegerType(),True),
    StructField('amount',IntegerType(),True),
    StructField('customer_id',IntegerType(),True)
])
purchases_df=spark.createDataFrame(data,schema)
purchases_df.show()

# COMMAND ----------

joined_df=customers_df.join(purchases_df,on='customer_id',how="inner")
joined_df.show()

# COMMAND ----------

aggregated_df=joined_df.groupBy("name","region").agg(sum('amount').alias("total_amount_spent_by_customer"))
aggregated_df=aggregated_df.orderBy(desc('total_amount_spent_by_customer'))
aggregated_df.display()

# COMMAND ----------

