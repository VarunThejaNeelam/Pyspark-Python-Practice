# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

"""
Data Ingestion and Transformation from Local Files
Scenario: You have a collection of CSV files with product sales data stored locally in Databricks. The files contain columns such as product_id, region, sales_amount, and sales_date. You need to load these CSV files into a single DataFrame, clean the data, and perform transformations.
Task:
Load multiple CSV files from a directory into a PySpark DataFrame.
Clean the data by removing rows with missing or invalid values.
Group the data by product_id and calculate the total sales per product.
Convert the sales_date to a DateType and filter for sales within a specific range (e.g., last 30 days).
Save the cleaned and transformed data as a Parquet file in the Databricks File System (DBFS)."""

# COMMAND ----------

data = [
    (1, "North", 1000.50, "2024-11-01"),
    (2, "South", 1500.75, "2024-11-02"),
    (3, "East", 2000.25, "2024-11-03"),
    (4, "West", 1250.00, "2024-11-01"),
    (5, None, None, "2024-11-04"),
    (1, "North", 1100.00, "2024-11-02"),
    (2, "South", 1300.50, "2024-11-03"),
    (3, "East", 2100.75, "2024-11-04"),
    (4, "West", 1400.00, "2024-11-02"),
    (5, None, None,"2024-11-05")
]

schema=StructType([
   StructField("product_id",IntegerType(),True),
  StructField("region",StringType(),True),
  StructField("sales_amount",FloatType(),True),
  StructField("sales_date",StringType(),True)
])

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

#Clean the data by removing rows with missing or invalid values.
df_cleaned=df.withColumn("sales_date",to_date(col("sales_date"),"yyyy-mm-dd"))

#df_filtered=df_cleaned.fillna({'sales_amount': 0, 'region': 'Unknown'})
df_filtered=df_cleaned.filter((col("sales_amount")>0)&(col("region").isNotNull()))
df_filtered.display()

# COMMAND ----------

#Group the data by product_id and calculate the total sales per product.
#window_spec=Window.partitionBy("product_id")
#product_sales_df=df_filtered.withColumn("total_sales",sum("sales_amount").over(window_spec))
#product_sales_df.display()
df_grouped=df_filtered.groupBy("product_id").agg(sum("sales_amount").alias("total_sales"))
df_grouped.display()

# COMMAND ----------

#Convert the sales_date to a DateType and filter for sales within a specific range (e.g., last 30 days).
df_with_before_date=df_filtered.withColumn("thirty_days_before_date",date_sub(col("sales_date"), 30))
#df_with_before_date.display()

df_final=df_with_before_date.filter(col("sales_date")>=col("thirty_days_before_date"))
df_final.show()
df_final.coalesce(1).write.format("parquet").save("dbfs:/FileStore/tables/parquet")
print("File saved to the path")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/parquet/part-00000-tid-6886693828075356301-8fe1b207-498a-4bec-820f-d16feb6b84c2-138-1-c000.snappy.parquet")
df.show()

# COMMAND ----------

"""
SCD Type 2 Implementation with Sample Data
Scenario: You are tasked with implementing a Slowly Changing Dimension (SCD) Type 2 for a small employee dataset, where each employee's record might change over time (e.g., job title, department, or salary). You need to keep track of historical changes while ensuring that the most recent information is available.
Task:
Create two DataFrames: source_df (new incoming data) and target_df (existing data).
Compare the two DataFrames based on employee_id and determine which records need to be inserted, updated, or archived.
Implement logic to mark old records as "expired" and insert new records with updated information.
Keep track of the valid_from and valid_to columns for each employee's historical record.
Output the updated records to a new DataFrame and print the results."""

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS employee_details")

# COMMAND ----------

historical_data = [
    (101, "Software Engineer", "IT", 60000, "2020-01-01", "2021-06-30", "N"),
    (101, "Senior Software Engineer", "IT", 75000, "2021-07-01", "9999-12-31", "Y"),
    (102, "Data Scientist", "Data Science", 65000, "2020-03-01", "2022-02-28", "N"),
    (102, "Senior Data Scientist", "Data Science", 80000, "2022-03-01", "9999-12-31", "Y"),
    (103, "Marketing Manager", "Marketing", 70000, "2019-11-01", "2021-12-31", "N"),
    (103, "Senior Marketing Manager", "Marketing", 85000, "2022-01-01", "9999-12-31", "Y")
]

schema=StructType([
  StructField("employee_id",IntegerType(),True),
  StructField("job_title",StringType(),True),
  StructField("department",StringType(),True),
  StructField("salary",IntegerType(),True),
  StructField("start_date",StringType(),True),
  StructField("end_date",StringType(),True),
  StructField("current_flag",StringType(),True)
])
historical_df=spark.createDataFrame(historical_data,schema)
historical_df.write.format("delta").save("dbfs:/FileStore/data/employee_details")

# COMMAND ----------

target_table=DeltaTable.forPath(spark,"dbfs:/FileStore/data/employee_details")
target_table.toDF().display()

# COMMAND ----------

incoming_data = [
    (101, "Lead Software Engineer", "IT", 80000, "2022-07-01", "9999-12-31", "Y"),  # Promotion for employee 101
    (104, "Data Analyst", "Data Science", 55000, "2023-01-01", "9999-12-31", "Y"),  # New employee 104
    (103, "Senior Marketing Director", "Marketing", 95000, "2023-02-01", "9999-12-31", "Y"),  # Promotion for employee 103
    (102, "Principal Data Scientist", "Data Science", 95000, "2023-03-01", "9999-12-31", "Y")  # Promotion for employee 102
]

schema=StructType([
  StructField("employee_id",IntegerType(),True),
  StructField("job_title",StringType(),True),
  StructField("department",StringType(),True),
  StructField("salary",IntegerType(),True),
  StructField("start_date",StringType(),True),
  StructField("end_date",StringType(),True),
  StructField("current_flag",StringType(),True)
])
incoming_df=spark.createDataFrame(incoming_data,schema)


# COMMAND ----------

target_table.toDF().show(truncate=False)


# COMMAND ----------

"""target_table.alias("target").merge(
    incoming_df.alias("source"),
    condition="target.employee_id = source.employee_id"
).whenMatchedUpdate(
    condition="target.job_title != source.job_title OR target.salary != source.salary",
    set={
        "target.end_date": "source.start_date",
        "target.current_flag": "'N'"
    }
).whenNotMatchedInsert(
    condition="target.employee_id IS NULL",  # This ensures no matching record exists for the employee
    values={
        "employee_id": "source.employee_id",
        "job_title": "source.job_title",
        "department": "source.department",
        "salary": "source.salary",
        "start_date": "source.start_date",
        "end_date": "'9999-12-31'",  # Set end_date to 9999-12-31 for the new record
        "current_flag": "'Y'"  # Mark the new record as current
    }
).execute()
"""

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/FileStore/tables/employee_history")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table employees_det(
# MAGIC   employee_id int,
# MAGIC   job_title varchar(20),
# MAGIC   department varchar(20),
# MAGIC   salary int,
# MAGIC   start_date date,
# MAGIC   end_date date,
# MAGIC   current_flag char(1)
# MAGIC ) using delta
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employees_det
# MAGIC CHANGE COLUMN job_title job_title STRING;

# COMMAND ----------

historical_data = [
    (101, "Software Engineer", "IT", 60000, "2020-01-01", "2021-06-30", "N"),
    (101, "Senior Software Engineer", "IT", 75000, "2021-07-01", "9999-12-31", "Y"),
    (102, "Data Scientist", "Data Science", 65000, "2020-03-01", "2022-02-28", "N"),
    (102, "Senior Data Scientist", "Data Science", 80000, "2022-03-01", "9999-12-31", "Y"),
    (103, "Marketing Manager", "Marketing", 70000, "2019-11-01", "2021-12-31", "N"),
    (103, "Senior Marketing Manager", "Marketing", 85000, "2022-01-01", "9999-12-31", "Y")
]


# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   employees_det
# MAGIC values
# MAGIC   (
# MAGIC     101,
# MAGIC     "Software Engineer",
# MAGIC     "IT",
# MAGIC     60000,
# MAGIC     "2020-01-01",
# MAGIC     "2021-06-30",
# MAGIC     "N"
# MAGIC   ),
# MAGIC   (
# MAGIC     101,
# MAGIC     "Senior Software Engineer",
# MAGIC     "IT",
# MAGIC     75000,
# MAGIC     "2021-07-01",
# MAGIC     "9999-12-31",
# MAGIC     "Y"
# MAGIC   ),
# MAGIC   (
# MAGIC     102,
# MAGIC     "Data Scientist",
# MAGIC     "Data Science",
# MAGIC     65000,
# MAGIC     "2020-03-01",
# MAGIC     "2022-02-28",
# MAGIC     "N"
# MAGIC   ),
# MAGIC   (
# MAGIC     102,
# MAGIC     "Senior Data Scientist",
# MAGIC     "Data Science",
# MAGIC     80000,
# MAGIC     "2022-03-01",
# MAGIC     "9999-12-31",
# MAGIC     "Y"
# MAGIC   ),
# MAGIC   (
# MAGIC     103,
# MAGIC     "Marketing Manager",
# MAGIC     "Marketing",
# MAGIC     70000,
# MAGIC     "2019-11-01",
# MAGIC     "2021-12-31",
# MAGIC     "N"
# MAGIC   ),
# MAGIC   (
# MAGIC     103,
# MAGIC     "Senior Marketing Manager",
# MAGIC     "Marketing",
# MAGIC     85000,
# MAGIC     "2022-01-01",
# MAGIC     "9999-12-31",
# MAGIC     "Y"
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from employees_det

# COMMAND ----------

incoming_data = [
    (101, "Lead Software Engineer", "IT", 80000, "2022-07-01", "9999-12-31", "Y"),  
    (104, "Data Analyst", "Data Science", 55000, "2023-01-01", "9999-12-31", "Y"),  
    (103, "Senior Marketing Director", "Marketing", 95000, "2023-02-01", "9999-12-31", "Y"),  
    (102, "Principal Data Scientist", "Data Science", 95000, "2023-03-01", "9999-12-31", "Y")  
]

schema=StructType([
  StructField("employee_id",IntegerType(),True),
  StructField("job_title",StringType(),True),
  StructField("department",StringType(),True),
  StructField("salary",IntegerType(),True),
  StructField("start_date",StringType(),True),
  StructField("end_date",StringType(),True),
  StructField("current_flag",StringType(),True)
])
incoming_df=spark.createDataFrame(incoming_data,schema)


# COMMAND ----------

updated_df=incoming_df.withColumn("start_date",to_date(col("start_date"))).withColumn("end_date",to_date(col("end_date")))

# COMMAND ----------

updated_df.printSchema()

# COMMAND ----------

updated_df.createOrReplaceTempView('incoming_source')

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from incoming_source

# COMMAND ----------

# MAGIC %sql
# MAGIC ---merge operation
# MAGIC merge into employees_det as target
# MAGIC using (
# MAGIC   select
# MAGIC         employee_id as merge_key,
# MAGIC         employee_id,
# MAGIC         job_title,
# MAGIC         department,
# MAGIC         salary,
# MAGIC         start_date,
# MAGIC         end_date,
# MAGIC         current_flag 
# MAGIC   from incoming_source
# MAGIC   union
# MAGIC   select
# MAGIC         null as merge_key,
# MAGIC         employee_id,
# MAGIC         job_title,
# MAGIC         department,
# MAGIC         salary,
# MAGIC         start_date,
# MAGIC         end_date,
# MAGIC         current_flag 
# MAGIC   from incoming_source 
# MAGIC   where exists(
# MAGIC     select * from employees_det 
# MAGIC     where employees_det.employee_id=incoming_source.employee_id) 
# MAGIC )as source
# MAGIC on target.employee_id=source.merge_key and target.current_flag="Y"
# MAGIC when matched then
# MAGIC update set target.current_flag="N",
# MAGIC            target.end_date=source.start_date
# MAGIC when not matched then
# MAGIC insert(employee_id,job_title,department,salary,start_date,end_date,current_flag)
# MAGIC values(source.employee_id,source.job_title,source.department,source.salary,source.start_date,source.end_date,source.current_flag)              
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees_det

# COMMAND ----------

historical_data = [
    (101, "Software Engineer", "IT", 60000, "2020-01-01", "2021-06-30", "N"),
    (101, "Senior Software Engineer", "IT", 75000, "2021-07-01", "9999-12-31", "Y"),
    (102, "Data Scientist", "Data Science", 65000, "2020-03-01", "2022-02-28", "N"),
    (102, "Senior Data Scientist", "Data Science", 80000, "2022-03-01", "9999-12-31", "Y"),
    (103, "Marketing Manager", "Marketing", 70000, "2019-11-01", "2021-12-31", "N"),
    (103, "Senior Marketing Manager", "Marketing", 85000, "2022-01-01", "9999-12-31", "Y")
]

schema=StructType([
  StructField("employee_id",IntegerType(),True),
  StructField("job_title",StringType(),True),
  StructField("department",StringType(),True),
  StructField("salary",IntegerType(),True),
  StructField("start_date",StringType(),True),
  StructField("end_date",StringType(),True),
  StructField("current_flag",StringType(),True)
])
historical_df=spark.createDataFrame(historical_data,schema)
historical_df.write.format("delta").save("dbfs:/FileStore/data/employee_details")

# COMMAND ----------

incoming_data = [
    (101, "Lead Software Engineer", "IT", 80000, "2022-07-01", "9999-12-31", "Y"),  
    (104, "Data Analyst", "Data Science", 55000, "2023-01-01", "9999-12-31", "Y"),  
    (103, "Senior Marketing Director", "Marketing", 95000, "2023-02-01", "9999-12-31", "Y"),  
    (102, "Principal Data Scientist", "Data Science", 95000, "2023-03-01", "9999-12-31", "Y")  
]

schema=StructType([
  StructField("employee_id",IntegerType(),True),
  StructField("job_title",StringType(),True),
  StructField("department",StringType(),True),
  StructField("salary",IntegerType(),True),
  StructField("start_date",StringType(),True),
  StructField("end_date",StringType(),True),
  StructField("current_flag",StringType(),True)
])
incoming_df=spark.createDataFrame(incoming_data,schema)



# COMMAND ----------

source_df=incoming_df.withColumn("start_date",to_date(col("start_date"))).withColumn("end_date",to_date(col("end_date")))

# COMMAND ----------

source_df.display()

# COMMAND ----------

target_table=DeltaTable.forPath(spark,"dbfs:/FileStore/data/employee_details")


# COMMAND ----------

# Perform the merge operation
target_table.alias("target").merge(
    source_df.alias("source"),
    "target.employee_id = source.employee_id AND target.current_flag = 'Y'"
).whenMatchedUpdate(set={
    "current_flag": lit("N"),
    "end_date": col("source.start_date")
}).whenNotMatchedInsert(values={
    "employee_id": col("source.employee_id"),
    "job_title": col("source.job_title"),
    "department": col("source.department"),
    "salary": col("source.salary"),
    "start_date": col("source.start_date"),
    "end_date": col("source.end_date"),
    "current_flag": col("source.current_flag")
}).execute()

# COMMAND ----------

emp_df=spark.read.format('delta').load("dbfs:/FileStore/data/employee_details")
emp_df.display()

# COMMAND ----------

# Define the schema for the clickstream data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("page_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True)
])

# Sample data
data = [
    (1, 101, "2024-11-12 09:00:00", "click"),
    (1, 102, "2024-11-12 09:05:00", "click"),
    (1, 101, "2024-11-12 09:10:00", "click"),
    (1, 101, "2024-11-12 09:05:00", "click"),
    (1, 103, "2024-11-12 09:07:00", "click"),
    (2, 103, "2024-11-11 08:00:00", "click"),
    (2, 101, "2024-11-11 09:30:00", "click"),
    (2, 102, "2024-11-11 09:40:00", "click"),
    (2, 102, "2024-11-11 09:35:00", "click"),
    (1, 102, "2024-11-12 09:15:00", "click")
]

# Create DataFrame
clickstream_df = spark.createDataFrame(data, schema)

# COMMAND ----------

clickstream_df=clickstream_df.withColumn("timestamp",to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
clickstream_df.printSchema()

# COMMAND ----------

clickstream_df.display()

# COMMAND ----------

filtered_df=clickstream_df.filter(to_date(col("timestamp"))>=date_sub(current_date(),1))
filtered_df.display()

# COMMAND ----------

#Aggregate the data to calculate the number of visits per user for each page.
aggregated_df=filtered_df.groupBy("user_id","page_id").agg(count(col("page_id")).alias("visit_counts"))
aggregated_df.display()

# COMMAND ----------

#Identify the top 3 most visited pages for each user.
window_spec = Window.partitionBy("user_id").orderBy(col("visit_counts").desc())

ranking_df=aggregated_df.withColumn("rank",rank().over(window_spec))
ranking_df.display() 

# COMMAND ----------

ranking_df.write.format('parquet').save("dbfs:/FileStore/data/clickstream")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/data/clickstream/part-00000-tid-7039761499437345150-fbdcc715-d45e-4d0f-a116-c054ff701937-280-1-c000.snappy.parquet")
df.display()

# COMMAND ----------

"""
Scenario: You have a dataset containing customer data with columns customer_id, email, and purchase_amount. You need to perform data quality checks and generate a report with the issues found.
Task:
Load the customer data from a CSV file into a DataFrame.
Perform the following data quality checks:
Check for duplicate customer_id values.
Identify rows where email is missing or invalid (use a simple regex for email validation).
Check for negative or missing purchase_amount values.
Generate a summary report of the issues found (e.g., count of duplicates, invalid emails, etc.).
Save the rows with data quality issues to a separate CSV file."""

# COMMAND ----------

# Define schema for the data
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("purchase_amount", DoubleType(), True)
])

# Sample data
data = [
    (1, "john.doe@example.com", 150.00),
    (2, "jane.doe@example", 200.00),
    (3, None, 300.00),
    (4, "bob.smith@sample.org", -50.00),
    (5, "alice.jones@example.com", None),
    (1, "john.doe@example.com", 150.00),
    (6, "jim.beam@@example.com", 250.00),
    (7, "mike@example.com", 100.00),
    (8, "valid.email@domain.com", -10.00)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, count

# Check for duplicate customer_id values
window_spec = Window.partitionBy("customer_id")
duplicates_df = df.withColumn("duplicate_count_for_customer", count("customer_id").over(window_spec))
duplicates_df = duplicates_df.filter(col("duplicate_count_for_customer") > 1)

# Regex pattern for a basic email validation (using rlike for regex in PySpark)
email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w{2,}$'
invalid_emails_df = df.filter(col("email").isNull() | ~col("email").rlike(email_pattern))

# Check for negative or missing purchase_amount values
negative_or_missing_purchase_amount_df = df.filter((col("purchase_amount") < 0) | col("purchase_amount").isNull())

# Summary dictionary to store DataFrames with issues
summary = {
    "duplicates_df": duplicates_df,
    "invalid_emails_df": invalid_emails_df,
    "negative_or_missing_purchase_amount_df": negative_or_missing_purchase_amount_df
}

# Display each DataFrame with issues
for key, issue_df in summary.items():
    print(f"\nDataFrame name: {key}")
    issue_df.show()
