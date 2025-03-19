# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 4: Handling Complex Data Types
# MAGIC Task: You have a DataFrame with a details column that contains nested JSON objects.
# MAGIC Flatten the nested JSON structure using PySpark functions.
# MAGIC Select only relevant fields from the flattened structure (e.g., customer_id, order_id, product_details).
# MAGIC Save the flattened DataFrame as a Parquet file.
# MAGIC Goal: Practice working with nested data and flattening complex structures.

# COMMAND ----------

data = [
    (
        {
            "employees": [
                {
                    "employee_id": 1,
                    "name": "varun",
                    "department": {
                        "dept_id": 101,
                        "dept_name": "Dev",
                    },
                    "projects": [
                        {
                            "project_id": 1001,
                            "project_name": "Project A",
                            "project_status": "Completed",
                        },
                        {
                            "project_id": 1002,
                            "project_name": "Project B",
                            "project_status": "Ongoing",
                        },
                    ],
                }
            ]
        }
    )
]

json_schema=StructType([
    StructField("employees",ArrayType(
        StructType([
        StructField("employee_id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("department",StructType([
            StructField("dept_id",IntegerType(),True),
            StructField("dept_name",StringType(),True)
        ]),True),
                StructField("projects",ArrayType(StructType([
                    StructField("project_id",IntegerType(),True),
                    StructField("project_name",StringType(),True),
                    StructField("project_status",StringType(),True)
                ])),True) 
        ])
    ),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,json_schema)

 # Explode the employees array to create a separate row for each employee
df_exploded = df.select(explode(col("employees")).alias("employee"))

# Now explode and select fields as before
df_flat = df_exploded.select(
    col("employee.employee_id").alias("employee_id"),
    col("employee.name").alias("name"),
    col("employee.department.dept_id").alias("dept_id"),
    col("employee.department.dept_name").alias("dept_name"),
    explode(col("employee.projects")).alias("project_details")
)

# Selecting project details individually
df_final = df_flat.select(
    "employee_id",
    "name",
    "dept_id",
    "dept_name",
    col("project_details.project_id").alias("project_id"),
    col("project_details.project_name").alias("project_name"),
    col("project_details.project_status").alias("project_status")
)

df_final.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 3: Data Partitioning and Optimization
# MAGIC Task: You have a large dataset of logs stored in a CSV file.
# MAGIC Ingest the data and repartition it based on the date column.
# MAGIC Write the repartitioned data to Delta Lake with proper partitioning by date.
# MAGIC Perform a query to count the number of log entries per day.
# MAGIC Goal: Practice partitioning data for optimization and efficient querying.

# COMMAND ----------

df=spark.read.option('header','true').format('csv').load("dbfs:/FileStore/data/loginfo.csv")
df.display()

# COMMAND ----------

df=df.withColumnRenamed("ip_address","user_id").withColumnRenamed("timestamp","date")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(f"Number of partitions for original dataframe:{df.rdd.getNumPartitions()}")

# COMMAND ----------

repartitioned_df=df.repartition(3,"date")
print(f"Number of partitions after applying repartition :{repartitioned_df.rdd.getNumPartitions()}")

# COMMAND ----------

df_agg=repartitioned_df.groupBy('date').agg(count("*").alias("numOfLogs"))
df_agg.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 5: Window Functions
# MAGIC Task: You have sales data for different products.
# MAGIC Use window functions to calculate the running total of sales for each product.
# MAGIC Also, calculate the rank of each sale within its respective product category based on the sale_amount.
# MAGIC Goal: Practice using PySpark's window functions to compute cumulative sums and rankings.

# COMMAND ----------

data=[
    (1,101,200,"TV","Electronics"),
    (2,102,300,"Laptop","Digital"),
    (3,101,400,"TV","Electronics"),
    (4,101,500,"TV","Electronics"),
    (5,102,700,"Laptop","Digital"),
    (6,103,100,"DiningTable","HomeResources"),
    (7,103,450,"DiningTable","HomeResources")
]
schema=StructType([
    StructField("sale_id",IntegerType(),True),
    StructField("product_id",IntegerType(),True),
    StructField("sale_amount",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("product_category",StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# Define the window specification for cumulative sum
window_config_sum = Window.partitionBy("product_id").orderBy("sale_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative sum
df_with_running_sum = df.withColumn("cumulative_sum", sum(col("sale_amount")).over(window_config_sum))

# Define window for ranking based on sale_amount
window_config_rank = Window.partitionBy("product_id").orderBy(desc("sale_amount"))

# Calculate rank
df_with_running_sum = df_with_running_sum.withColumn("rank", rank().over(window_config_rank))

# Show the result
df_with_running_sum.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Customer Segmentation
# MAGIC You have a DataFrame customers with customer_id, name, age, city, and spend_score.
# MAGIC Another DataFrame, transactions, has transaction_id, customer_id, purchase_amount, and date.
# MAGIC Tasks:
# MAGIC Calculate the total and average purchase amount per customer.
# MAGIC Segment customers based on their average purchase amount (e.g., low, medium, high spender).
# MAGIC Find the top 5 cities with the highest number of customers in the "high spender" category.

# COMMAND ----------

# Create data for customers DataFrame
customers_data = [
   (1, "John Doe", 28, "New York", 75),
    (2, "Jane Smith", 34, "Los Angeles", 85),
    (3, "Alice Lee", 23, "Chicago", 65),
    (4, "Bob Brown", 40, "Houston", 90),
    (5, "Carol King", 30, "Phoenix", 80),
    (6, "David White", 29, "New York", 88),
    (7, "Emma Green", 35, "Chicago", 60),
    (8, "Frank Black", 31, "Los Angeles", 95),
    (9, "Grace Brown", 26, "Houston", 70),
    (10, "Helen Clark", 32, "Phoenix", 45)
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("spend_score", IntegerType(), True)
])
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# COMMAND ----------

# Create data for transactions DataFrame
transactions_data = [
    (1001, 1, 150.00, "2023-01-10"),
    (1002, 2, 200.50, "2023-02-15"),
    (1003, 3, 75.00, "2023-03-20"),
    (1004, 1, 120.00, "2023-04-25"),
    (1005, 4, 300.00, "2023-05-30"),
    (1006, 5, 250.00, "2023-06-05"),
    (1007, 2, 180.75, "2023-07-12"),
    (1008, 3, 50.25, "2023-08-18"),
    (1009, 4, 210.00, "2023-09-23"),
    (1010, 5, 90.00, "2023-10-10"),
    (1011, 6, 500.00, "2023-11-01"),
    (1012, 7, 40.00, "2023-11-05"),
    (1013, 8, 400.00, "2023-11-10"),
    (1014, 9, 60.00, "2023-11-12"),
    (1015, 10, 20.00, "2023-11-15")
]
# Define schema for transactions DataFrame
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("purchase_amount", FloatType(), True),
    StructField("date", StringType(), True)
])
# Create transactions DataFrame
transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)


# COMMAND ----------

joined_df=customers_df.join(transactions_df,on="customer_id",how="inner")
joined_df.display()

# COMMAND ----------

#Calculate the total and average purchase amount per customer.
aggregated_df = joined_df.groupBy("customer_id","name","city","spend_score").agg(
    sum("purchase_amount").alias("total_amount"),
    avg("purchase_amount").alias("avg_purchase_amount"),
)
aggregated_df.display()

# COMMAND ----------

#Segment customers based on their average purchase amount (e.g., low, medium, high spender).
segmented_df=aggregated_df.withColumn(
    "category",
    when(col("avg_purchase_amount")>150,"higher_category" )
    .when((col("avg_purchase_amount")>100) & (col("avg_purchase_amount")<=200),"medium_category")
    .when((col("avg_purchase_amount")>50) & (col("avg_purchase_amount")<=100),"lower_category")
    .otherwise("bad_category")
)
segmented_df.display()

# COMMAND ----------

#Find the top 5 cities with the highest number of customers in the "high spender" category.
higher_spender=segmented_df.filter(col("category")=="higher_category")
#grouping the higher categories by city
grouped_df=higher_spender.groupBy("city").count()
grouped_df.show()
#finding top cities having more higher category
top_cities_df=grouped_df.orderBy(col("count").desc()).limit(5)
top_cities_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC . Employee Data Management with Hierarchical Structure
# MAGIC You have an employees DataFrame with employee_id, name, manager_id, department_id, and salary.
# MAGIC Tasks:
# MAGIC Find the direct reports for each manager.
# MAGIC Calculate the total salary expenditure for each department.
# MAGIC Identify the top 3 highest-paid employees in each department using a window function.
# MAGIC For employees with a salary in the top 10% within their department, flag them as "high earner."

# COMMAND ----------

# Define schema for employees DataFrame
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

# Sample data for employees DataFrame
data = [
    (1, "Alice", None, 10, 90000.0),
    (2, "Bob", 1, 10, 60000.0),
    (3, "Carol", 1, 10, 70000.0),
    (4, "David", 2, 20, 50000.0),
    (5, "Eve", 2, 20, 55000.0),
    (6, "Frank", 3, 30, 85000.0),
    (7, "Grace", 3, 10, 75000.0),
    (8, "Heidi", 6, 30, 80000.0),
    (9, "Ivan", 7, 20, 52000.0),
    (10, "Judy", 5, 20, 53000.0)
]

# Create employees DataFrame
employees_df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# Define schema for employees DataFrame
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("salary", FloatType(), True)
])

# Sample data for employees DataFrame
data = [
    (1, "Alice", None, 10, 90000.0),
    (2, "Bob", 1, 10, 60000.0),
    (3, "Carol", 1, 10, 70000.0),
    (4, "David", 2, 20, 50000.0),
    (5, "Eve", 2, 20, 55000.0),
    (6, "Frank", 3, 30, 85000.0),
    (7, "Grace", 3, 10, 75000.0),
    (8, "Heidi", 6, 30, 80000.0),
    (9, "Ivan", 7, 20, 52000.0),
    (10, "Judy", 5, 20, 53000.0)
]

# Create employees DataFrame
managers_df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

#Find the direct reports for each manager.
direct_reports=employees_df.alias("emps").join(managers_df.alias("mangs"),col("emps.manager_id")==col("mangs.employee_id"),how="inner")\
    .select(
        col("mangs.employee_id").alias("manager_id"),
        col("mangs.name").alias("manager_name"),
        col("emps.employee_id").alias("employee_id"),
        col("emps.name").alias("employee_name")
    )
direct_reports.display()    

# COMMAND ----------

window_spec=Window.partitionBy("department_id")

# COMMAND ----------

#Calculate the total salary expenditure for each department.
employees_df_expenditure=employees_df.withColumn(
    "total_expenditure",
    sum("salary").over(window_spec)
)
employees_df_expenditure.display()

# COMMAND ----------

#Identify the top 3 highest-paid employees in each department using a window function.
window_earner_spec=Window.partitionBy("department_id").orderBy(col("salary").desc())

ranking_df=employees_df_expenditure.withColumn(
    "rank",
    rank().over(window_earner_spec)
)
#top_three_earners_df.display()
top_earners=ranking_df.filter(col("rank")<=3)
top_earners.display()

# COMMAND ----------

window_top_ten_spec=Window.partitionBy("department_id").orderBy("salary")

# COMMAND ----------

#For employees with a salary in the top 10% within their department, flag them as "high earner."
employees_with_percent_df=employees_df.withColumn(
    "percentile_rank",
    percent_rank().over(window_top_ten_spec)
)
employees_with_percent_df.display()

# COMMAND ----------

filtered_top_employees=employees_with_percent_df.filter(col("percentile_rank")>0.9)
filtered_top_employees.show()

# COMMAND ----------

top_10_percent_df=filtered_top_employees.withColumn(
    "higher_earner",
    when(col("percentile_rank")>0.9,"yes")
    .otherwise("no")
)
top_10_percent_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Data Analysis with Time-Based Aggregations
# MAGIC A sales DataFrame contains sale_id, product_id, sale_amount, sale_date, and region.
# MAGIC Another DataFrame, products, has product_id, product_name, and category.
# MAGIC Tasks:
# MAGIC Calculate monthly total and average sales for each product category.
# MAGIC Determine the region with the highest monthly sales and the highest number of sales transactions.
# MAGIC Use window functions to find the moving average of sales for each product over the past 3 months.

# COMMAND ----------

# Sample data for the sales DataFrame
sales_data = [
    (1, 101, 250.0, "2023-01-15", "North"),
    (2, 102, 300.0, "2023-01-18", "South"),
    (3, 103, 150.0, "2023-02-05", "East"),
    (4, 104, 400.0, "2023-02-15", "West"),
    (5, 101, 200.0, "2023-03-10", "North"),
    (6, 103, 350.0, "2023-03-20", "South"),
    (7, 104, 450.0, "2023-04-25", "East"),
    (8, 102, 250.0, "2023-04-28", "West"),
    (9, 101, 100.0, "2023-05-10", "North"),
    (10, 102, 200.0, "2023-05-15", "South"),
]

# Define schema for the sales DataFrame
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_amount", FloatType(), True),
    StructField("sale_date", StringType(), True),
    StructField("region", StringType(), True)
])

# Create the sales DataFrame
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Sample data for the products DataFrame
products_data = [
    (101, "Product_A", "Category_1"),
    (102, "Product_B", "Category_2"),
    (103, "Product_C", "Category_1"),
    (104, "Product_D", "Category_3")
]

# Define schema for the products DataFrame
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

# Create the products DataFrame
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Show sample data
sales_df.display()
products_df.display()

# COMMAND ----------

#Calculate monthly total and average sales for each product category.
joined_df=sales_df.join(products_df,on="product_id",how="inner")
df_with_month=joined_df.withColumn(
    "sale_month",month(sales_df["sale_date"])
)
df_with_month.display()

# COMMAND ----------

total_and_avg_sales_df=df_with_month.groupBy('sale_month',"product_id","category").agg(
    sum("sale_amount").alias("total_sales"),
    avg("sale_amount").alias("avg_sales")
)
total_and_avg_sales_df.display()

# COMMAND ----------

#Determine the region with the highest monthly sales and the highest number of sales transactions.
df_highest_sales_transactions=df_with_month.groupBy('sale_month','region').agg(
    count('*').alias("total_transactions"),
    sum("sale_amount").alias("total_sales")
)
df_highest_sales_transactions.orderBy(desc("total_sales")).display()

# COMMAND ----------

#Use window functions to find the moving average of sales for each product over the past 3 months.
window_spec_cumulative = Window.partitionBy("product_id").orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  # Calculate the cumulative average of sale
df_with_moving_avg=df_with_month.withColumn(
    "cumulative_average_sales",
     avg("sale_amount").over(window_spec_cumulative)
  )
  #display the result
df_with_moving_avg.display()





# COMMAND ----------

# MAGIC %md
# MAGIC  User Engagement Tracking
# MAGIC A user_sessions DataFrame contains user_id, session_id, login_time, and logout_time.
# MAGIC Tasks:
# MAGIC Calculate the session duration for each user session.
# MAGIC Find the total login duration per user over a month and identify the top 5 most active users.
# MAGIC Use a window function to r

# COMMAND ----------

# Sample data
data = [
    ("U1", "S1", "2024-10-01 09:00:00", "2024-10-01 10:30:00"),
    ("U1", "S2", "2024-10-02 11:00:00", "2024-10-02 12:15:00"),
    ("U2", "S3", "2024-10-03 13:00:00", "2024-10-03 14:45:00"),
    ("U3", "S4", "2024-10-04 15:00:00", "2024-10-04 17:00:00"),
    ("U3", "S5", "2024-10-05 09:00:00", "2024-10-05 11:30:00"),
    ("U4", "S6", "2024-10-06 10:00:00", "2024-10-06 12:00:00"),
    ("U5", "S7", "2024-10-07 12:00:00", "2024-10-07 14:30:00"),
    ("U2", "S8", "2024-10-08 08:00:00", "2024-10-08 09:30:00"),
    ("U5", "S9", "2024-10-09 09:30:00", "2024-10-09 10:30:00")
]

# Create DataFrame
columns = ["user_id", "session_id", "login_time", "logout_time"]
user_sessions = spark.createDataFrame(data, columns)
user_sessions.display()

# COMMAND ----------

#calculate the session duration for each user session.
session_duration_df=user_sessions.withColumn(
  "session_duration",
  (unix_timestamp("logout_time")-unix_timestamp("login_time"))/60
)
session_duration_df.display()

# COMMAND ----------

#Find the total login duration per user over a month and identify the top 5 most active users.
total_login_duration=session_duration_df.groupBy("user_id").agg(sum("session_duration").alias("total_login_duration"))
top_5_users=total_login_duration.orderBy(col("total_login_duration").desc()).limit(5)
top_5_users.display()

# COMMAND ----------

#Use a window function to rank users by weekly login duration and determine the "most active" user each week.
session_duration_df=session_duration_df.withColumn(
    "week",
    weekofyear("login_time")
)
session_duration_df.display()

# COMMAND ----------

weekly_user_duration=session_duration_df.groupBy("user_id","week").agg(
    sum("session_duration").alias("weekly_duration")
)
weekly_user_duration.display()

# COMMAND ----------

windowspec=Window.partitionBy("week").orderBy(col("weekly_duration").desc())
ranking_users=weekly_user_duration.withColumn(
    "rank",
    rank().over(windowspec)
)
ranking_users.display()

# COMMAND ----------

top_active_users_per_week=ranking_users.filter(col("rank")==1)
top_active_users_per_week.display()

# COMMAND ----------

# Show results
print("Session Durations:")
session_duration_df.select("user_id", "session_id", "session_duration").show()

print("Top 5 Most Active Users in the Month:")
top_5_users.show()

print("Most Active User Each Week:")
top_active_users_per_week.show()

# COMMAND ----------

# Sample data for the inventory DataFrame
inventory_data = [
    (101, "A", 50, 30, "2024-08-15"),
    (102, "B", 10, 20, "2024-09-05"),
    (103, "A", 5, 15, "2024-07-20"),
    (104, "C", 100, 50, "2024-10-10"),
    (105, "B", 20, 10, "2024-09-25")
]

# Define schema for inventory DataFrame
inventory_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("last_restock_date", StringType(), True)
])

# Create inventory DataFrame
inventory_df = spark.createDataFrame(inventory_data, schema=inventory_schema)

# Sample data for the orders DataFrame
orders_data = [
    (201, 101, 5, "2024-10-01"),
    (202, 102, 10, "2024-10-05"),
    (203, 103, 2, "2024-10-08"),
    (204, 104, 20, "2024-10-12"),
    (205, 105, 15, "2024-10-15"),
    (206, 101, 7, "2024-10-18")
]

# Define schema for orders DataFrame
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_quantity", IntegerType(), True),
    StructField("order_date", StringType(), True)
])

# Create orders DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Show the data
inventory_df.display()
orders_df.display()

# COMMAND ----------

joined_df=inventory_df.join(orders_df,on="product_id",how="inner")
joined_df.display()

# COMMAND ----------

#Calculate the current stock level for each product by subtracting order quantities.
stock_level_df=joined_df.withColumn(
    "stock_level",
    col("stock_quantity")-col("order_quantity")
)
stock_level_df.display()

# COMMAND ----------

#Identify products that need restocking (where stock_quantity falls below reorder_level).
restocking_needed_products=stock_level_df.withColumn(
    "restock",
    when(col("stock_quantity")<col("reorder_level"),"restock_needed")
    .otherwise("not_needed")
)
restocking_needed_products.display()

# COMMAND ----------

# Sample data representing historical restocks for each product
historical_inventory_data = [
    (101, "A", 50, 30, "2024-08-01"),
    (101, "A", 50, 30, "2024-08-20"),
    (101, "A", 50, 30, "2024-09-10"),
    (102, "B", 10, 20, "2024-07-15"),
    (102, "B", 10, 20, "2024-09-05"),
    (102, "B", 10, 20, "2024-10-05"),
    (103, "A", 5, 15, "2024-06-01"),
    (103, "A", 5, 15, "2024-07-20"),
    (103, "A", 5, 15, "2024-08-30")
]

# Define schema
inventory_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("last_restock_date", StringType(), True)
])

# Create DataFrame
historical_inventory_df = spark.createDataFrame(historical_inventory_data, schema=inventory_schema)
historical_inventory_df.display()

# COMMAND ----------

#Find the average restocking interval for each product based on last_restock_date and flag products with inconsistent restocking patterns.
window_spec=Window.partitionBy("product_id").orderBy("last_restock_date")

interval_df=historical_inventory_df.withColumn(
    "restock_interval",
     datediff(col("last_restock_date"),lag("last_restock_date",1).over(window_spec))
)
interval_df=interval_df.filter(col("restock_interval").isNotNull())
interval_df.display()

# COMMAND ----------

#calculating average and standard deviation for each product
interval_stats_df=interval_df.groupBy("product_id").agg(
    avg("restock_interval").alias("avg_restock_interval"),
    stddev("restock_interval").alias("stddev_interval")
)

# Flag products with high standard deviation in intervals
# Assuming a threshold for inconsistency, e.g., stddev > 50% of avg_interval
flagged_df = interval_stats_df.withColumn(
    "inconsistent_restock", 
    col("stddev_interval") > (0.5 * col("avg_restock_interval"))
)

# Show the final result
flagged_df.show()

# COMMAND ----------

"""
Scenario:
A JSON file containing customer order data is stored in Azure Data Lake Storage. The file structure is nested, with fields like OrderID, CustomerInfo (containing Name and Address), and Products (an array of objects with ProductID, ProductName, and Quantity).

Load the JSON data.
Flatten the nested structure so that each row corresponds to a single product in an order.
Save the flattened data as a CSV file in DBFS.
"""

# COMMAND ----------

import json
import os

# Define the flatten_json_simple function
def flatten_json_simple(nested_json, parent_key='', sep='_'):
    items = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json_simple(value, new_key, sep=sep))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    items.update(flatten_json_simple(item, f"{new_key}{sep}{index}", sep=sep))
                else:
                    items[f"{new_key}{sep}{index}"] = item
        else:
            items[new_key] = value
    return items

# Sample JSON data
json_data = [
    {
        "OrderID": "ORD001",
        "CustomerInfo": {
            "Name": "John Doe",
            "Address": "123 Elm Street, Springfield"
        },
        "Products": [
            {"ProductID": "P001", "ProductName": "Laptop", "Quantity": 1},
            {"ProductID": "P002", "ProductName": "Mouse", "Quantity": 2}
        ]
    },
    {
        "OrderID": "ORD002",
        "CustomerInfo": {
            "Name": "Jane Smith",
            "Address": "456 Oak Avenue, Shelbyville"
        },
        "Products": [
            {"ProductID": "P003", "ProductName": "Keyboard", "Quantity": 1},
            {"ProductID": "P004", "ProductName": "Monitor", "Quantity": 1},
            {"ProductID": "P005", "ProductName": "Desk Lamp", "Quantity": 2}
        ]
    }
]

# Flatten the JSON data using the function
flattened_data = []
for record in json_data:
    flattened_data.append(flatten_json_simple(record))

# Convert flattened data to Spark DataFrame
df = spark.createDataFrame(flattened_data)

# Show the flattened DataFrame
df.display()

