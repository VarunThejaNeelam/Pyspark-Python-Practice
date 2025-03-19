# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

data=[('Varun',25,25000),('lokesh',26,50000),('Venu',24,40000),('Suresh',27,50000)]
schema=StructType([
            StructField('name',StringType(),True),
            StructField('age',IntegerType(),True),
            StructField('salary',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_select=df.filter(df["age"]>25)
df_sorted=df_select.orderBy(col("age").desc())
df_sorted.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 2: Working with Built-in Functions
# MAGIC

# COMMAND ----------

data=[('Varun',30000,101),('Venu',40000,102),('Suresh',35000,101),('Lokesh',40000,102),('manoj',20000,103)]
schema=StructType([
          StructField('name',StringType(),True),
          StructField('salary',IntegerType(),True),
          StructField('dept_id',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# Increase each salary by 10%
df_updated = df.withColumn("new_salary", col("salary") * 1.10)

# Calculate the average salary for each dept_id
df_avg_sal = df_updated.groupBy("dept_id").agg(avg("salary").alias("avg_sal"))
df_avg_sal.show()

# Add a new column 'salary_category' categorizing salaries as 'High', 'Medium', or 'Low'
df_with_category = df_updated.withColumn(
    "salary_category",
    when((col("salary") >= 30000) & (col("salary") <= 35000), "Medium")
    .when(col("salary") >= 40000, "High")
    .otherwise("Low")
)

# Show the results
df_with_category.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Date functions

# COMMAND ----------

data=[('Varun',"1999-02-26"),('Venu',"1999-07-11"),('Suresh',"1998-07-26")]
schema=StructType([
          StructField('emp_name',StringType(),True),
          StructField('date_of_birth',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

#change dateofbirth column to date
df=df.withColumn("date_of_birth",col("date_of_birth").cast('date'))
#calculate age for each person in dataframe
df_with_age=df.withColumn("age",year(current_date())-year(col("date_of_birth")))
#Extract the column month from date of birth
df_with_month=df_with_age.withColumn("birth_month",month(col("date_of_birth")))
#Filter the rows which is matched to month july
df_month_filter=df_with_month.filter(col("birth_month")==7)
df_month_filter.show()


# COMMAND ----------

data=[('Development',1,25000),('HR',2,30000),('HR',3,40000),('Development',4,50000),("Design",5,60000)]
schema=StructType([
            StructField('department',StringType(),True),
            StructField('employee_id',IntegerType(),True),
            StructField('salary',IntegerType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_aggregated=df.groupBy('department').agg(
                sum("salary").alias('total_salary'),
                max("salary").alias('max_salary'),
                avg('salary').alias("avg_salary")
)
df_aggregated.display()

# COMMAND ----------

data=[(1,30000,'Dev'),(2,40000,'Design'),(3,35000,'Dev'),(4,40000,'HR'),(5,20000,'Design')]
schema=StructType([
          StructField('employee_id',IntegerType(),True),
          StructField('salary',IntegerType(),True),
          StructField('department',StringType(),True)
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

#Rank the employess with salary on each department
df_with_rank=df.select('employee_id','salary',rank().over(Window.partitionBy("department").orderBy(desc("salary"))).alias("rank"))
#Calculate cumulative salaries for each department
df_total_salary=df_with_rank.groupBy('department').agg(sum("salary").alias('total_salary'),avg("salary").alias('avg_salary')).select(df_with_rank.employee_id,df_with_rank.salary,df_with_rank.rank,"avg_salary")
#Difference b/w employee salary with department average salary
df_total_salary.withColumn(
    "difference_salary",
    col("salary")-col("avg_salary")
)

# COMMAND ----------

orders_data = [
    (1, 101, "2024-05-01", "completed", 500.0),
    (2, 102, "2024-04-15", "completed", 200.0),
    (3, 103, "2023-12-01", "pending", 300.0),
    (4, 104, "2024-06-10", "completed", 400.0),
    (5, 105, "2023-11-20", "cancelled", 150.0),
    (6, 101, "2024-05-20", "completed", 600.0),
    (7, 102, "2024-07-01", "completed", 800.0),
    (8, 106, "2024-07-15", "completed", 1000.0),
    (9, 107, "2023-08-25", "completed", 250.0),
    (10, 108, "2024-05-11", "completed", 350.0),
]

orders_columns = ["order_id", "customer_id", "order_date", "order_status", "total_amount"]

orders_df = spark.createDataFrame(orders_data, orders_columns)

# COMMAND ----------

customers_data = [
    (101, "Alice", "alice@example.com", "New York"),
    (102, "Bob", "bob@example.com", "Los Angeles"),
    (103, "Charlie", "charlie@example.com", "Chicago"),
    (104, "Diana", "diana@example.com", "New York"),
    (105, "Eve", "eve@example.com", "Houston"),
    (106, "Frank", "frank@example.com", "Phoenix"),
    (107, "Grace", "grace@example.com", "Chicago"),
    (108, "Hank", "hank@example.com", "Los Angeles"),
    (109, "Ivy", "ivy@example.com", "San Francisco"),
    (110, "Jack", "jack@example.com", "Seattle"),
]

customers_columns = ["customer_id", "customer_name", "customer_email", "customer_city"]

customers_df = spark.createDataFrame(customers_data, customers_columns)

# COMMAND ----------

joined_df=customers_df.join(orders_df,on="customer_id",how="left")
# Filter only "completed" orders and group by customer_city to get the total order amount
top_5_cities_highest_order_amount = (
    joined_df.filter(col("order_status") == "completed")
    .groupBy("customer_city")
    .agg(sum("total_amount").alias("total_order_amount"))
    .orderBy(col("total_order_amount").desc())  # Global sorting
    .limit(5)  # Top 5 cities
)


# COMMAND ----------

#Identify customers who have not placed any orders in the last 6 months.
# Convert order_date to date type if necessary
joined_df = joined_df.withColumn("order_date", col("order_date").cast("date"))

# Calculate customers who have not placed orders in the last 6 months
customers_inactivity_last_six_months = (
    joined_df.groupBy("customer_id", "customer_name")
    .agg(max("order_date").alias("max_order_date"))
    .filter(col("max_order_date") < add_months(current_date(), -6))  # 6 months from today
    .select("customer_id", "customer_name")
)
customers_inactivity_last_six_months.show()

# COMMAND ----------

joined_df=customers_df.join(orders_df,on="customer_id",how="left")
joined_df.show()

# COMMAND ----------

#Generate a report showing the top 3 customers (by total spending) for each city.
# Step 1: Calculate total spending per customer per city
windowspec1 = Window.partitionBy("customer_id", "customer_city")
spending_amount_per_customer = joined_df.groupBy("customer_id", "customer_name", "customer_city").agg(
    sum("total_amount").alias("total_spending")
)

windowspec2=Window.partitionBy("customer_city").orderBy(col("total_spending").desc())
ranked_customers=spending_amount_per_customer.withColumn(
    "rank",
    rank().over(windowspec2)
)
top3_customers_per_city=ranked_customers.filter((col("rank")<=3) & (col("total_spending")).isNotNull())
report=top3_customers_per_city.select("customer_id","customer_city","total_spending","rank")
report.display()


# COMMAND ----------

#Calculate the average order value per customer, including customers who have no orders.
# Group by customer_id and calculate total_order_amount and order_count
aggregated_df = (
    joined_df.groupBy("customer_id")
    .agg(
        sum("total_amount").alias("total_order_amount"),
        count("order_id").alias("order_count")
    )
    .withColumn(
        # Calculate avg_order_value and handle cases with no orders
        "avg_order_value",
        when(col("order_count") == 0, lit(0))  # Set avg_order_value to 0 if no orders
        .otherwise(col("total_order_amount") / col("order_count"))
    )
    .select("customer_id", "avg_order_value")
)

aggregated_df.show()


# COMMAND ----------

"""
Assignment 3: E-Commerce Product Recommendation
You are given three datasets:

Products:
product_id, category, price
Transactions:
transaction_id, product_id, customer_id, purchase_date
Customer Browsing History:
customer_id, product_id, browse_date
Tasks:

Identify products that are frequently purchased together (market basket analysis).
Find customers who browsed a product but didn’t purchase it within 7 days.
Create a report showing the most popular product categories for each month.
Generate personalized product recommendations for each customer based on purchase and browsing history."""

# COMMAND ----------

#Find customers who browsed a product but didn’t purchase it within 7 days.
# Browsing History Schema
browsing_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("browse_date", StringType(), True)
])

# Browsing History Data
browsing_data = [
    ("C001", "P101", "2023-09-01"),
    ("C002", "P102", "2023-09-03"),
    ("C003", "P104", "2023-09-05"),
    ("C001", "P105", "2023-09-08")
]

# Create DataFrame
browsing_df = spark.createDataFrame(browsing_data, schema=browsing_schema)
browsing_df.show()

# Transactions Schema
transactions_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("purchase_date", StringType(), True)
])

# Transactions Data
transactions_data = [
    ("C001", "P101", "2023-09-02"),
    ("C002", "P102", "2023-09-10"),
    ("C003", "P103", "2023-09-07")
]

# Create DataFrame
transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)
transactions_df.show()

# Products Schema
products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True)
])

# Products Data
products_data = [
    ("P101", "Electronics", "500"),
    ("P102", "Apparel", "40"),
    ("P103", "Books", "20"),
    ("P104", "Electronics", "800"),
    ("P105", "Home Appliances", "300")
]

# Create DataFrame
products_df = spark.createDataFrame(products_data, schema=products_schema)
products_df.show()

# COMMAND ----------

#joined both browsing and transactions dataframes
joined_df=browsing_df.join(transactions_df,on=["product_id", "customer_id"],how="left")
joined_df.show()

# COMMAND ----------

#creating a date diff column
joined_df=joined_df.withColumn("date_difference",datediff(col("purchase_date"),col("browse_date")))
joined_df.show()

# COMMAND ----------

#filtering data which is in between 7 days of purchase after browsing 
filtered_df=joined_df.filter(
    (col("date_difference").isNotNull()) & (col("date_difference")>7) |
    col("purchase_date").isNull()
).select("product_id","customer_id")
#displaying dataframe
filtered_df.show()

# COMMAND ----------

joined_df=browsing_df.join(transactions_df,on=["product_id","customer_id"],how="full_outer")
joined_df.display()

# COMMAND ----------

recommended_product_df=joined_df.withColumn(
    "product_recommendation",
    when((col("browse_date").isNotNull()) & (col("purchase_date").isNull()),col("product_id"))
    .when(col("purchase_date").isNotNull(),col("product_id")) 
)

#recommended_product_df.select("customer_id","product_recommendation").show()
# Aggregate Recommendations per Customer
final_recommendations_df = recommended_product_df.groupBy("customer_id").agg(
    collect_list("product_recommendation").alias("recommended_products")
)
final_recommendations_df.display()

# COMMAND ----------

"""
Customer Order Analysis
📌 Scenario: You have an e-commerce dataset with customer orders stored in JSON format. The dataset includes nested fields such as customer_info, order_details, and payment_info.

🔹 Task:

Extract customer details (customer ID, name, location).
Extract each item from the order_details array as a separate row (denormalization).
Find the total revenue for each customer and identify their most purchased product category.
Find customers who placed an order but never paid (payment_status = 'Failed').
Save the transformed data as Delta tables in the curated layer

# COMMAND ----------

#Sample data
json_data = [
    {
        "customer_info": {"customer_id": 1, "name": "John Doe", "location": "New York"},
        "order_details": [
            {"product_id": "P101", "product_name": "Laptop", "category": "Electronics", "price": 1200, "quantity": 1},
            {"product_id": "P102", "product_name": "Mouse", "category": "Accessories", "price": 25, "quantity": 2}
        ],
        "payment_info": {"payment_status": "Completed", "total_amount": 1250}
    },
    {
        "customer_info": {"customer_id": 2, "name": "Jane Smith", "location": "California"},
        "order_details": [
            {"product_id": "P103", "product_name": "Smartphone", "category": "Electronics", "price": 800, "quantity": 1},
            {"product_id": "P104", "product_name": "Headphones", "category": "Accessories", "price": 100, "quantity": 1}
        ],
        "payment_info": {"payment_status": "Completed", "total_amount": 900}
    },
    {
        "customer_info": {"customer_id": 3, "name": "Alice Johnson", "location": "Texas"},
        "order_details": [
            {"product_id": "P105", "product_name": "Keyboard", "category": "Accessories", "price": 50, "quantity": 1}
        ],
        "payment_info": {"payment_status": "Failed", "total_amount": 50}
    }
]
#schema for json data
json_schema = StructType([
    StructField("customer_info",StructType([
        StructField("customer_id",IntegerType(),True),
        StructField("name",StringType(),True),
        StructField("location",StringType(),True)
    ]),True),
    StructField("order_details",ArrayType(StructType([
        StructField("product_id",StringType(),True),
        StructField("product_name",StringType(),True),
        StructField("category",StringType(),True),
        StructField("price",IntegerType(),True),
        StructField("quantity",IntegerType(),True)
    ])),True),
    StructField("payment_info",StructType([
        StructField("payment_status",StringType(),True),
        StructField("total_amount",IntegerType(),True)
    ]),True)
])
# Create DataFrame
df = spark.createDataFrame(json_data,json_schema)

# Explode order_details while keeping payment_info
df_exploded = df.select(
    col("customer_info.customer_id").alias("customer_id"),
    col("customer_info.name").alias("name"),
    col("customer_info.location").alias("location"),
    col("payment_info.payment_status").alias("payment_status"),
    col("payment_info.total_amount").alias("total_amount"),
    explode(col("order_details")).alias("orders")
)

# Flatten the exploded order_details column
flattened_df = df_exploded.select(
    col("customer_id"),
    col("name"),
    col("location"),
    col("payment_status"),
    col("total_amount"),
    col("orders").getItem("product_id").alias("product_id"),
    col("orders").getItem("product_name").alias("product_name"),
    col("orders").getItem("category").alias("category"),
    col("orders").getItem("price").alias("price"),
    col("orders").getItem("quantity").alias("quantity"),
)
flattened_df.display()

#window specification for total revenue
window_spec=Window.partitionBy("customer_id","category")
total_revenue_per_category=flattened_df.withColumn(
    "total_revenue",
    sum(col("price") * col("quantity")).over(window_spec)
    )

#window spec for rank  category
window_sp=Window.partitionBy("customer_id").orderBy(col("total_revenue").desc())
ranked_df=total_revenue_per_category.withColumn(
    "most_purchased_rank",
    rank().over(window_sp)
    )
most_purchased_category_df = ranked_df.filter(
    col("most_purchased_rank") == 1
)
most_purchased_category_df.display()

#Find customers who placed an order but never paid (payment_status = 'Failed').
filtered_df=flattened_df.filter(
    (col("product_id").isNotNull()) & (col("payment_status") == "Failed")
)
filtered_df.show()

# COMMAND ----------

