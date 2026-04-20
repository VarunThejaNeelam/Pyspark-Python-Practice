# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

transaction_type_data = [
    ("T001", "Payment", "Money transfer"),
    ("T002", "Withdrawal", "Cash withdrawal"),
    ("T003", "Deposit", "Cash deposit"),
    ("T004", "Loan Repayment", "Loan EMI payment"),
    ("T005", "Purchase", "Point-of-sale purchase"),
    ("T006", "Refund", "Transaction refund"),
    ("T007", "Fee", "Service fee deduction"),
    ("T008", "Interest", "Interest credited"),
    ("T009", "Transfer", "Account transfer"),
    ("T010", "Reversal", "Transaction reversal")
]

transaction_type_schema = StructType([
    StructField("Transaction_Type_ID", StringType()),
    StructField("Transaction_Type_Name", StringType()),
    StructField("Description", StringType())
])

transaction_type_dim_df = spark.createDataFrame(transaction_type_data, schema=transaction_type_schema)

agg_df = transaction_type_dim_df.groupBy("Transaction_Type_Name").agg(
    countDistinct("*").alias("total_count")
)

agg_df.show()

# COMMAND ----------

df = spark.read.format('parquet').load("dbfs:/FileStore/tables/delta/_delta_log/00000000000000000010.checkpoint.parquet")
df.display()

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



# Sample transactions data
transactions_data = [
    ("txn1", "region1", 100.0),
    ("txn2", "region2", 150.0),
    ("txn3", "region1", 200.0),
    ("txn4", "region3", 50.0),
    ("txn5", "region2", 300.0),
    ("txn6", "region1", 120.0),
]

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("amount", DoubleType(), True),
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

transactions_df.show()


# Sample regions data
regions_data = [
    ("region1", "North"),
    ("region2", "South"),
    ("region3", "East"),
    ("region4", "West"),
]

regions_schema = StructType([
    StructField("region_id", StringType(), True),
    StructField("region_name", StringType(), True),
])

regions_df = spark.createDataFrame(regions_data, schema=regions_schema)

regions_df.show()


# COMMAND ----------

# Applying broadcast join 
joined_df = transactions_df.join(broadcast(regions_df), on="region_id") 

# Aggregating total transaction amount for regions
aggregated_df = joined_df.groupBy("region_id", "region_name").agg(
    sum("amount").alias("total_transaction_amount")
)

# Ranking regions based on total transaction amount
window_rank = Window.orderBy(desc("total_transaction_amount"))
ranking_df = aggregated_df.withColumn(
    "rank",
    rank().over(window_rank)
)
top2_regions_df = ranking_df.filter(col("rank") <= 2)
top2_regions_df.show()

# COMMAND ----------



schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("month", StringType(), True)  # We'll use string for simplicity like '2025-01'
])

data = [
    ("E01", "HR",     3000, "2025-01"),
    ("E01", "HR",     3200, "2025-02"),
    ("E01", "HR",     3100, "2025-03"),

    ("E02", "Finance", 4000, "2025-01"),
    ("E02", "Finance", 4200, "2025-02"),

    ("E03", "HR",     2800, "2025-01"),
    ("E03", "HR",     2900, "2025-02"),

    ("E04", "Finance", 4500, "2025-01"),
    ("E04", "Finance", 4600, "2025-03"),

    ("E05", "IT",     5000, "2025-01"),
    ("E05", "IT",     5200, "2025-02"),
    ("E05", "IT",     5300, "2025-03")
]

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

# Calculate running total of salary across months

# window specification for cumulative salary for employees
window = Window.partitionBy("emp_id").orderBy("month")

# Aggregating cumulative salary for each employee
cumu_sal_df = df.withColumn(
    "cumulative_sal",
    sum("salary").over(window)
)
cumu_sal_df.show()

cumu_sal_df.explain(True)


# COMMAND ----------

"""
For each department:
Calculate cumulative salary payout per month
Highest earning employee so far (until current month)""
"""

# Convert month to proper date for accurate ordering
df = df.withColumn("month", to_date(col("month"), "yyyy-MM"))

# Window specification for cumulative salary for each department
window_spec = Window.partitionBy("department").orderBy("month")

dept_cum_df = df.withColumn(
    "dept_cum_sal",
    sum("salary").over(window_spec)
)
dept_cum_df.show()

# COMMAND ----------

# Cumulative highest salary so far (within department) by month
max_salary_window = Window.partitionBy("department").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

salary_tracking = df.withColumn(
    "max_sal_so_far",
    max("salary").over(max_salary_window)
)

# Join with original df to get employee details matching max salary at each step
final_df = salary_tracking.filter(col("salary") == col("max_sal_so_far"))
final_df.show()

# COMMAND ----------

"""
Product Lifecycle Analysis
Dataset:
product_events: product_id, event_type (launched, sold, returned, discontinued), event_time

Tasks:
For each product, extract its full lifecycle:
Launch date
Time to first sale
Return rate (returns/sales)
Time from launch to discontinuation
Flag products discontinued within 30 days
"""

# COMMAND ----------

# ─── 1. Sample data with timestamps as strings ───
data = [
    ("P1", "launched",     "2023-01-01 10:00:00"),
    ("P1", "sold",         "2023-01-03 12:00:00"),
    ("P1", "returned",     "2023-01-10 09:00:00"),
    ("P1", "sold",         "2023-01-15 14:00:00"),
    ("P1", "discontinued", "2023-03-01 08:00:00"),

    ("P2", "launched",     "2023-02-05 11:00:00"),
    ("P2", "sold",         "2023-02-06 10:00:00"),
    ("P2", "sold",         "2023-02-07 10:00:00"),
    ("P2", "returned",     "2023-02-08 09:00:00"),
    ("P2", "discontinued", "2023-02-25 17:00:00"),

    ("P3", "launched",     "2023-03-10 09:00:00"),
    ("P3", "discontinued", "2023-03-12 09:00:00"),
]

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True)   # ← initially a STRING
])

df_raw = spark.createDataFrame(data, schema)

# ─── 2. Convert the string column to TimestampType ───
df = df_raw.withColumn(
    "event_time",
    to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")
)

# (Optional) Verify the result
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

class ProductLifeCycle:
    def read_data(self, path):
        try:
            df = spark.read.format('csv').option('header', True).load(path)
            return df
        except Exception as err:
            print('Error occured while reading: ', err)
            return None
        
    def productAnalysis(self, df):
        # Launch Date
        # Filtering launched details for products    
        product_launches_df = df.filter(col("event_type") == "launched")

        # Time to first sale
        # Filtering sales records for products
        product_sales_df = df.filter(col("event_type") == "sold")
        # Making windows for product sales
        window_spec = Window.partitionBy("product_id").orderBy("event_time")
        product_sales_order_df = product_sales_df.withColumn("rn", row_number().over(window_spec))
        # Filtering first ale for products
        first_sales_df = product_sales_order_df.filter(col("rn") == 1)
        # Joining with product launches
        product_with_sales_launches_df = product_launches_df.alias("df1").join(first_sales_df.alias("df2"), on="product_id", how="inner")
        product_req_det_df = product_with_sales_launches_df.select(
            col("df1.product_id").alias("product_id"),
            col("df1.event_type").alias("event_type"),
            col("df1.event_time").alias("launched_time"),
            col("df2.event_time").alias("sold_time")
        )
        # Checking the difference between launched date and sold date
        sold_time_diff_df = product_req_det_df.withColumn(
            "day_diff",
            datediff(col("sold_time"), col("launched_time"))
        )

        # Return rate (returns/sales)
        # Aggregating total sales and returns for products
        products_aggregated_df = df.groupBy("product_id").agg(
            count(when(col("event_type") == "sold", True)).alias("total_sales"),
            count(when(col("event_type") == "returned", True)).alias("total_returns")
        )
        # Return rate for products
        product_return_rate_df = products_aggregated_df.withColumn(
            "return_rate",
            (col("total_returns") / col("total_sales"))
        )

        # Time from launch to discontinuation
        # Filtering discontinued events for products 
        products_disc_df = df.filter(col("event_type") == "discontinued")
        # Joining with launched dataframe 
        products_with_launches_disc_df = product_launches_df.alias("df1").join(products_disc_df.alias("df2"), on="product_id", how="inner")
        updated_products_with_laun_df = products_with_launches_disc_df.select(
            col("df1.product_id").alias("product_id"),
            col("df1.event_type").alias("event_type"),
            col("df1.event_time").alias("launched_time"),
            col("df2.event_time").alias("discontinued_time")
        )
        # Checking the difference between launched date and discontinued date
        discontinued_diff_df = updated_products_with_laun_df.withColumn(
            "day_diff",
            datediff(col("discontinued_time"), col("launched_time"))
        )
        # Flaging products where difference is within 30 days
        flaging_df = discontinued_diff_df.withColumn(
            "is_discontinued_within_30days",
            when(col("day_diff") <= 30, 'Yes').otherwise('No')
        )

        return product_launches_df, sold_time_diff_df, product_return_rate_df, flaging_df

# COMMAND ----------

"""
Customer Churn Risk Scoring
Dataset:
transactions: customer_id, amount, transaction_date

logins: customer_id, login_date

Tasks:
For each customer:
Avg spend per month
Days since last login
Churn score = low spend + inactivity
Flag high-risk customers (churn score > threshold)
"""

# COMMAND ----------

# ----------------------------
# Transactions Data
# ----------------------------

transactions_data = [
    ("C001", 120.50, "2024-11-10"),
    ("C002", 89.00,  "2024-12-01"),
    ("C001", 75.00,  "2024-12-20"),
    ("C003", 45.25,  "2025-01-15"),
    ("C004", 200.00, "2025-03-05"),
    ("C002", 60.00,  "2025-02-10"),
    ("C005", 20.00,  "2025-01-30")
]

transactions_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

# Convert string to date
transactions_df = transactions_df.withColumn("transaction_date", to_date("transaction_date", "yyyy-MM-dd"))

transactions_df.show()
#transactions_df.printSchema()

# ----------------------------
# Logins Data
# ----------------------------

logins_data = [
    ("C001", "2025-05-10"),
    ("C002", "2025-03-01"),
    ("C003", "2025-02-01"),
    ("C004", "2024-12-31"),
    ("C005", "2025-05-01")
]

logins_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("login_date", StringType(), True)
])

logins_df = spark.createDataFrame(logins_data, schema=logins_schema)

# Convert string to date
logins_df = logins_df.withColumn("login_date", to_date("login_date", "yyyy-MM-dd"))

logins_df.show()
#logins_df.printSchema()



# COMMAND ----------

# Avg spend per month
# Aggregating total spend over each month
transactions_df = transactions_df.withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
monthly_amount_df = transactions_df.groupBy("customer_id", "year_month").agg(
    sum("amount").alias("monthly_total")
)
# Aggregating average monthly spend for customers based on these monthly total
customers_avg_spending_df = monthly_amount_df.groupBy("customer_id").agg(
    avg("monthly_total").alias("avg_spending")
)
customers_avg_spending_df.show()

# Days since last login
customers_last_login_df = logins_df.groupBy("customer_id").agg(
    max("login_date").alias("last_login")
)

# Days difference from last login to present day
cust_lastlogin_diff_df = customers_last_login_df.withColumn("day_diff", datediff(current_date(), col("last_login")))
cust_lastlogin_diff_df.show()

# Churn score = low spend + inactivity
# Aggregating overall avgerage spend
overall_avg_spend_df = customers_avg_spending_df.agg(
    avg("avg_spending").alias("overall_avg_spending")
)
# Applying cross join overall avg spending for all customers
joined_df = customers_avg_spending_df.crossJoin(overall_avg_spend_df)
# Join with customers last login 
customers_with_avg_last_login_df = joined_df.join(cust_lastlogin_diff_df, on="customer_id", how="inner")
# Flaging customers with churn score
flaging_df = customers_with_avg_last_login_df.withColumn(
    "churn_score",
    when((col("avg_spending") < col("overall_avg_spending")) & (col("day_diff") > 90), 3)
    .when((col("avg_spending") < col("overall_avg_spending")) & (col("day_diff") > 60), 2)
    .otherwise(1)
)
flaging_df = flaging_df.withColumn(
    "high_risk_flag",
    when(col("churn_score") >= 2, lit("YES")).otherwise(lit("NO"))
)
flaging_df.show()

# COMMAND ----------

"""
Delivery SLA Compliance Tracker
Dataset:
orders: order_id, customer_id, dispatch_time, delivery_time, promised_delivery_date

For each order:
 --Calculate delivery duration
 --Flag late deliveries

For each customer:
 -- % of late deliveries
 --Avg delivery time
 """

# COMMAND ----------

data = [
    ("O1", "C1", "2024-01-01 10:00:00", "2024-01-02 12:00:00", "2024-01-02 11:00:00"),
    ("O2", "C1", "2024-01-05 14:00:00", "2024-01-07 10:00:00", "2024-01-06 12:00:00"),
    ("O3", "C2", "2024-02-01 09:00:00", "2024-02-01 18:00:00", "2024-02-01 20:00:00"),
    ("O4", "C2", "2024-02-03 08:00:00", "2024-02-04 07:00:00", "2024-02-03 20:00:00"),
    ("O5", "C3", "2024-02-05 12:00:00", "2024-02-07 13:00:00", "2024-02-08 15:00:00"),
    ("O6", "C3", "2024-02-10 09:30:00", "2024-02-10 10:30:00", "2024-02-10 11:00:00"),
]

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("dispatch_time", StringType(), True),
    StructField("delivery_time", StringType(), True),
    StructField("promised_delivery_date", StringType(), True),
])

orders_df = spark.createDataFrame(data, schema)
orders_df = orders_df.withColumn(
    "dispatch_time", 
    to_timestamp("dispatch_time")
).withColumn(
    "delivery_time", 
    to_timestamp("delivery_time")
).withColumn(
    "promised_delivery_date", 
    to_timestamp("promised_delivery_date")
)
orders_df.show(truncate=False)



# COMMAND ----------

class DeliverySLAComplianceTracker:
    def __init__(self, path):
        self.path = path

    def read_data(self):
        try:
            df = spark.read.format('csv').option('header', True).load(self.path)
            return df
        except Exception as e:
            print(f"Error while reading file: {e}")
            return None

    def ordersTracking(self, orders_df):
        orders_df = orders_df.withColumn("dispatch_time", to_timestamp("dispatch_time")) \
            .withColumn("delivery_time", to_timestamp("delivery_time")) \
            .withColumn("promised_delivery_date", to_timestamp("promised_delivery_date"))

        # For each order Calculate delivery duration
        # Using unix_timestamp between delivery and dispatch timestamps to find the duration
        orders_with_duration_df = orders_df.withColumn(
            "duration",
            (unix_timestamp(col("delivery_time")) - unix_timestamp(col("dispatch_time"))) / 3600
        )
        # Flag late deliveries
        orders_with_duration_late_deliveries_df = orders_with_duration_df.withColumn(
            "is_late",
            when(col("delivery_time") > col("promised_delivery_date"), 1).otherwise(0)
        )
        
        # For each customer % of late deliveries
        # Aggregating total orders and late orders per customer
        aggregated_df = orders_with_duration_late_deliveries_df.groupBy("customer_id").agg(
            count("order_id").alias("total_orders"),
            count(when(col("is_late") == 1, True)).alias("late_orders")
        ) 
        df_with_late_per_df = aggregated_df.withColumn(
            "percentage",
            (col("late_orders") / col("total_orders")) * 100
        )

        # Avg delivery time 
        # Aggregating average delivery time for each customer
        filtered_df =  orders_with_duration_late_deliveries_df.filter(col("is_late") == 1)
        avg_delivery_time_df = filtered_df.groupBy("customer_id").agg(
            avg("duration").alias("avg_delivery_time")
        ) 

        return orders_with_duration_late_deliveries_df, df_with_late_per_df, avg_delivery_time_df
    
# Instantiating the class by passing path    
orders_track_inst = DeliverySLAComplianceTracker("dbfs://FileStore/raw/orders")

# Reading the orders dataframe
orders_df = orders_track_inst.read_data()

# Reading the orders tracking method 
orders_with_flags_df, late_percentage_df, avg_delivery_df = orders_track_inst.ordersTracking(orders_df)

orders_with_flags_df.show()
late_percentage_df.show()
avg_delivery_df.show()

# COMMAND ----------

"""
Sequential Process Flow Gaps
Dataset:
process_logs: job_id, step_name, start_time, end_time

Tasks:
For each job:
Order steps by start time
Calculate time gap between end of one step and start of next
Flag jobs with gaps > 15 min
"""

# COMMAND ----------

data = [
    ("job_1", "step_1", "2025-06-01 10:00:00", "2025-06-01 10:30:00"),
    ("job_1", "step_2", "2025-06-01 10:45:00", "2025-06-01 11:15:00"),
    ("job_1", "step_3", "2025-06-01 11:16:00", "2025-06-01 11:45:00"),

    ("job_2", "step_1", "2025-06-02 09:00:00", "2025-06-02 09:20:00"),
    ("job_2", "step_2", "2025-06-02 09:50:00", "2025-06-02 10:10:00"),
]

schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("step_name", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
])

df = spark.createDataFrame(data, schema)

df = df.withColumn("start_time", to_timestamp(col("start_time"))) \
       .withColumn("end_time", to_timestamp(col("end_time")))

df.show()       

# COMMAND ----------

class SequentialProcessFlowGaps:
    def __init__(self, path):
        self.path = path

    def read_data(self):
        try:
            df = spark.read.format('csv').option('header', True).option('inferschema', True).load(self.path)
            return df
        except Exception as e:
            print(f"Error while file reading {e}")
            return None

    def sequentialProcess(self, df):
        # Window specification to get previous end_times
        window_spec = Window.partitionBy("job_id").orderBy("start_time")
        df_with_prev_end = df.withColumn(
            "prev_end",
            lag("end_time").over(window_spec)
        )
        # Checking the difference between current start and prev end 
        diff_df = df_with_prev_end.withColumn(
            "difference",
            (unix_timestamp(col("start_time")) - unix_timestamp(col("prev_end"))) / 60
        )
        # Flaging jobs if difference is > 15 minutes
        flaging_df = diff_df.withColumn(
            "is_delayed",
            when(col("difference") > 15, 'Yes').otherwise('No')
        )
        return flaging_df
    
seq_inst = SequentialProcessFlowGaps("dbfs://Filestore/raw/jobs")

jobs_df = seq_inst.read_data()

flaging_df = seq_inst.sequentialProcess(jobs_df)

flaging_df.show()


# COMMAND ----------

"""
Region-Based Product Demand Spike Detector
Dataset:
sales: product_id, region, units_sold, sale_date

Tasks:
Rolling 7-day sales for each product in each region
Detect days where spike > 50% compared to prior 7-day average
"""

# COMMAND ----------

data = [
    ("P1", "North", 100, "2025-05-01"),
    ("P1", "North", 120, "2025-05-02"),
    ("P1", "North", 110, "2025-05-03"),
    ("P1", "North", 130, "2025-05-04"),
    ("P1", "North", 125, "2025-05-05"),
    ("P1", "North", 140, "2025-05-06"),
    ("P1", "North", 150, "2025-05-07"),
    ("P1", "North", 240, "2025-05-08"),  # spike expected

    ("P2", "South", 200, "2025-05-01"),
    ("P2", "South", 210, "2025-05-02"),
    ("P2", "South", 205, "2025-05-03"),
    ("P2", "South", 190, "2025-05-04"),
    ("P2", "South", 220, "2025-05-05"),
    ("P2", "South", 215, "2025-05-06"),
    ("P2", "South", 230, "2025-05-07"),
    ("P2", "South", 235, "2025-05-08"),  # no spike
]


schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("units_sold", IntegerType(), True),
    StructField("sale_date", StringType(), True)
])

products_df = spark.createDataFrame(data, schema=schema)
products_df.show()

# COMMAND ----------

# Convert date string to timestamp
products_df = products_df.withColumn("sale_date", to_date("sale_date"))

# Create a numeric column for ordering
products_df = products_df.withColumn("sale_ts", unix_timestamp("sale_date"))  # this is BIGINT (seconds)

products_df.select("sale_date", "sale_ts").show(20, False)

# Define window using the BIGINT column
window_spec = Window.partitionBy("region", "product_id") \
                    .orderBy("sale_ts") \
                    .rangeBetween(-7 * 86400, 0)  # last 7 days including today

# Compute rolling 7-day sum
sales_df = products_df.withColumn("rolling_sales", sum("units_sold").over(window_spec))

sales_df.select("product_id", "region", "sale_date", "units_sold", "rolling_sales").show()


# COMMAND ----------

# Detect days where spike > 50% compared to prior 7-day average
# Define window using the BIGINT column
window_spec = Window.partitionBy("region", "product_id") \
                    .orderBy("sale_ts") \
                    .rangeBetween(-7 * 86400, -1)  
products_avg_df = products_df.withColumn(
    "rolling_avg",
    avg("units_sold").over(window_spec)
)
# Filtering the days where spike > 50 % of avg sales
filtered_df = products_avg_df.filter(col("units_sold") > 1.5 * col("rolling_avg"))
filtered_df.show()

# COMMAND ----------

sales_df.explain(True)

# COMMAND ----------

"""
Warehouse Inventory Reconciliation
Datasets:
inventory_actual: product_id, warehouse, stock_count
inventory_system: product_id, warehouse, system_count

Tasks:
Join and detect mismatches
Flag warehouses with consistent >10% discrepancy
For each product, calculate total variance across warehouses
"""

# COMMAND ----------

# Schema for both tables
schema_actual = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("warehouse", StringType(), True),
    StructField("stock_count", IntegerType(), True)
])

schema_system = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("warehouse", StringType(), True),
    StructField("system_count", IntegerType(), True)
])

# Sample data for actual inventory
data_actual = [
    (101, "WH1", 500),
    (101, "WH2", 480),
    (102, "WH1", 200),
    (102, "WH2", 180),
    (103, "WH1", 600),
    (103, "WH2", 590)
]

# Sample data for system inventory
data_system = [
    (101, "WH1", 520),
    (101, "WH2", 450),
    (102, "WH1", 210),
    (102, "WH2", 150),
    (103, "WH1", 600),
    (103, "WH2", 600)
]

# Create DataFrames
inventory_actual = spark.createDataFrame(data_actual, schema_actual)
inventory_system = spark.createDataFrame(data_system, schema_system)

inventory_actual.show()
inventory_system.show()


# COMMAND ----------

# Join and detect mismatches
joined_df = inventory_actual.join(inventory_system, on=["product_id", "warehouse"], how="inner")

# Flaging mismatches by using when otherwise conndition
flaging_stocks_df = joined_df.withColumn(
    "is_mismatched",
    when(col("stock_count") != col("system_count"), 'Yes').otherwise('No')
)
flaging_stocks_df.show()

# COMMAND ----------

# Flag warehouses with consistent >10% discrepancy
discrepencies_df = joined_df.withColumn(
    "discrepency",
    ((col("stock_count") - col("system_count")) / col("system_count")) * 100
)
# Checking consistent discrepencies for warehouses
discrepencies_df = discrepencies_df.withColumn(
    "streaks",
    when(col("discrepency") > 10, 1).otherwise(0)
)
# Aggregating total discrepencies for warehouses
aggregated_df = discrepencies_df.groupBy("warehouse").agg(
    sum(col("streaks")).alias("total_discrepencies")
)
# Flaging warehouses
flaging_warehouses_df = aggregated_df.withColumn(
    "is_10%discrepencies",
    when(col("total_discrepencies") > 1, 'Yes').otherwise('No')
)
flaging_warehouses_df.show()

# COMMAND ----------

# For each product, calculate total variance across warehouses
product_analysis_df = inventory_actual.groupBy("product_id").agg(
    max("stock_count").alias("max_stocks"),
    min("stock_count").alias("min_stocks")
)
# Product total variance
product_variance_df = product_analysis_df.withColumn(
    "variance",
    col("max_stocks") - col("min_stocks")
)
product_variance_df.show()

# COMMAND ----------

"""
Behavioral Anomaly Detection in Event Logs
Dataset:
user_events: user_id, event_type, event_time

Tasks:
For each user:
Detect abnormal burst of actions (e.g., 5+ events in 2 minutes)
Flag if burst patterns repeat across days
List top 10 anomalous users
"""

# COMMAND ----------

# Define schema with event_time as string initially
schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_time_str", StringType(), nullable=False)
])

# Sample data with event_time as string
data = [
    (1, "click", "2025-06-01 10:00:00"),
    (1, "click", "2025-06-01 10:00:20"),
    (1, "click", "2025-06-01 10:01:00"),
    (1, "click", "2025-06-01 10:01:30"),
    (1, "click", "2025-06-01 10:01:50"),  # burst 5 in 2 mins
    (1, "click", "2025-06-02 11:15:00"),
    (1, "click", "2025-06-02 11:16:00"),
    (1, "click", "2025-06-02 11:16:30"),
    (1, "click", "2025-06-02 11:17:00"),
    (1, "click", "2025-06-02 11:17:30"),  # repeated burst next day
    (2, "view", "2025-06-01 09:00:00"),
    (2, "click", "2025-06-01 09:30:00"),
    (3, "click", "2025-06-01 14:00:00"),
    (3, "click", "2025-06-01 14:01:00"),
    (3, "click", "2025-06-01 14:02:00"),
    (3, "click", "2025-06-01 14:03:00"),
    (3, "click", "2025-06-01 14:04:00"),  # 5 events over 4+ mins
    (4, "click", "2025-06-01 15:00:00"),
    (4, "click", "2025-06-01 15:00:10"),
    (4, "click", "2025-06-01 15:00:20"),
    (4, "click", "2025-06-01 15:00:30"),
    (4, "click", "2025-06-01 15:00:40"),  # tight burst
    (5, "click", "2025-06-03 08:00:00"),
    (5, "click", "2025-06-03 08:00:10"),
    (5, "click", "2025-06-03 08:01:00"),
    (5, "click", "2025-06-03 08:01:30"),
    (5, "click", "2025-06-03 08:01:40"),
    (5, "click", "2025-06-04 08:00:00"),  # only one event next day
]

# Create DataFrame with string timestamp column
df = spark.createDataFrame(data, schema)

# Convert event_time_str to actual timestamp type in a new column event_time
df = df.withColumn("event_time", to_timestamp("event_time_str", "yyyy-MM-dd HH:mm:ss")) \
       .drop("event_time_str")

df.show(truncate=False)

# COMMAND ----------

# Detect abnormal burst of actions (e.g., 5+ events in 2 minutes)
# Converting timestamp to long for rangebetween
df = df.withColumn("event_ts", unix_timestamp(col("event_time")))

# Creating date column from event_time
df = df.withColumn("event_date", to_date("event_time"))

# window specification for burst events on 2 minutes
window_spec = Window.partitionBy("user_id", "event_type", "event_date").orderBy("event_ts").rangeBetween(-2 * 60, 0)

# Aggregating burst events for each user on events
bursts_df = df.withColumn(
    "burst_count",
    count("*").over(window_spec)
)

# Filtering users with bursts greaterr than or equals to 5
filtered_df = bursts_df.filter(col("burst_count") >= 5)
filtered_df.show()

# Flag if burst patterns repeat across days
# Filtering bursts across days from bursts dataframe
bursts_days_df = bursts_df.filter(col("burst_count") >= 5)

# Counting burst days for users
total_burst_days_df = bursts_days_df.groupBy("user_id").agg(
    countDistinct("event_date").alias("total_days")
)
total_burst_days_df.show()

# List top 10 anomalous users
window_rank = Window.orderBy(desc("total_days"))

ranking_users_df = total_burst_days_df.withColumn(
    "rnk",
    dense_rank().over(window_rank)
)

top10_anomalies_users_df = ranking_users_df.filter(col("rnk") <= 10)
top10_anomalies_users_df.show()

# COMMAND ----------

# Flag if burst patterns repeat across days
# Filtering bursts across days from bursts dataframe
bursts_days_df = bursts_df.filter(col("burst_count") == 3)

# Counting burst days for users
total_burst_days_df = bursts_days_df.groupBy("user_id").agg(
    countDistinct("event_date").alias("total_days")
)
total_burst_days_df.show()

# List top 10 anomalous users
window_rank = Window.orderBy(desc("total_days"))

ranking_users_df = total_burst_days_df.withColumn(
    "rnk",
    dense_rank().over(window_rank)
)

top10_anomalies_users_df = ranking_users_df.filter(col("rnk") <= 10)
top10_anomalies_users_df.show()

# COMMAND ----------

"""
Customer Lifetime Value (CLTV) Calculation
Dataset:
orders: order_id, customer_id, amount, order_date

Tasks:
For each customer:

Calculate average purchase value
Purchase frequency per month
CLTV = (avg value × freq per month) × avg customer lifespan (assume 24 months)
Segment customers: High, Medium, Low CLTV
"""

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True)  # initially string
])

data = [
    (1, 101, 120.0, "2023-01-15"),
    (2, 101, 130.0, "2023-02-20"),
    (3, 102, 80.0, "2023-01-10"),
    (4, 103, 200.0, "2023-03-05"),
    (5, 101, 150.0, "2023-03-25"),
    (6, 102, 100.0, "2023-02-14"),
    (7, 104, 90.0, "2023-03-15"),
    (8, 104, 95.0, "2023-03-20"),
    (9, 105, 300.0, "2023-01-05"),
    (10, 105, 250.0, "2023-02-10"),
]

orders_df = spark.createDataFrame(data, schema)

# Convert order_date from string to date type
orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

orders_df.show()

# COMMAND ----------

# Calculate average purchase value
cust_avg_val_df = orders_df.groupBy("customer_id").agg(
    avg("amount").alias("avg_purchase_value")
)
# cust_avg_val_df.show()

# Purchase frequency per month
# Aggregating Total orders per customer
customers_orders_df = orders_df.groupBy("customer_id").agg(
    count("order_id").alias("total_orders")
)

# Formatting year_month column from date 
orders_df = orders_df.withColumn("year_month", date_format("order_date", "yyyy_MM"))

# Aggregating total active months per customer
cust_active_months_df = orders_df.groupBy("customer_id").agg(
    countDistinct("year_month").alias("total_active_months")
)

# Joining both customer_orders and cust_active_months_df to calculate frequency per customer
joined_df = customers_orders_df.join(cust_active_months_df, on="customer_id", how="inner")
cust_purchase_frequencies_df = joined_df.withColumn(
    "freq_per_month",
    (col("total_orders") / col("total_active_months"))
)
# cust_purchase_frequencies_df.show()

# CLTV = (avg value × freq per month) × avg customer lifespan (assume 24 months)
# Joining both customer average purchase value and purchase frequency per customer 
joined_df = cust_avg_val_df.join(cust_purchase_frequencies_df, on="customer_id", how="inner")

# Calculating CLTV for customers
customers_cltv_df = joined_df.withColumn(
    "CLTV",
    (col("avg_purchase_value") * col("freq_per_month") * 24)
)

# Segregating customers based on cltv like High Medium and Low
customers_cltv_df = customers_cltv_df.withColumn(
    "Segmenting",
    when(col("CLTV") > 4000, "High")
    .when((col("CLTV") > 2000) & (col("CLTV") <= 4000), "Medium")
    .otherwise("Low")
)
customers_cltv_df.show()

# COMMAND ----------

"""
Chained Transactions Fraud Detection
Dataset:
transactions: txn_id, user_id, amount, txn_time, receiver_id

Tasks:
Identify sequences where:
A transaction occurs and within 2 minutes, the receiver sends money to someone else (chain)
Chain length ≥ 3 → flag as suspicious
Output: chain_id, user_ids_in_chain, total_amount
"""

# COMMAND ----------

# Schema
schema = StructType([
    StructField("txn_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("txn_time", StringType(), True),  # initially string
    StructField("receiver_id", IntegerType(), True)
])

# Sample transaction data as strings
data = [
    (1, 101, 100.0, "2023-01-01 10:00:00", 102),
    (2, 102, 90.0, "2023-01-01 10:01:30", 103),
    (3, 103, 85.0, "2023-01-01 10:03:00", 104),
    (4, 104, 80.0, "2023-01-01 10:06:00", 105),  # outside 2 min
    (5, 106, 50.0, "2023-01-01 11:00:00", 107),
    (6, 107, 45.0, "2023-01-01 11:01:00", 108),
    (7, 108, 40.0, "2023-01-01 11:01:59", 109),
    (8, 110, 100.0, "2023-01-01 12:00:00", 111),
    (9, 111, 50.0, "2023-01-01 12:05:00", 112)  # outside 2 min
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert txn_time from string to TimestampType
df = df.withColumn("txn_time", to_timestamp("txn_time", "yyyy-MM-dd HH:mm:ss"))

df.show(truncate=False)

# COMMAND ----------

# A transaction occurs and within 2 minutes, the receiver sends money to someone else (chain)
# window spec for to get previous txn_time
window_spec = Window.orderBy("txn_time")

# Using lag to get previous transaction time
transactions_df = df.withColumn(
    "prev_txn_time",
    lag("txn_time").over(window_spec)
)

# checking the difference betwen times


# COMMAND ----------

"""
Department Budget Misuse Alert System
Dataset:

expenses: expense_id, department, amount, submitted_by, approved_by, expense_date

Tasks:
For each department:
Average monthly expense
Flag any single transaction that exceeds 3× average
List top 5 employees with highest over-budget expense count
"""

# COMMAND ----------

# Step 1: Define schema with `expense_date` as string
schema = StructType([
    StructField("expense_id", IntegerType(), False),
    StructField("department", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("submitted_by", StringType(), False),
    StructField("approved_by", StringType(), False),
    StructField("expense_date", StringType(), False)  # initially as string
])

# Step 2: Sample data with date as string
data = [
    (1, "HR", 5000.0, "Alice", "Manager1", "2024-01-15"),
    (2, "HR", 4500.0, "Bob", "Manager1", "2024-01-20"),
    (3, "HR", 18000.0, "Alice", "Manager2", "2024-02-10"),
    (4, "Finance", 12000.0, "Carol", "Manager3", "2024-01-25"),
    (5, "Finance", 15000.0, "Dan", "Manager3", "2024-02-05"),
    (6, "Finance", 48000.0, "Eve", "Manager4", "2024-02-15"),
    (7, "IT", 9000.0, "Frank", "Manager5", "2024-01-18"),
    (8, "IT", 8500.0, "Grace", "Manager5", "2024-01-30"),
    (9, "IT", 27000.0, "Frank", "Manager6", "2024-02-12"),
    (10, "IT", 8600.0, "Heidi", "Manager5", "2024-03-01"),
    (11, "HR", 4700.0, "Ivan", "Manager2", "2024-03-05"),
    (12, "Finance", 11000.0, "Dan", "Manager4", "2024-03-10"),
    (13, "IT", 9500.0, "Grace", "Manager6", "2024-03-15"),
    (14, "Finance", 49500.0, "Eve", "Manager3", "2024-03-20")
]

# Step 3: Create DataFrame
expenses_df = spark.createDataFrame(data, schema)

# Step 4: Convert `expense_date` from string to actual DateType
expenses_df = expenses_df.withColumn("expense_date", to_date("expense_date", "yyyy-MM-dd"))

# Step 5: Display the final DataFrame
expenses_df.show()

# COMMAND ----------

class DepartmentBudgetMisuse:
    def read_data(self, path):
        try:
            df = spark.read.format("csv").option('header', True).load(path)
            return df 
        except Exception as err:
            print(f"Error occurred when reading file from path: {path} — {err}")
            return None
        
    def AvgMonthlyExpense(self, expenses_df):
        # For each department: Average monthly expense
        # Creating month column from expense date    
        expenses_df = expenses_df.withColumn("year_month", date_format(col("expense_date"), "yyyy_MM"))
        # Aggregating average department
        avg_expense_per_dept_df = expenses_df.groupBy("department", "year_month").agg(
            avg("amount").alias("avg_monthly_expense")
        )
        return avg_expense_per_dept_df
    
    def FlaggingTransactions(self, expenses_df):
        # Calling avg monthly expense function to get avg expenses
        avg_expense_per_dept_df = self.AvgMonthlyExpense(expenses_df)
        # Creating month column from expense date 
        expenses_df = expenses_df.withColumn("year_month", date_format(col("expense_date"), "yyyy_MM"))
        # joining avg expense with original dataframe
        dept_with_avg_exp_df = expenses_df.join(avg_expense_per_dept_df, on=["department", "year_month"], how="inner")
        # Flaging transactions based on expense
        flagged_df = dept_with_avg_exp_df.withColumn(
            "is_excedded",
            when(col("amount") > 3 * col("avg_monthly_expense"), "Yes").otherwise("No")
        )
        return flagged_df
    
    def HighBudgetExpenseEmployees(self, expenses_df):
        # Calling flagged transactions function to get flag details
        flagged_df = self.FlaggingTransactions(expenses_df)
        # Counting high budget expense counts for employees
        aggregated_df = flagged_df.groupBy("submitted_by").agg(
            count(when(col("is_excedded") == "Yes"), True).alias("total_over_budget_expenses")
        )
        # window specification for ranking
        window_rank = Window.orderBy(desc("total_over_budget_expenses"))
        # Ranking employees based on total expenses count
        ranking_df = aggregated_df.withColumn(
            "rnk",
            dense_rank().over(window_rank)
        )
        # Filtering top 5 employees
        top5_emps_df = ranking_df.filter(col("rnk") <= 5)

        return top5_emps_df
    
# Instantiating the class    
dept_inst = DepartmentBudgetMisuse()

# Reading the file as dataframe 
expenses_df = dept_inst.read_data("dbfs://FileStore/raw/expenses")

# Calling methods
avg_expense_per_dept_df = dept_inst.AvgMonthlyExpense(expenses_df)
flagged_df = dept_inst.FlaggingTransactions(expenses_df)
top5_emps_df = dept_inst.HighBudgetExpenseEmployees(expenses_df)

#Showing dataframes
avg_expense_per_dept_df.show()
flagged_df.show()
top5_emps_df.show()

# COMMAND ----------

"""
Website Session Path Frequency Analysis
Dataset:
web_logs: user_id, page, event_time

Tasks:
Define sessions (30-minute inactivity rule)
Extract user journeys: e.g., "Home > Products > Cart > Checkout"
Count frequency of each unique path
Identify top 5 most common abandoned paths (end before Checkout)
"""

# COMMAND ----------

# Sample data with string dates
data = [
    ("u1", "Home", "2024-06-14 10:00:00"),
    ("u1", "Products", "2024-06-14 10:05:00"),
    ("u1", "Cart", "2024-06-14 10:10:00"),
    ("u1", "Checkout", "2024-06-14 10:15:00"),

    ("u2", "Home", "2024-06-14 09:00:00"),
    ("u2", "Products", "2024-06-14 09:10:00"),
    ("u2", "Cart", "2024-06-14 09:20:00"),

    ("u3", "Home", "2024-06-14 08:00:00"),
    ("u3", "Products", "2024-06-14 08:20:00"),
    ("u3", "Cart", "2024-06-14 09:00:00"),  # >30min gap = new session
    ("u3", "Checkout", "2024-06-14 09:10:00"),

    ("u4", "Home", "2024-06-14 11:00:00"),
    ("u4", "About", "2024-06-14 11:25:00"),
    ("u4", "Contact", "2024-06-14 11:50:00"),

    ("u5", "Home", "2024-06-14 10:00:00"),
    ("u5", "Products", "2024-06-14 10:05:00"),
    ("u5", "Cart", "2024-06-14 10:50:00")  # >30min gap from previous
]

# Define schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("event_time", StringType(), True)
])

# Create DataFrame
web_logs_df = spark.createDataFrame(data, schema)

# Convert event_time to actual timestamp
web_logs_df = web_logs_df.withColumn("event_time", to_timestamp("event_time"))

# Show DataFrame
web_logs_df.show(truncate=False)


# COMMAND ----------

# Define sessions (30-minute inactivity rule)
window_spec = Window.partitionBy("user_id").orderBy("event_time")
web_logs_data_df = web_logs_df.withColumn(
    "prev_event_time",
    lag("event_time").over(window_spec)
)
web_logs_data_df = web_logs_data_df.withColumn(
    "time_diff",
    (unix_timestamp(col("event_time")) - unix_timestamp(col("prev_event_time"))) / 60
)
web_logs_flags_df = web_logs_data_df.withColumn(
    "streak",
    when((col("time_diff").isNull()) | (col("time_diff") <= 30), 0).otherwise(1)
)
# Creatiung sessions with running sum
web_sessions_df = web_logs_flags_df.withColumn(
    "sessionID",
    sum("streak").over(window_spec)
)
web_sessions_df.show()

# COMMAND ----------

# Extract user journeys: e.g., "Home > Products > Cart > Checkout"
home_events_df = web_sessions_df.filter(col("page") == "Home")

products_events_df = web_sessions_df.filter(col("page") == "Products")

cart_events_df = web_sessions_df.filter(col("page") == "Cart")

Checkout_events_df = web_sessions_df.filter(col("page") == "Checkout")

About_events_df = web_sessions_df.filter(col("page") == "About")

pricing_events_df = web_sessions_df.filter(col("page") == "Pricing")

contact_events_df = web_sessions_df.filter(col("page") == "Contact") 

users_home_with_products_df = home_events_df.alias("home").join(products_events_df.alias("products"), on=["user_id", "sessionID"], how="left")
users_home_with_products_df = users_home_with_products_df.select(
    col("user_id"),
    col("sessionID"),
    col("home.page").alias("home_event"),
    col("products.page").alias("product_event") 
)
users_home_products_cart_df = users_home_with_products_df.alias("hp").join(cart_events_df.alias("cart"), on=["user_id", "sessionID"], how="left")
users_home_products_cart_df = users_home_products_cart_df.select(
    col("user_id"),
    col("sessionID"),
    col("home_event"),
    col("product_event"),
    col("cart.page").alias("cart_event")
)
users_home_product_cart_checkout_df = users_home_products_cart_df.join(Checkout_events_df.alias("checkout"), on=["user_id", "sessionID"], how="left")
users_home_product_cart_checkout_df = users_home_product_cart_checkout_df.select(
    col("user_id"),
    col("sessionID"),
    col("home_event"),
    col("product_event"),
    col("cart_event"),
    col("checkout.page").alias("checkout_event")
)

users_home_product_cart_checkout_about_df = users_home_product_cart_checkout_df.join(About_events_df.alias("about"), on=["user_id", "sessionID"], how="left")
users_home_product_cart_checkout_about_df = users_home_product_cart_checkout_about_df.select(
    col("user_id"),
    col("sessionID"),
    col("home_event"),
    col("product_event"),
    col("cart_event"),
    col("checkout_event"),
    col("about.page").alias("about_event")
)

users_home_product_cart_checkout_about_pricing_df = users_home_product_cart_checkout_about_df.join(pricing_events_df.alias("p"), on =["user_id", "sessionID"], how="left")
users_home_product_cart_checkout_about_pricing_df = users_home_product_cart_checkout_about_pricing_df.select(
    col("user_id"),
    col("sessionID"),
    col("home_event"),
    col("product_event"),
    col("cart_event"),
    col("checkout_event"),
    col("about_event"),
    col("p.page").alias("price_event")
)

users_home_product_cart_checkout_about_pricing_contact_df = users_home_product_cart_checkout_about_pricing_df.join(contact_events_df.alias("c"), on=["user_id", "sessionID"], how="left")
users_home_product_cart_checkout_about_pricing_contact_df = users_home_product_cart_checkout_about_pricing_contact_df.select(
    col("user_id"),
    col("sessionID"),
    col("home_event"),
    col("product_event"),
    col("cart_event"),
    col("checkout_event"),
    col("about_event"),
    col("price_event"),
    col("c.page").alias("contact_event")
)
users_unique_path_df = users_home_product_cart_checkout_about_pricing_contact_df.withColumn(
    "journey",
    concat_ws(">", col("home_event"), col("product_event"), col("cart_event"), col("checkout_event"), col("about_event"), col("price_event"), col("contact_event"))
)
users_unique_path_df.select(
    col("user_id"),
    col("sessionID"),
    col("journey")
).display()


# COMMAND ----------

# Count frequency of each unique path
path_frequencies_df = users_unique_path_df.groupBy("journey").agg(
    count("*").alias("total_frequencies")
).show()

# COMMAND ----------

# Identify top 5 most common abandoned paths (end before Checkout)
last_element_df = users_unique_path_df.withColumn(
    "last_event",
    element_at(split(col("journey"), ">"), -1)
)

updated_df = last_element_df.select(
    col("user_id"),
    col("sessionID"),
    col("journey"),
    col("last_event")
)

filtered_df = updated_df.filter(col("last_event") != "Checkout")

abandoned_df = filtered_df.groupBy("journey").agg(
    count("*").alias("total_abondends")
)

window_rank = Window.orderBy(desc("total_abondends"))

ranking_paths_df = abandoned_df.withColumn(
    "rnk",
    dense_rank().over(window_rank)
)

top5_abandoned_paths_df = ranking_paths_df.filter(col("rnk") <= 5)

top5_abandoned_paths_df.show()

# COMMAND ----------

"""
Social Network – Influencer Detection
Dataset:
follows: follower_id, followee_id
posts: user_id, post_id, timestamp, likes

Tasks:
Find users with:
High follower count (>500)
High average likes per post
Rank influencers by an influence score = followers × avg likes
"""

# COMMAND ----------

follows_schema = StructType([
    StructField("follower_id", IntegerType(), True),
    StructField("followee_id", IntegerType(), True)
])

follows_data = [
    (101, 201),
    (102, 201),
    (103, 202),
    (104, 201),
    (105, 202),
    (106, 203),
    (107, 201),
    (108, 202),
    (109, 204),
    (110, 201),
    (111, 204),
    (112, 202),
    (113, 201),
    (114, 201),
    (115, 202)
]

follows_df = spark.createDataFrame(follows_data, schema=follows_schema)
follows_df.show()

# COMMAND ----------

posts_schema_str = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("post_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # Initially as string
    StructField("likes", IntegerType(), True)
])

posts_data_str = [
    (201, "p1", "2024-01-01 10:00:00", 120),
    (201, "p2", "2024-01-02 12:00:00", 340),
    (201, "p3", "2024-01-04 08:30:00", 220),
    (202, "p4", "2024-01-01 15:00:00", 50),
    (202, "p5", "2024-01-03 11:00:00", 70),
    (203, "p6", "2024-01-05 09:00:00", 200),
    (204, "p7", "2024-01-01 10:30:00", 30),
    (204, "p8", "2024-01-02 14:00:00", 20),
    (204, "p9", "2024-01-04 16:00:00", 15)
]

posts_df_str = spark.createDataFrame(posts_data_str, schema=posts_schema_str)
posts_df = posts_df_str.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
posts_df.show()

# COMMAND ----------

class InfluencerDetection:
    def read_data(self, path):
        try:
            df = spark.read.format('csv').option('header', True).load(path)
            return df
        except Exception as err:
            print(f"Error occured while reading file {path} — {err}")
            return None
        
    def usersFollowersCount(self, follows_df):
        # Aggregating total follower counts for each user
        users_df = follows_df.groupBy("followee_id").agg(
            count("follower_id").alias("total_followers")
        )    
        # Filtering high followers users
        users_followers_df = users_df.filter(col("total_followers") >= 500)
        return users_followers_df
    
    def usersAverageLikes(self, posts_df):
        # Aggregating average likes for each user
        avg_likes_df = posts_df.groupBy("user_id").agg(
            avg("likes").alias("avg_likes")
        )
        return avg_likes_df
    
    def usersInfluenceScores(self, follows_df, posts_df):
        # Calling methods
        users_followers_df = self.usersFollowersCount(follows_df)
        avg_likes_df = self.usersAverageLikes(posts_df)
        # Joining both follows and posts dataframes
        users_with_followers_count_avg_likes_df = users_followers_df.join(avg_likes_df, users_followers_df["followee_id"] == avg_likes_df["user_id"], how="inner") 
        users_with_followers_count_avg_likes_df = users_with_followers_count_avg_likes_df.select(
            col("user_id"),
            col("total_followers"),
            col("avg_likes")
        ) 
        # Calculating influencer score for users
        users_influence_scores_df = users_with_followers_count_avg_likes_df.withColumn(
            "influence_score",
            col("total_followers") * col("avg_likes")
        ) 
        return users_influence_scores_df
    
    def rankingInfluencers(self, follows_df, posts_df):
        users_influence_scores_df = self.usersInfluenceScores(follows_df, posts_df)
        # Window spec for Ranking influencers
        window_spec = Window.orderBy(desc("influence_score"))
        influencers_ranking_df = users_influence_scores_df.withColumn(
            "rnk",
            dense_rank().over(window_spec)
        )
        return influencers_ranking_df
    
# Instantiating the class    
influ_inst = InfluencerDetection()    

# Calling dataframes
follows_df = influ_inst.read_data("dbfs://FileStore/raw/follows_df")
posts_df = influ_inst.read_data("dbfs://FileStore/raw/posts_df")

# Calling methods 
users_followers_df = influ_inst.usersFollowersCount(follows_df)
avg_likes_df = influ_inst.usersAverageLikes(posts_df)
users_influence_scores_df = influ_inst.usersInfluenceScores(follows_df, posts_df)
influencers_ranking_df = influ_inst.rankingInfluencers(follows_df, posts_df)

# COMMAND ----------

"""
 OTT Platform – Binge Watch Pattern Detection
Dataset:
watch_history: user_id, content_id, watch_time, duration

Tasks:
Define a binge: watching 3+ episodes in a row of same series within 6 hours
For each user:
Count binge sessions
Identify most binged series
"""

# COMMAND ----------

watch_history_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("series", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("watch_time", StringType(), True),  # keep as string first
    StructField("duration", IntegerType(), True)
])

watch_history_data = [
    (101, "series1", "series1_ep1", "2024-06-10 10:00:00", 45),
    (101, "series1", "series1_ep2", "2024-06-10 10:50:00", 40),
    (101, "series1", "series1_ep3", "2024-06-10 11:40:00", 50),
    (101, "series2", "series2_ep1", "2024-06-11 09:00:00", 30),
    (102, "series3", "series3_ep1", "2024-06-12 13:00:00", 60),
    (102, "series3", "series3_ep2", "2024-06-12 14:00:00", 55),
    (102, "series3", "series3_ep3", "2024-06-12 15:00:00", 50),
    (102, "series3", "series3_ep4", "2024-06-12 19:30:00", 45),  # this one breaks the binge (too late)
    (103, "series1", "series1_ep1", "2024-06-13 17:00:00", 45),
    (103, "series1", "series1_ep2", "2024-06-13 17:50:00", 40)
]

watch_history_df_str = spark.createDataFrame(watch_history_data, schema=watch_history_schema)

watch_history_df = watch_history_df_str.withColumn("watch_time", to_timestamp("watch_time", "yyyy-MM-dd HH:mm:ss"))
watch_history_df.show()

# COMMAND ----------

# Define a binge: watching 3+ episodes in a row of same series within 6 hours

# window spec for to get previous watch time
window_spec = Window.partitionBy("user_id", "series").orderBy("watch_time")

# Converting watch time to numeric 
watch_history_df = watch_history_df.withColumn(
    "watch_ts",
    unix_timestamp(col("watch_time"))
)

# Creating previous watch time
users_watch_analysis_df = watch_history_df.withColumn(
    "prev_watch",
    lag("watch_time").over(window_spec)
)

# Checking timestamp difference 
users_watch_diff_df = users_watch_analysis_df.withColumn(
    "time_diff",
    (unix_timestamp(col("watch_time")) - unix_timestamp(col("prev_watch"))) / 3600
)

# Window specification for episodes count
window = Window.partitionBy("user_id", "series").orderBy("watch_ts").rangeBetween(-21600, 0)

# Aggregating episodes count
users_episodes_df = users_watch_diff_df.withColumn(
    "episodes_count",
    count("*").over(window)
)

users_episodes_df = users_episodes_df.withColumn(
    "is_binge",
    when(col("episodes_count") >= 3, 1).otherwise(0)
)
# Filtering only series having episodes count >= 3
user_binges_df = users_episodes_df.filter(col("is_binge") == 1)

# Creating sessions by rolling cumulative sum
users_sessions_df = user_binges_df.withColumn(
    "sessionID",
    sum("is_binge").over(window_spec)
)

# Aggregating total binge sessions per user
user_total_binge_sessions_df = users_sessions_df.groupBy("user_id").agg(
    countDistinct("sessionID").alias("total_sessions")
)
user_total_binge_sessions_df.show()

# Identify most binged series
user_most_binge_series_df = users_sessions_df.groupBy("user_id", "series").agg(
    count("sessionID").alias("most_watched_series_per_user")
)
user_most_binge_series_df.show()


# COMMAND ----------

users_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("user_name", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("account_type", StringType(), True)
])

users_data = [
    ("U1", "Arjun",  "2023-01-10", "India",    "FREE"),
    ("U2", "Rahul",  "2023-02-15", "India",    "PREMIUM"),
    ("U3", "Sneha",  "2023-03-20", "India",    "FREE"),
    ("U4", "John",   "2023-01-05", "USA",      "PREMIUM"),
    ("U5", "Emma",   "2023-04-12", "USA",      "FREE"),
    ("U6", "Liam",   "2023-05-18", "USA",      "PREMIUM"),
    ("U7", "Noah",   "2023-02-01", "UK",       "FREE"),
    ("U8", "Olivia", "2023-06-10", "UK",       "PREMIUM"),
    ("U9", "Ava",    "2023-07-22", "Germany",  "FREE"),
    ("U10","Mia",    "2023-08-30", "Germany",  "PREMIUM"),
    ("U11","Lucas",  "2023-09-12", "France",   "FREE"),
    ("U12","Ethan",  "2023-10-05", "France",   "PREMIUM")
]

users_df = spark.createDataFrame(users_data, ["user_id","user_name","signup_date","country","account_type"]) \
    .withColumn("signup_date", to_date("signup_date"))

users_df.show()

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("order_status", StringType(), True)
])

orders_data = [
    ("O1", "U1", "2024-01-01 10:00:00", 500.0,  "CARD", "SUCCESS"),
    ("O2", "U1", "2024-01-05 12:00:00", 300.0,  "UPI", "FAILED"),
    ("O3", "U1", "2024-01-10 14:00:00", 700.0,  "CARD", "SUCCESS"),

    ("O4", "U2", "2024-02-01 09:00:00", 2000.0, "CARD", "SUCCESS"),
    ("O5", "U2", "2024-02-03 11:00:00", 1500.0, "NETBANKING", "SUCCESS"),
    ("O6", "U2", "2024-02-05 16:00:00", 1000.0, "CARD", "FAILED"),

    ("O7", "U3", "2024-03-01 10:00:00", 250.0,  "UPI", "FAILED"),
    ("O8", "U3", "2024-03-02 12:00:00", 350.0,  "UPI", "FAILED"),

    ("O9", "U4", "2024-01-15 10:30:00", 5000.0, "CARD", "SUCCESS"),
    ("O10","U4", "2024-01-20 15:45:00", 4500.0, "CARD", "SUCCESS"),
    ("O11","U4", "2024-02-01 09:20:00", 4000.0, "UPI", "SUCCESS"),

    ("O12","U5", "2024-04-01 10:00:00", 100.0,  "UPI", "SUCCESS"),

    ("O13","U6", "2024-05-01 13:00:00", 8000.0, "CARD", "SUCCESS"),
    ("O14","U6", "2024-05-05 14:00:00", 9000.0, "CARD", "SUCCESS"),
    ("O15","U6", "2024-05-10 15:00:00", 10000.0,"NETBANKING", "SUCCESS"),
    ("O16","U6", "2024-05-15 16:00:00", 2000.0, "UPI", "FAILED"),

    ("O17","U7", "2024-06-01 10:00:00", 600.0,  "CARD", "SUCCESS"),
    ("O18","U8", "2024-06-05 11:00:00", 700.0,  "CARD", "SUCCESS"),
    ("O19","U9", "2024-07-01 09:00:00", 1200.0, "UPI", "SUCCESS"),
    ("O20","U10","2024-07-10 14:00:00", 2200.0, "CARD", "FAILED")
]

orders_df = spark.createDataFrame(orders_data, ["order_id","user_id","order_date","order_amount","payment_mode","order_status"]) \
    .withColumn("order_date", to_timestamp("order_date"))

orders_df.show()

refunds_schema = StructType([
    StructField("refund_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("refund_date", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("refund_reason", StringType(), True)
])

refunds_data = [
    ("R1", "O1", "2024-01-02 10:00:00", 500.0, "Product Issue"),
    ("R2", "O4", "2024-02-02 10:00:00", 500.0, "Late Delivery"),
    ("R3", "O5", "2024-02-06 12:00:00", 1500.0, "Customer Cancelled"),
    ("R4", "O9", "2024-01-16 10:00:00", 1000.0, "Damaged"),
    ("R5", "O13","2024-05-02 14:00:00", 8000.0, "Fraud Suspected"),
    ("R6", "O14","2024-05-06 14:30:00", 9000.0, "Fraud Suspected")
]

refunds_df = spark.createDataFrame(refunds_data, ["refund_id","order_id","refund_date","refund_amount","refund_reason"]) \
    .withColumn("refund_date", to_timestamp("refund_date"))


refunds_df.show()


users_df.write.format("delta").mode("overwrite").save("")



# COMMAND ----------

"""
Find the popularity percentage for each user on Meta/Facebook. The dataset contains two columns, user1 and user2, which represent pairs of friends. Each row indicates a mutual friendship between user1 and user2, meaning both users are friends with each other. A user's popularity percentage is calculated as the total number of friends they have (counting connections from both user1 and user2 columns) divided by the total number of unique users on the platform. Multiply this value by 100 to express it as a percentage.


Output each user along with their calculated popularity percentage. The results should be ordered by user ID in ascending order.
"""


friends_pairone_df = facebook_friends_df.select(
    col("user1").alias("userID"),
    col("user2").alias("friendID")
)

friends_pairtwo_df = facebook_friends_df.select(
    col("user2").alias("userID"),
    col("user1").alias("friendID")
)

friends_pairs_df = friends_pairone_df.union(friends_pairtwo_df)

friends_pairs_df.show()

users_total_connections_df = friends_pairs_df.groupBy("userID").agg(
    count("friendID").alias("total_connections")
)

total_unique_users_df = friends_pairs_df.agg(
    countDistinct("userID").alias("total_unique_users")
)

final_upd_df = users_total_connections_df.crossJoin(total_unique_users_df)

users_popularity_percentages_df = final_upd_df.withColumn(
    "popularity_perc",
    (col("total_connections") / col("total_unique_users")) * 100
)

final_result_df = users_popularity_percentages_df.orderBy(col("userID"))

final_result_df.show()



