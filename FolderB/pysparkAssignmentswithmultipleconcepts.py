# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# Define schemas
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("country", StringType(), False)
])

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", StringType(), False),
    StructField("amount", DoubleType(), False)
])

# Create DataFrames with sample data
customers_data = [
    (1, "Alice", "USA"),
    (2, "Bob", "Canada"),
    (3, "Charlie", "India"),
    (4, "David", "USA")
]

orders_data = [
    (101, 1, "2024-01-10", 500.0),
    (102, 2, "2024-01-11", 300.0),
    (103, 3, "2024-01-12", 700.0),
    (104, 1, "2024-02-10", 600.0),
    (105, 2, "2024-02-15", 400.0),
    (106, 4, "2024-03-01", 800.0)
]

customers_df = spark.createDataFrame(customers_data, schema=customer_schema)
orders_df = spark.createDataFrame(orders_data, schema=order_schema)

# Display DataFrames
customers_df.show()
orders_df = orders_df.withColumn(
    "order_date",
    to_date("order_date")
)
orders_df.show()

# COMMAND ----------

# Calculate the total amount spent by each customer using aggregations.
customersWithOrders_df = customers_df.join(orders_df, on="customer_id", how="inner")
totalAmount_per_cust_df = customersWithOrders_df.groupBy("customer_id").agg(
    sum("amount").alias("total_amount_spent")
)
totalAmount_per_cust_df.show()

# Identify the customer with the highest total spending using window functions.
window_spec = Window.orderBy(desc("total_amount_spent"))
ranking_df = totalAmount_per_cust_df.withColumn("rank",rank().over(window_spec))
highest_total_spending_cust_df = ranking_df.filter(col("rank") == 1)
highest_total_spending_cust_df.show()

# COMMAND ----------

# Use a left anti join to find customers who haven't placed any orders.
customersWithNoOrders_df = customers_df.join(orders_df, on="customer_id", how="leftanti")
customersWithNoOrders_df.show()

# COMMAND ----------

# Perform an inner join to get customer details with their orders.
inner_joined = customers_df.join(orders_df, on="customer_id", how="inner")
inner_joined.show()

# COMMAND ----------

# Define schemas
employee_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False)
])

promotion_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("promotion_date", StringType(), False),  # Initially as String to cast later
    StructField("new_salary", IntegerType(), False)
])

# Create DataFrames
employees_data = [
    (101, "John", "HR", 50000),
    (102, "Alice", "IT", 70000),
    (103, "Bob", "Finance", 60000),
    (104, "Charlie", "IT", 75000),
    (105, "David", "HR", 52000)
]

promotions_data = [
    (101, "2023-03-01", 52000),
    (103, "2023-05-10", 65000),
    (102, "2023-06-15", 75000),
    (104, "2024-01-01", 80000)
]

employees_df = spark.createDataFrame(employees_data, schema=employee_schema)
promotions_df = spark.createDataFrame(promotions_data, schema=promotion_schema)

# Convert promotion_date to DateType
promotions_df = promotions_df.withColumn("promotion_date", to_date(col("promotion_date"), "yyyy-MM-dd"))

# Display DataFrames
employees_df.show()
promotions_df.show()

# COMMAND ----------

# Identify the employee who received the largest salary increase after promotion.
joined_df = employees_df.join(promotions_df, on="emp_id", how="inner")
empWithSalDiff_df = joined_df.withColumn(
    "sal_diff",
    col("new_salary") - col("salary")
)
window_spec = Window.orderBy(desc("sal_diff"))
ranking_df = empWithSalDiff_df.withColumn("rank",dense_rank().over(window_spec))
emp_with_highest_sal_increase_df = ranking_df.filter(col("rank") == 1)
emp_with_highest_sal_increase_df.show()

# COMMAND ----------

# Use a window function to find each employee's previous salary before promotion.
employeesWithPromotions_df = employees_df.join(promotions_df, on="emp_id", how="left")

employeesWithPrevSalary_df = employeesWithPromotions_df.filter(col("new_salary").isNotNull()).select(
    col("emp_id"),
    col("salary").alias("previous_salary")
)
employeesWithPrevSalary_df.show()

# COMMAND ----------

# Products DataFrame
products_data = [
    (1, "Laptop", "Electronics"),
    (2, "Mobile", "Electronics"),
    (3, "Shirt", "Fashion")
]


products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

products_df = spark.createDataFrame(products_data, schema=products_schema)

# Sales DataFrame
sales_data = [
    (101, 1, "2024-01-01", 10),
    (102, 2, "2024-01-05", 20),
    (103, 1, "2024-02-10", 15),
    (104, 3, "2024-02-15", 30),
    (105, 2, "2024-03-01", 25)
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("units_sold", IntegerType(), True)
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Display DataFrames
print("Products DataFrame:")
products_df.show()

print("Sales DataFrame:")
sales_df = sales_df.withColumn("sale_date", to_date("sale_date"))
sales_df.show()

# COMMAND ----------

# Perform a cross join to find all possible product-sale combinations.
product_sales_combo_df = products_df.crossJoin(sales_df)
product_sales_combo_df.show()

# COMMAND ----------

# Use a window function to calculate running total sales per product.
window_spec = Window.partitionBy("product_id").orderBy("sale_date")

running_total_df = sales_df.withColumn(
    "running_total",
    sum("units_sold").over(window_spec)
)

running_total_df.show()

# COMMAND ----------

# Identify the top 2 products with the highest total units sold.
total_units_sold_df = sales_df.groupBy("product_id").agg(
    sum("units_sold").alias("total_units")
)

window_rank = Window.orderBy(desc("total_units"))
ranking_df = total_units_sold_df.withColumn(
    "rank",
    rank().over(window_rank)
)
top2_products_with_highest_units_df = ranking_df.filter(col("rank") <= 2)
top2_products_with_highest_units_df.show()

# COMMAND ----------

json_data = [
    {
  "customer_id": "123",
  "name": "John Doe",
  "reviews": [
    {"product_id": "P1", "rating": 5, "comment": "Great!"},
    {"product_id": "P2", "rating": 3, "comment": "Average"},
    {"product_id": "P3", "rating": 4, "comment": "Good quality"}
  ]
}
]

json_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("reviews", ArrayType(StructType([
        StructField("product_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("comment", StringType(), True)
    ])), True)  # Note: reviews should be ArrayType
]) 

df = spark.createDataFrame(json_data, json_schema)
df = df.withColumn(
    "review",
    explode("reviews")
)
df = df.select(
    col("customer_id"),
    col("name"),
    col("review.product_id"),
    col("review.rating"),
    col("review.comment")
)
df.show()

# COMMAND ----------

# Find customers who gave an average rating below 3.
customers_avg_rating_df = df.groupBy("customer_id").agg(
    avg("rating").alias("avg_rating")
)
final_df = customers_avg_rating_df.filter(col("avg_rating") < 3)

# COMMAND ----------

customers_avg_rating_df.createOrReplaceTempView("customer_ratings")


# COMMAND ----------

spark.sql("select * from customer_ratings").show()

# COMMAND ----------

"""
Customer Churn Prediction (Feature Engineering Focus)
Dataset: Customer subscription data with:
customer_id, signup_date, last_login, total_spent, subscription_status, region, plan_type.

Tasks:
Compute customer tenure (days since signup).
Calculate average spending per month for each customer.
Identify inactive customers (no login in the last 90 days).
Use window functions to find customers whose spending is consistently decreasing over time.
Label customers as "At Risk" if their spending drops by 40% or more in the last 3 months"""

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("Customer Churn Prediction").getOrCreate()

# Sample Data
data = [
    ("C001", "2022-01-10", "2024-03-01", 500.0, "Active", "North", "Premium"),
    ("C002", "2021-05-20", "2023-11-15", 1200.0, "Active", "South", "Standard"),
    ("C003", "2023-02-01", "2023-12-10", 300.0, "Inactive", "West", "Basic"),
    ("C004", "2020-08-15", "2023-05-20", 2000.0, "Active", "East", "Premium"),
    ("C005", "2022-10-01", "2024-02-28", 750.0, "Active", "North", "Standard"),
    ("C006", "2023-01-05", "2023-09-01", 400.0, "Inactive", "South", "Basic"),
]

# Define Schema
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("last_login", StringType(), True),
    StructField("total_spent", DoubleType(), True),
    StructField("subscription_status", StringType(), True),
    StructField("region", StringType(), True),
    StructField("plan_type", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display Sample Data
df = df.withColumn("signup_date", to_date("signup_date"))\
    .withColumn("last_login", to_date("last_login"))

df.show()


# COMMAND ----------

Identify inactive customers (no login in the last 90 days).

# COMMAND ----------

# Compute customer tenure (days since signup).

class customer_churn_prediction():

    def read_data(self, path, file_format):
        df = spark.read.format(file_format).option('header',True).load(path)
        return df 
    
    def customer_tenture(self, customers_df, last_login, signup_date):
        try:
            customer_tenture_df = customers_df.withColumn(
                "tenture",
                date_diff(last_login, signup_date)
            )
        except Exception as e:
            print(f"Error:{e}")  
        return customer_tenture_df

    def avg_spending(self, customers_df, signup_date, last_login, avg_key):
        updated_df = customers_df.withColumn(
            "year_month",
            date_format(col(signup_date), "yyyy-MM")
        ).withColumn(
            "months_active",
            floor(months_between(col(last_login), col(signup_date)))
        )
        
        aggregated_df = updated_df.groupBy("customer_id", "year_month").agg(
            round((col(avg_key) / col("months_active")), 2).alias("avg_spending_per_month")
        )
        
        return aggregated_df

cust_instance = CustomerChurnPrediction()
df = cust_instance.read_data("dbfs://FileStore/rawlayer/customers", "csv")

result1 = cust_instance.customer_tenture(df, "signup_date")
result2 = cust_instance.avg_spending(df, "signup_date", "last_login", "total_spent")

result1.show()
result2.show()
    


# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Sample sales data
sales_data = [
    ("2023-01-01", "North", "Electronics", 5000.0),
    ("2023-01-01", "South", "Electronics", 3000.0),
    ("2023-02-01", "North", "Furniture", 4000.0),
    ("2023-02-01", "South", "Electronics", 2000.0),
    ("2023-03-01", "East", "Clothing", 3500.0),
    ("2023-03-01", "West", "Furniture", 6000.0),
    ("2023-03-01", "North", "Electronics", 4500.0),
    ("2023-04-01", "East", "Electronics", 3000.0),
    ("2023-04-01", "West", "Clothing", 2500.0),
    ("2023-05-01", "South", "Furniture", 5000.0),
    ("2023-05-01", "East", "Clothing", 4000.0),
    ("2023-06-01", "North", "Electronics", 2000.0),
    ("2023-06-01", "West", "Furniture", 4500.0),
]

# Define schema
sales_schema = StructType([
    StructField("date", StringType(), True),
    StructField("region", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("sales_amount", DoubleType(), True)
])

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Display the sample data
sales_df = sales_df.withColumn("date", to_date(col("date")))
sales_df.show()

# COMMAND ----------

# Pivot the data to display monthly sales per region.
sales_df = sales_df.withColumn("year_month", date_format("date", "yyyy-MM"))
pivoted_df = sales_df.groupBy("region").pivot("year_month").agg(sum("sales_amount"))
pivoted_df.show()

# COMMAND ----------

# Use unpivoting to convert wide data back to long format.
unpivoted_df = pivoted_df.selectExpr(
    "region",
    "stack(6, '2023-01', `2023-01`, '2023-02', `2023-02`, '2023-03', `2023-03`, "
    "'2023-04', `2023-04`, '2023-05', `2023-05`, '2023-06', `2023-06`) "
    "as (year_month, total_sales)"
).where("total_sales is not null")
unpivoted_df.show()

# COMMAND ----------

# Identify the top 3 performing regions by total sales for each month.
window_spec = Window.partitionBy("year_month").orderBy(desc("total_sales"))
ranking_df = unpivoted_df.withColumn("rank", rank().over(window_spec))
top3_regions_df = ranking_df.filter(col("rank") <= 3)
top3_regions_df.show()

# COMMAND ----------

# Find regions that experienced a sales decline in consecutive months.
#window spec
window_spec = Window.partitionBy("region").orderBy("year_month")

# Add Previous Month Sales
updated_df = unpivoted_df.withColumn(
    "prev_month_sales",
    lag("total_sales").over(window_spec)
)

# Filter out nulls (first entry per region will have no previous month)
updated_df = updated_df.filter(col("prev_month_sales").isNotNull())

# Calculate Declining Streaks
aggregated_df = updated_df.groupBy("region").agg(
    sum(when(col("total_sales") < col("prev_month_sales"), 1).otherwise(0)).alias("declining_streaks"),
    F.count("*").alias("total_months")
)


# Identify Regions with Continuous Declines
sales_decline_regions_df = aggregated_df.filter(F.col("declining_streaks") >= 2)
sales_decline_regions_df.show()