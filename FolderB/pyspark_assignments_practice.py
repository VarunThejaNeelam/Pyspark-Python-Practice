# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("salary", IntegerType(), False)
])

# Create data
data = [
    (1, "John", "IT", 2021, 80000),
    (2, "Mike", "HR", 2021, 60000),
    (1, "John", "IT", 2022, 85000),
    (2, "Mike", "HR", 2022, 62000),
    (3, "Sarah", "IT", 2021, 75000),
    (3, "Sarah", "IT", 2022, 77000)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

#Find the previous year's salary for each employee using LAG().
#window specification to find previous salary
window_spec=Window.partitionBy("employee_id").orderBy("year")

updated_df=df.withColumn(
    "previous_year_salary",
    lag("salary").over(window_spec)
)
updated_df.display()

#Calculate the salary difference from the previous year.
salary_diff_df=updated_df.withColumn(
    "salary_diff",
    col("salary") - col("previous_year_salary")
).filter(col("previous_year_salary").isNotNull()) 

salary_diff_df.display()

#Find the highest paid employee per department using RANK().
window=Window.partitionBy("department","year").orderBy(desc("salary"))

df=df.withColumn(
    "rank",
    rank().over(window)
)
# Sort by year first, then by rank within each year
df = df.orderBy("year", "rank")

# Display the result
df.display()

#Assign row numbers to employees within each department by salary using ROW_NUMBER().
row_numbers_df=df.withColumn(
    "row_number",
    row_number().over(window)
)

#displaying the df with rownumbers
row_numbers_df.display()



# COMMAND ----------

print(spark.conf.get("spark.master")) 


# COMMAND ----------

spark.conf.get("spark.executor.instances")  # Number of executors
spark.conf.get("spark.executor.memory")  # Memory per executor
spark.conf.get("spark.executor.cores")  # Cores per executor
spark.conf.get("spark.driver.memory")  # Memory allocated to Driver



# COMMAND ----------

"""
Bank Transaction Fraud Detection
📌 Scenario: A bank has a dataset of transactions with columns account_id, transaction_id, transaction_amount, transaction_type, timestamp, and location.

🔹 Task:

Detect duplicate transactions (same amount, timestamp, and account ID).
Identify customers who made more than 5 high-value transactions (e.g., amount > 1,00,000) in a single day.
Find customers who withdrew more than 75% of their account balance in a single transaction.
Flag suspicious transactions where the same account has transactions from different cities within 5 minutes.
Save flagged transactions in an enriched layer for further investigation.
"""

# COMMAND ----------

# Define Schema
schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("transaction_amount", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("location", StringType(), False),
    StructField("account_balance", DoubleType(), False)
])

# Sample Data
data = [
    ("A001", "T1001", 150000.0, "credit", "2024-01-09 10:15:00", "New York", 500000.0),
    ("A001", "T1002", 150000.0, "credit", "2024-01-09 10:15:00", "New York", 500000.0),  # Duplicate
    ("A002", "T2001", 200000.0, "debit", "2024-01-09 11:00:00", "Los Angeles", 300000.0),
    ("A002", "T2002", 120000.0, "debit", "2024-01-09 11:30:00", "Los Angeles", 300000.0),
    ("A002", "T2003", 180000.0, "debit", "2024-01-09 12:00:00", "Los Angeles", 300000.0),
    ("A002", "T2004", 110000.0, "debit", "2024-01-09 13:00:00", "Los Angeles", 300000.0),
    ("A002", "T2005", 105000.0, "debit", "2024-01-09 14:00:00", "Los Angeles", 300000.0),
    ("A002", "T2006", 150000.0, "debit", "2024-01-09 15:00:00", "Los Angeles", 300000.0),  # More than 5 high-value transactions
    ("A003", "T3001", 90000.0, "debit", "2024-01-09 09:45:00", "Chicago", 120000.0),  # More than 75% withdrawal
    ("A004", "T4001", 50000.0, "debit", "2024-01-09 14:10:00", "San Francisco", 200000.0),
    ("A004", "T4002", 50000.0, "debit", "2024-01-09 14:12:00", "Los Angeles", 200000.0),  # Different city within 5 minutes
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df=df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType())
)

#Detect duplicate transactions (same amount, timestamp, and account ID).
# Define window for detecting duplicate transactions
window = Window.partitionBy("account_id").orderBy("timestamp")

# Create lag columns to compare with previous transactions
updated_df = df.withColumn("lag_account_id", lag("account_id").over(window)) \
               .withColumn("lag_transaction_amount", lag("transaction_amount").over(window)) \
               .withColumn("lag_timestamp", lag("timestamp").over(window))

# Remove null lag values (first row in each partition will have nulls)
updated_df = updated_df.filter(col("lag_account_id").isNotNull())

# Detect duplicate transactions (same amount, timestamp, and account ID)
duplicate_trans_df = updated_df.filter(
    (col("account_id") == col("lag_account_id")) &
    (col("transaction_amount") == col("lag_transaction_amount")) &
    (col("timestamp") == col("lag_timestamp"))
)

# Select required columns
duplicate_trans_df.select(
    "account_id", "lag_account_id", "transaction_amount", 
    "lag_transaction_amount", "timestamp", "lag_timestamp"
).show()

#Identify customers who made more than 5 high-value transactions (e.g., amount > 1,00,000) in a single day.

# Extract Date from Timestamp
df = df.withColumn("date", to_date(col("timestamp")))

# Define Window to partition by account_id and date
windowSpec = Window.partitionBy("account_id", "date")

# Count high-value transactions (amount > 100000) per day per account
transaction_df = df.withColumn(
    "high_transaction_count",
    sum(when(col("transaction_amount") >= 100000, 1).otherwise(0)).over(windowSpec)
)

# Filter accounts where high-value transactions exceed 5 in a single day
high_transactions_customer_df = transaction_df.filter(col("high_transaction_count") > 5)

# display result
high_transactions_customer_df.display()


#Find customers who withdrew more than 75% of their account balance in a single transaction

# Define a window partitioned by account_id
windowSpec = Window.partitionBy("account_id").orderBy("timestamp")

# Add a column to track the previous transaction ID
updated_df = df.withColumn("lag_transaction_id", lag("transaction_id").over(windowSpec))

# Filter transactions where the withdrawal exceeds 75% of the account balance
final_df = updated_df.filter(
    (col("transaction_amount") > col("account_balance") * 0.75) &  # Fix: Use & instead of and
    (col("lag_transaction_id").isNull())  # Fix: Correct filter condition
)

# Show the result
final_df.display()


#Flag suspicious transactions where the same account has transactions from different cities within 5 minutes.

# Define window partitioned by account_id, ordered by timestamp
window_spec = Window.partitionBy("account_id").orderBy("timestamp")

# Create lag columns for location and timestamp
updated_df = df.withColumn("lag_location", lag("location").over(window_spec)) \
               .withColumn("lag_timestamp", lag("timestamp").over(window_spec))

# Filter transactions where location has changed
updated_df = updated_df.filter(col("location") != col("lag_location"))

# Calculate time difference in seconds
updated_df = updated_df.withColumn(
    "time_diff",
    unix_timestamp(col("timestamp")) - unix_timestamp(col("lag_timestamp"))
)

# Filter transactions occurring within 5 minutes (300 seconds)
suspicious_transaction_df = updated_df.filter(col("time_diff") <= 300)

# Display the result
suspicious_accounts_df = suspicious_transaction_df.select("account_id").distinct()
suspicious_accounts_df.show()


# COMMAND ----------

# Define a window partitioned by account_id
windowSpec = Window.partitionBy("account_id").orderBy("timestamp")
df.display()
# Add a column to track the previous transaction ID
updated_df = df.withColumn("lag_transaction_id", lag("transaction_id").over(windowSpec))

# Filter transactions where the withdrawal exceeds 75% of the account balance
final_df = updated_df.filter(
    (col("transaction_type") == "debit") &  # Only withdrawals
    (col("transaction_amount") > col("account_balance") * 0.75)  # More than 75% withdrawal
)

# Show the result
final_df.display() 

# COMMAND ----------

"""
3. IoT Sensor Data Processing
📌 Scenario: IoT devices send sensor readings in JSON format, including device_id, temperature, humidity, location, timestamp, and an array alerts containing anomalies detected.

🔹 Task:

Extract and flatten the alerts array (each alert should be a separate row).
Compute average temperature for each location per hour.
Find devices with faulty readings (temperature < -50°C or > 100°C).
Detect missing sensor readings using window functions (if a device has no reading for more than 10 minutes, mark it as missing).
Store cleaned and processed data in Parquet format."""

# COMMAND ----------

# Sample JSON data
data = [
    {
        "device_id": "D1001",
        "temperature": 22.5,
        "humidity": 60.2,
        "location": "Warehouse A",
        "timestamp": "2025-01-11 10:05:00",
        "alerts": ["High Humidity"]
    },
    {
        "device_id": "D1002",
        "temperature": -55.0,
        "humidity": 45.3,
        "location": "Warehouse B",
        "timestamp": "2025-01-11 10:15:00",
        "alerts": ["Low Temperature"]
    },
    {
        "device_id": "D1003",
        "temperature": 105.3,
        "humidity": 30.1,
        "location": "Warehouse C",
        "timestamp": "2025-01-11 10:25:00",
        "alerts": ["High Temperature"]
    },
    {
        "device_id": "D1001",
        "temperature": 23.0,
        "humidity": 58.6,
        "location": "Warehouse A",
        "timestamp": "2025-01-11 10:35:00",
        "alerts": []
    },
    {
        "device_id": "D1002",
        "temperature": 24.1,
        "humidity": 50.4,
        "location": "Warehouse B",
        "timestamp": "2025-01-11 10:50:00",
        "alerts": ["Sensor Calibration Required"]
    },
    {
        "device_id": "D1003",
        "temperature": 21.8,
        "humidity": 32.7,
        "location": "Warehouse C",
        "timestamp": "2025-01-11 11:05:00",
        "alerts": []
    }
]

# Define schema
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("alerts", ArrayType(StringType()), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert timestamp column to proper TimestampType
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

#df.display()

#flttening the alerts array 
flattened_df=df.select(
    col("device_id"),
    col("temperature"),
    col("humidity"),
    col("location"),
    col("timestamp"),
    explode(col("alerts")).alias("alert")
)

flattened_df.display()

# COMMAND ----------

#Detect missing sensor readings using window functions (if a device has no reading for more than 10 minutes, mark it as missing).

#window specification to calculate previous timestamp
window_spec=Window.partitionBy("device_id").orderBy("timestamp")

#logic to create timestamp
missing_readings_df=flattened_df.withColumn(
    "lag_timestamp",
    lag("timestamp").over(window_spec)
)

#finding difference between timestamps
time_diff_df=missing_readings_df.withColumn(
    "time_diff",
    unix_timestamp(F.col("timestamp")) - unix_timestamp(F.col("lag_timestamp"))
)
final_df=time_diff_df.withColumn(
    "reading_missing",
    F.when(
        (F.col("time_diff") > 600)  | (F.col("time_diff").isNull()),"missing"
    ).otherwise("not")
)
final_df.display()

# COMMAND ----------

# Find devices with faulty readings (temperature < -50°C or > 100°C)
faulty_device_df = flattened_df.filter(
    (col("temperature") < -50) | (col("temperature") > 100)
)
faulty_device_df.show()

# COMMAND ----------

#Compute average temperature for each location per hour.
updated_df=flattened_df.withColumn("hour",hour(col("timestamp")))

#Average temperature per location
avg_temp_df=updated_df.groupBy("location","hour").agg(
    avg("temperature").alias("average_temp_per_loc_per_hour")
)
avg_temp_df.display()

# COMMAND ----------

"""
 Social Media Analytics
📌 Scenario: A social media company wants to analyze user posts and engagements. The dataset contains user_id, post_id, content, hashtags, likes, comments, shares, and timestamp.

🔹 Task:

Extract hashtags and generate a hashtag popularity ranking (most used hashtags).
Find users whose posts got more than 1000 likes in a single day.
Detect fake engagements (users who like their own posts using multiple fake accounts).
Identify the most active hours for posting in a day.
Store aggregated insights in a Hive table."""

# COMMAND ----------

# Sample data for social media analytics
data = [
    (1, 101, "Check out this awesome sunrise! #sunrise #nature", ["sunrise", "nature"], 1200, 50, 10, "2025-01-15 06:30:00"),
    (2, 102, "Love this weather #rain #cozy", ["rain", "cozy"], 800, 30, 5, "2025-01-15 09:45:00"),
    (3, 103, "Best coffee ever! #coffee #morning", ["coffee", "morning"], 500, 25, 2, "2025-01-15 07:15:00"),
    (4, 104, "Amazing workout today! #fitness #health", ["fitness", "health"], 200, 10, 1, "2025-01-15 18:20:00"),
    (5, 105, "Can't believe this happened! #shocking #news", ["shocking", "news"], 3000, 500, 50, "2025-01-15 12:00:00"),
    (6, 106, "Exploring new places #travel #adventure", ["travel", "adventure"], 700, 40, 8, "2025-01-15 14:30:00"),
    (7, 107, "Relaxing at home #selfcare #relax", ["selfcare", "relax"], 150, 5, 0, "2025-01-15 20:45:00"),
    (8, 108, "Healthy eating is the best #food #health", ["food", "health"], 100, 15, 0, "2025-01-15 08:00:00"),
    (9, 109, "Best vacation ever #holiday #beach", ["holiday", "beach"], 2500, 350, 30, "2025-01-15 10:30:00"),
    (10, 110, "Nature is healing #nature #peace", ["nature", "peace"], 900, 70, 12, "2025-01-15 17:00:00"),
    (1, 111, "Another beautiful sunrise! #sunrise #sky", ["sunrise", "sky"], 1800, 80, 15, "2025-01-15 06:35:00"),
    (2, 112, "Rainy day feels #rain #chill", ["rain", "chill"], 600, 20, 3, "2025-01-15 09:50:00"),
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("user_id", IntegerType(), True),         # User ID (Integer)
    StructField("post_id", IntegerType(), True),         # Post ID (Integer)
    StructField("content", StringType(), True),          # Post content (String)
    StructField("hashtags", ArrayType(StringType()), True),  # Hashtags (Array of Strings)
    StructField("likes", IntegerType(), True),           # Number of likes (Integer)
    StructField("comments", IntegerType(), True),        # Number of comments (Integer)
    StructField("shares", IntegerType(), True),          # Number of shares (Integer)
    StructField("timestamp", StringType(), True)      # Timestamp of the post (Timestamp)
])

# Create DataFrame
social_media_df = spark.createDataFrame(data, schema)

social_media_df = social_media_df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType())
)
#display dataframe
social_media_df.display()

# COMMAND ----------

#Extract hashtags and generate a hashtag popularity ranking (most used hashtags).
flattened_df=social_media_df.select(
    col("user_id"),
    col("post_id"),
    col("content"),
    explode(col("hashtags")).alias("hashtag"),
    col("likes"),
    col("comments"),
    col("shares"),
    col("timestamp")
)

# Filter and count occurrences of each hashtag in the content
hashtag_counts = flattened_df.groupBy("hashtag").agg(
    count(col("content").like(f"%{col('hashtag')}%")).alias("occurrences")
)

hashtag_counts.show()

# COMMAND ----------

#Find users whose posts got more than 1000 likes in a single day.

# Add a date column
active_users_df=social_media_df.withColumn(
    "date",
    to_date(col("timestamp"))
)

# Filter rows within the 24-hour period
filtered_df = active_users_df.filter(
    (col("timestamp") >= "2025-01-15 00:00:00") &
    (col("timestamp") < "2025-01-16 00:00:00")  # 24-hour period
)

#finding user whose posts have more than 1000 likes
aggregated_df=filtered_df.groupBy("user_id","date").agg(
    sum("likes").alias("total_likes")
)

#Filter users whose total likes exceed 1000
final_df=aggregated_df.filter(col("total_likes") > 1000)

#show the result
final_df.show()


# COMMAND ----------

#Detect fake engagements (users who like their own posts using multiple fake accounts).
posts_df=social_media_df.select(
    col("post_id"),
    col("user_id")
)

likers_df=social_media_df.select(
    col("post_id"),
    col("user_id").alias("liker_id")
)

joined_df=posts_df.join(likers_df,on="post_id",how="inner")

# Filter for self-likes where liker_id matches user_id
self_likes_df = joined_df.filter(col("liker_id") == col("user_id"))

# Detect potential fake engagements
fake_engagement_df = joined_df.groupBy("user_id").agg(
    countDistinct("liker_id").alias("distinct_likers"),  # Number of unique likers
    count("liker_id").alias("total_likes")  # Total likes received
).filter(col("distinct_likers") < 3)  # Threshold: Less than 3 distinct likers

# Show results
print("Self-Likes:")
self_likes_df.show()

print("Fake Engagements (based on repeated patterns):")
fake_engagement_df.show()


# COMMAND ----------

#Identify the most active hours for posting in a day
df=social_media_df.withColumn(
    "hour",
    hour(col("timestamp"))
)

# Group by hour and count the total posts
final_df = df.groupBy("hour").agg(
    count("post_id").alias("total_posts")
).orderBy(desc("total_posts"))  # Correct usage of desc

# Show the result
final_df.show() 

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("caller_id", IntegerType(), True),
    StructField("receiver_id", IntegerType(), True),
    StructField("call_duration", DoubleType(), True),
    StructField("call_type", StringType(), True),
    StructField("call_timestamp", TimestampType(), True),
    StructField("location", StringType(), True)
])

# Sample data
data = [
    (1, 6, 120.0, "outgoing", datetime(2025, 1, 1, 8, 0, 0), "New York"),
    (1, 7, 4.0, "incoming", datetime(2025, 1, 1, 8, 2, 0), "New York"),
    (2, 8, 300.0, "outgoing", datetime(2025, 1, 1, 9, 0, 0), "Chicago"),
    (3, 9, 3.0, "incoming", datetime(2025, 1, 1, 10, 0, 0), "Houston"),
    (4, 10, 15.0, "outgoing", datetime(2025, 1, 1, 11, 0, 0), "San Francisco"),
    (5, 11, 45.0, "incoming", datetime(2025, 1, 1, 12, 0, 0), "Los Angeles"),
    (6, 12, 500.0, "outgoing", datetime(2025, 1, 1, 13, 0, 0), "New York"),
    (7, 13, 2.0, "incoming", datetime(2025, 1, 1, 14, 0, 0), "Chicago"),
    (8, 14, 120.0, "outgoing", datetime(2025, 1, 1, 15, 0, 0), "Houston"),
    (9, 15, 1.0, "incoming", datetime(2025, 1, 1, 16, 0, 0), "San Francisco"),
    (10, 16, 600.0, "outgoing", datetime(2025, 1, 1, 17, 0, 0), "Los Angeles"),
    (1, 6, 60.0, "outgoing", datetime(2025, 1, 2, 8, 0, 0), "New York"),
    (2, 7, 4.5, "incoming", datetime(2025, 1, 2, 8, 2, 0), "Chicago"),
    (3, 8, 320.0, "outgoing", datetime(2025, 1, 2, 9, 0, 0), "Houston"),
    (4, 9, 3.5, "incoming", datetime(2025, 1, 2, 10, 0, 0), "San Francisco"),
    (5, 10, 10.0, "outgoing", datetime(2025, 1, 2, 11, 0, 0), "Los Angeles"),
]

# Create DataFrame
cdr_df = spark.createDataFrame(data, schema=schema)

# displaying the DataFrame
cdr_df.display()


# COMMAND ----------

#Identify frequent callers (users making more than 20 calls per day).
#window specification
window_spec=Window.partitionBy("caller_id", "call_timestamp")

#creating new column date
cdr_df=cdr_df.withColumn(
    "call_date",
    to_date(col("call_timestamp"))
)
#finding total calls per user
callers_data_df=cdr_df.withColumn(
    "total_calls",
    count("*").over(window_spec)
)

#finding users who made more than 20 calls
final_df=callers_data_df.filter(col("total_calls") > 20)

#displaying dataframe
final_df.display()

# COMMAND ----------

#Find call drops (calls with duration < 5 seconds).
call_drops_df=cdr_df.filter(col("call_duration") < 5)

call_drops_df.show()

# COMMAND ----------

#Detect call forwarding chains (if caller A calls B and within 1 minute B calls C, mark it as call forwarding).
# Select columns with aliases to resolve ambiguity

initiated_caller_df = cdr_df.select(
    col("caller_id").alias("initiated_caller_id"),
    col("receiver_id").alias("initiated_receiver_id"),
    col("call_timestamp").alias("initiated_call_timestamp")
)

intermediate_caller_df = cdr_df.select(
    col("caller_id").alias("intermediate_caller_id"),
    col("receiver_id").alias("intermediate_receiver_id"),
    col("call_timestamp").alias("intermediate_call_timestamp")
)

# Perform the join on the relevant columns
joined_df = initiated_caller_df.join(
    intermediate_caller_df,
    initiated_caller_df["initiated_receiver_id"] == intermediate_caller_df["intermediate_caller_id"],
    how="inner"
)

#creating time diff column
time_diff_df=joined_df.withColumn(
    "time_diff",
    unix_timestamp(col("intermediate_call_timestamp")) - unix_timestamp(col("initiated_call_timestamp"))
).filter(col("time_diff") > 0)

#creating a new column is_forwarder
calls_forwarder_df=time_diff_df.withColumn(
    "is_forwarder",
    when(col("time_diff") < 60,"forwarder").otherwise("not")
)

#displaying the result
calls_forwarder_df.display()

# COMMAND ----------

#Generate a weekly call pattern per user (weekday vs. weekend usage trends).
cdr_df=cdr_df.withColumn(
    "day",
    dayofweek(col("call_timestamp"))
)

#window specification for calculating usage trend
window_spec=Window.partitionBy("caller_id","day")

#adding a new column usage trend
updated_df=cdr_df.withColumn(
    "usage_trend",
    sum("call_duration").over(window_spec)
) 

# Separate weekday and weekend usage before aggregation
weekday_df = updated_df.filter((col("day") >= 2) & (col("day") <= 6))
weekend_df = updated_df.filter((col("day") == 1) | (col("day") == 7))

# Aggregate usage trends for weekdays and weekends
weekday_usage_df=weekday_df.groupBy("caller_id").agg(
    sum("usage_trend").alias("weekday_usage")
    )

weekend_usage_df=weekend_df.groupBy("caller_id").agg(
    sum("usage_trend").alias("weekend_usage")
    )

# Combine weekday and weekend usage using join
final_df = weekday_usage_df.join(
    weekend_usage_df, on="caller_id", how="outer"
).fillna(0)

final_df.show()

# COMMAND ----------

"""
 Retail Sales Demand Forecasting
📌 Scenario: A retail store wants to analyze its daily sales to improve demand forecasting. The dataset contains store_id, product_id, category, sales_quantity, sales_amount, timestamp.

🔹 Task:

Compute daily sales trends for each product.
Identify best-selling products in each category.
Calculate moving average sales for the past 7 days.
Find stores that had no sales for 3 consecutive days (possible stock issue).
Predict the next day's sales using time series methods in PySpark."""

# COMMAND ----------

# Sample data for the DataFrame
data = [
    ("S1", "P101", "Electronics", 10, 5000.0, "2025-01-01 10:30:00"),
    ("S1", "P102", "Electronics", 5, 2500.0, "2025-01-01 11:00:00"),
    ("S2", "P103", "Groceries", 20, 600.0, "2025-01-01 12:00:00"),
    ("S1", "P104", "Groceries", 15, 450.0, "2025-01-02 10:00:00"),
    ("S3", "P105", "Furniture", 2, 4000.0, "2025-01-02 14:00:00"),
    ("S2", "P103", "Groceries", 10, 300.0, "2025-01-02 16:00:00"),
    ("S3", "P106", "Electronics", 8, 3200.0, "2025-01-03 09:30:00"),
    ("S1", "P102", "Electronics", 7, 3500.0, "2025-01-03 11:45:00"),
    ("S2", "P104", "Groceries", 12, 360.0, "2025-01-03 14:15:00"),
    ("S3", "P107", "Furniture", 1, 2000.0, "2025-01-03 15:30:00"),
    ("S1", "P101", "Electronics", 6, 3000.0, "2025-01-04 10:30:00"),
    ("S2", "P103", "Groceries", 25, 750.0, "2025-01-04 12:30:00"),
    ("S3", "P105", "Furniture", 3, 6000.0, "2025-01-04 13:00:00"),
    ("S1", "P108", "Groceries", 0, 0.0, "2025-01-05 11:00:00"),
    ("S2", "P109", "Groceries", 0, 0.0, "2025-01-05 14:30:00"),
    ("S3", "P110", "Furniture", 0, 0.0, "2025-01-05 16:00:00"),
]

# Define the schema
schema = StructType([
    StructField("store_id", StringType(), True),        # Store ID as a string
    StructField("product_id", StringType(), True),      # Product ID as a string
    StructField("category", StringType(), True),        # Product category as a string
    StructField("sales_quantity", IntegerType(), True), # Sales quantity as an integer
    StructField("sales_amount", FloatType(), True),     # Sales amount as a float
    StructField("timestamp", StringType(), True)     # Timestamp as a datetime
])

# Create the DataFrame
sales_df = spark.createDataFrame(data, schema)

# changing the timestamp datatype
sales_df=sales_df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType())
)

#display dataframe
sales_df.display()

# COMMAND ----------

#Compute daily sales trends for each product.

#Add date column
df=sales_df.withColumn(
    "date",
    to_date(col("timestamp"))
)
## Compute total daily sales per product
product_sales_trend=df.groupBy("product_id","date").agg(
    sum(col("sales_quantity")).alias("total_quantity_sold"),
    sum(col("sales_amount")).alias("total_sales_amount")
)
#show the result
product_sales_trend.show()

# COMMAND ----------

#Identify best-selling products in each category.
df=sales_df.groupBy("product_id","category").agg(
    sum(col("sales_quantity")*col("sales_amount")).alias("total_sales")
)
#window specification to give ranks for products 
window_spec=Window.partitionBy("category").orderBy(desc(col("total_sales")))

#creating rank column
ranking_df=df.withColumn(
    "rank",
    rank().over(window_spec)
)

#best selling products in each category
best_selling_products=ranking_df.filter(col("rank") == 1)

best_selling_products.show()

# COMMAND ----------

#Find stores that had no sales for 3 consecutive days (possible stock issue).

#Add new column
df=sales_df.withColumn(
    "date",
    to_date(col("timestamp"))
)
#aggregating dataframe
aggregated_df=df.groupBy("store_id","date").agg(
    sum("sales_amount").alias("total_sales_per_store")
)
#ordering dataframe
aggregated_df=aggregated_df.orderBy("store_id","date")

#window specification 
window_spec=Window.partitionBy("store_id").orderBy("date")

# Add a column to track consecutive no-sales days
updated_df = aggregated_df.withColumn(
    "is_no_sales", when(col("total_sales_per_store") == 0, 1).otherwise(0)
).withColumn(
    "consecutive_days_no_sales",
    sum("is_no_sales").over(window_spec.rowsBetween(-2, 0))  # Sum over 3 consecutive days
)

updated_df.show()
# Filter for stores with 3 consecutive days of no sales
stores_with_no_sales = updated_df.filter(col("consecutive_days_no_sales") == 3)

# Show the result
stores_with_no_sales.show()

# COMMAND ----------

#Calculate moving average sales for the past 7 days.

# Add a date column
df=sales_df.withColumn(
    "date",
    to_date(col("timestamp"))
)

# Define a window specification with a 7-day range
window_spec=Window.partitionBy("store_id").orderBy("date").rowsBetween(-7,0)

# Calculate 7-day moving average
final_df=df.withColumn(
    "moving_avg",
    avg("sales_amount").over(window_spec)
)

# Show results
final_df.show()

# COMMAND ----------

"""
Healthcare Patient Data Processing
📌 Scenario: A hospital has patient records including patient_id, admission_date, discharge_date, diagnosis, prescribed_medications (array), doctor_id, and hospital_id.

🔹 Task:

Flatten the prescribed_medications array so each medication is in a separate row.
Identify patients who had multiple hospital visits in 30 days.
Find doctors who have treated the most unique patients.
Calculate average length of stay per diagnosis.
Save patient treatment history as a Delta table"""

# COMMAND ----------

# Define schema for the DataFrame
schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("admission_date", StringType(), True),
    StructField("discharge_date", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("prescribed_medications", ArrayType(StringType()), True),
    StructField("doctor_id", IntegerType(), True),
    StructField("hospital_id", IntegerType(), True)
])

# Create sample data
data = [
    (1, "2025-01-01", "2025-01-05", "Flu", ["Paracetamol", "Ibuprofen"], 101, 201),
    (2, "2025-01-02", "2025-01-08", "COVID-19", ["Remdesivir", "Dexamethasone"], 102, 202),
    (3, "2025-01-03", "2025-01-07", "Diabetes", ["Insulin"], 103, 201),
    (1, "2025-01-10", "2025-01-15", "Flu", ["Paracetamol", "Aspirin"], 101, 201),
    (4, "2025-01-06", "2025-01-12", "Hypertension", ["Amlodipine"], 104, 203),
    (5, "2025-01-08", "2025-01-15", "Asthma", ["Salbutamol", "Budesonide"], 105, 202),
    (2, "2025-01-20", "2025-01-25", "COVID-19", ["Remdesivir", "Azithromycin"], 102, 202),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df = df.withColumn("admission_date", to_date("admission_date", "yyyy-MM-dd")) \
       .withColumn("discharge_date", to_date("discharge_date", "yyyy-MM-dd"))
#show the result       
df.show(truncate=False)

# COMMAND ----------

#df.printSchema()
#Calculate average length of stay per diagnosis.

updated_df=df.withColumn(
    "stayed_days",
    datediff(col("discharge_date"), col("admission_date"))
)
updated_df.display()
#window specification to find avg stay
window_spec=Window.partitionBy("diagnosis")

avg_stay_per_diagnosis=updated_df.withColumn(
    "avg_stay",
    avg("stayed_days").over(window_spec)
)

# Show the final result
avg_stay_per_diagnosis.select("diagnosis", "avg_stay").distinct().show()

# COMMAND ----------

#Find doctors who have treated the most unique patients.

# Group by doctor_id and count distinct patient_id
doctors_df=df.groupBy("doctor_id").agg(
    countDistinct("patient_id").alias("unique_patients_per_doctor")
)


# Filter doctors who treated more than one unique patient 
filtered_df=doctors_df.filter(col("unique_patients_per_doctor") > 1)

# Show the results (sorted by the number of unique patients)
filtered_df.orderBy(desc(col("unique_patients_per_doctor"))).show()

# COMMAND ----------

# Convert string dates to date type (if not already done)
df = df.withColumn("admission_date", to_date("admission_date", "yyyy-MM-dd"))

# Filter data to include hospital visits within 30 days from a given start date
start_date = "2025-01-01"
end_date = date_add(to_date(lit(start_date)), 30)


# Identify patients who had multiple hospital visits in 30 days
patients_his_df = df.filter(col("admission_date").between(start_date, end_date)) \
    .groupBy("patient_id") \
    .agg(count("*").alias("total_visits"))\
    .filter(col("total_visits") > 1)   

# Show the top 3 patients who visited the hospital the most
most_visited_patients = patients_his_df.orderBy(desc("total_visits")).limit(3)

most_visited_patients.show()


# COMMAND ----------

updated_df=df.select(
    col("patient_id"),
    col("admission_date"),
    col("discharge_date"),
    col("diagnosis"),
    explode(col("prescribed_medications")).alias("medications"),
    col("doctor_id"),
    col("hospital_id")
)
updated_df.show()

# COMMAND ----------

"""
 Financial Market Data Processing
📌 Scenario: A stock exchange records stock prices in a dataset with stock_id, company_name, opening_price, closing_price, high, low, traded_volume, timestamp.

🔹 Task:

Compute daily stock returns for each company.
Identify stocks with the highest volatility (difference between high and low).
Find stocks where the closing price dropped more than 5% compared to the previous day.
Generate a monthly stock performance report.
Store final stock data in Parquet format"""

# COMMAND ----------

# Sample data
data = [
    (1, "Company A", 100.0, 105.0, 110.0, 98.0, 5000, "2023-12-01 10:30:00"),
    (2, "Company B", 200.0, 190.0, 205.0, 185.0, 7000, "2023-12-01 10:30:00"),
    (3, "Company C", 300.0, 310.0, 315.0, 295.0, 10000, "2023-12-01 10:30:00"),
    (1, "Company A", 105.0, 102.0, 108.0, 100.0, 4500, "2023-12-02 10:30:00"),
    (2, "Company B", 190.0, 195.0, 200.0, 185.0, 6500, "2023-12-02 10:30:00"),
    (3, "Company C", 310.0, 305.0, 320.0, 300.0, 9500, "2023-12-02 10:30:00"),
    (1, "Company A", 102.0, 107.0, 110.0, 101.0, 4000, "2023-12-03 10:30:00"),
    (2, "Company B", 195.0, 192.0, 200.0, 190.0, 6000, "2023-12-03 10:30:00"),
    (3, "Company C", 305.0, 315.0, 320.0, 305.0, 9000, "2023-12-03 10:30:00"),
]

# Define schema
schema = StructType([
    StructField("stock_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("opening_price", DoubleType(), True),
    StructField("closing_price", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("traded_volume", IntegerType(), True),
    StructField("timestamp", StringType(), True),
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df=df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType())
)

df.show()

# COMMAND ----------

#Generate a monthly stock performance report.

# adding date month and year from a 'date' column
df = df.withColumn("date", to_date(col("timestamp"))) \
       .withColumn("month", month(col("date"))) \
       .withColumn("year", year(col("date")))

aggregated_df=df.groupBy("stock_id","month","year").agg(
    sum("traded_volume").alias("monthly_performance_per_stock")
)

#window specification to rank stocks by using monthly performance
window_spec=Window.partitionBy("month","year").orderBy(desc("monthly_performance_per_stock"))

#adding rank 
montly_stock_per=aggregated_df.withColumn(
    "rank_for_stock",
    rank().over(window_spec)
)

#show the result
monthly_stock_per.show()

# COMMAND ----------

#Find stocks where the closing price dropped more than 5% compared to the previous day.

#adding new column
df=df.withColumn("date",to_date(col("timestamp")))\
    .withColumnRenamed("closing_price", "current_day_closing_price")

#window specification to get the previous day's closing price
window_spec=Window.partitionBy("stock_id").orderBy("date")

# Add previous day's closing price and calculate percentage change 
df_with_new_column = df.withColumn(
    "previous_day_closing_price", 
    lag("current_day_closing_price").over(window_spec)
).filter(
    col("previous_day_closing_price").isNotNull()
).withColumn(
    "percentage_change",
    ((col("previous_day_closing_price") - col("current_day_closing_price")) / col("previous_day_closing_price")) * 100
)

# Filter stocks where the closing price dropped more than 5%
dropped_stocks_df=df_with_new_column.filter(col("percentage_change") > 5)

#show the result
dropped_stocks_df.show()

# COMMAND ----------

#Identify stocks with the highest volatility (difference between high and low)

#adding new column
df=df.withColumn(
    "date",
    to_date(col("timestamp"))
)

#creating a volatility column
df_with_volatility=df.withColumn(
    "volatility",
    col("high") - col("low")
)

#window specification
window_spec=Window.partitionBy("date").orderBy(desc("volatility"))

#identifying hihest volatility stocks on each date
highest_volatility_stocks_df=df_with_volatility.withColumn(
    "rank_for_volatility_stocks",
    rank().over(window_spec)
)

#show the result
highest_volatility_stocks_df.show()

# COMMAND ----------

#Compute daily stock returns for each company.

#adding new column
df=df.withColumn(
    "date",
    to_date(col("timestamp"))
)

#finding difference between prices
diff_b/w_price_df=df.withColumn(
    "price_diff",
    ((col("closing_price") - col("opening_price")) / col("opening_price")) * 100
)

#window spec
window_spec=Window.partitionBy("company_name","date").orderBy(desc("price_diff"))

#giving ranks for stocks on each company
df_with_daily_stocks_returns=diff_b/w_price_df.withColumn(
    "rank_for_stocks",
    rank().over(window_spec)
)

#show the result
df_with_daily_stocks_returns.show()

# COMMAND ----------

"""
Employee and Department Analysis
📌 Scenario: You have two datasets:

employees: employee_id, name, department_id, salary, hire_date
departments: department_id, department_name, manager_id
🔹 Task:

Perform an inner join to get the department name for each employee.
Find the total salary paid in each department.
Identify departments with no employees using a left join.
Find the employee with the highest salary in each department using a window function.
Save the enriched dataset with employee and department details to a Delta table."""

# COMMAND ----------

# Sample data for employees
employee_data = [
    (1, "John", 101, 50000.0, "2020-01-15"),
    (2, "Alice", 102, 60000.0, "2018-03-20"),
    (3, "Bob", 101, 55000.0, "2019-07-11"),
    (4, "Eve", None, 40000.0, "2021-05-05"),
    (5, "Charlie", 103, 70000.0, "2017-09-25"),
]

employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("salary", FloatType(), True),
    StructField("hire_date", StringType(), True),
])

# Sample data for departments
department_data = [
    (101, "Sales", 10),
    (102, "Engineering", 20),
    (103, "HR", 30),
    (104, "Finance", 40),
]

department_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department_name", StringType(), True),
    StructField("manager_id", IntegerType(), True),
])

# Create DataFrames
employee_df = spark.createDataFrame(data=employee_data, schema=employee_schema)
department_df = spark.createDataFrame(data=department_data, schema=department_schema)

# Show the DataFrames
employee_df=employee_df.withColumn("hire_date",to_date(col("hire_date")))
employee_df.show()
department_df.show()

# COMMAND ----------

#Identify departments with no employees using a left join.

#joining datsets with left join
joined_df=department_df.join(employee_df,on="department_id",how="left")

#identifying the departmenst with no emplyees
departemts_with_no_emp=joined_df.select("department_id","department_name").filter(col("employee_id").isNull())

#show the result
departemts_with_no_emp.show()

# COMMAND ----------

#Perform an inner join to get the department name for each employee.
joined_df=employee_df.join(department_df,on="department_id",how="inner")
joined_df.show()

#Find the employee with the highest salary in each department using a window function.

#window specification 
window_spec=Window.partitionBy("department_id").orderBy(desc("salary"))

#giving rank for every employee in each department
highest_salary_employee= joined_df.withColumn(
    "rank_for_employee",
    rank().over(window_spec)
)
highest_salary_employee.filter(col(rank_for_employee) == 1).show()

#Find the total salary paid in each department.
total_sal_per_dep_df=joined_df.groupBy("department_id").agg(
    sum("salary").alias("total_salary_per_department")
)

total_sal_per_dep_df.show()

# COMMAND ----------

"""
Product and Order Analysis
📌 Scenario: Two datasets:

products: product_id, product_name, category, price
orders: order_id, product_id, customer_id, order_date, quantity
🔹 Task:

Perform an inner join to get product details for each order.
Calculate the total revenue generated by each product category.
Use a left anti join to identify products that have never been ordered.
Identify the top 5 products in terms of quantity sold.
Save the enriched orders dataset in Parquet format."""

# COMMAND ----------

# Sample data for products
product_data = [
    (1, "Laptop", "Electronics", 800.0),
    (2, "Smartphone", "Electronics", 600.0),
    (3, "Tablet", "Electronics", 300.0),
    (4, "Headphones", "Accessories", 50.0),
    (5, "Backpack", "Accessories", 70.0),
    (6, "Notebook", "Stationery", 10.0),
    (7, "Pen", "Stationery", 2.0),
]

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
])

# Sample data for orders
order_data = [
    (101, 1, 1001, "2023-10-01", 2),
    (102, 2, 1002, "2023-10-02", 1),
    (103, 3, 1003, "2023-10-03", 4),
    (104, 4, 1004, "2023-10-04", 3),
    (105, 2, 1005, "2023-10-05", 2),
    (106, 6, 1006, "2023-10-06", 5),
]

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
])

# Create DataFrames
product_df = spark.createDataFrame(data=product_data, schema=product_schema)
order_df = spark.createDataFrame(data=order_data, schema=order_schema)

# Show the DataFrames
product_df.show()
order_df=order_df.withColumn(
    "order_date",
    to_date(col("order_date"))
)
order_df.show()

# COMMAND ----------

#Perform an inner join to get product details for each order.
joined_df=product_df.join(order_df,on="product_id",how="inner")
#show the result
joined_df.show()

#Use a left anti join to identify products that have never been ordered.
products_with_no_orders_df=product_df.join(order_df, on="product_id", how="left_anti")
products_with_no_orders_df.show()

#Calculate the total revenue generated by each product category.

#calculating total revenue for each category
total_revenue_per_cat=joined_df.withColumn(
    "revenue",
    col("quantity") * col("price")
).groupBy("category").agg(
    sum("revenue").alias("total_revenue_per_category")
)
#show the result
total_revenue_per_cat.show()


#Identify the top 5 products in terms of quantity sold.

#window specification for rank
windowForRank=Window.orderBy(desc("total_quantity"))

# Calculate total quantity sold for each product
total_quantity_per_product = joined_df.groupBy("product_id", "product_name").agg(
    sum("quantity").alias("total_quantity")
)
df=total_quantity_per_product.withColumn(
    "rank",
    rank().over(windowForRank)
)


top_5_products=df.filter(col("rank") <= 5).show()
top_5_products.show()

# COMMAND ----------

"""
Log Data Analysis for Web Traffic
📌 Scenario: A web analytics company tracks website visits with fields user_id, session_id, page_url, event_type, event_timestamp, device_type, location.

🔹 Task:

Identify unique users per day.
Find the most visited pages in the last 7 days.
Detect bot traffic (same user sending multiple requests in <1 second).
Calculate average session duration per user.
Store insights in a Hive table for reporting.
"""

# COMMAND ----------

# Sample data for web traffic logs
log_data = [
    (1, "sess_001", "/home", "view", "2025-01-20 10:05:00", "mobile", "USA"),
    (1, "sess_001", "/product", "view", "2025-01-20 10:05:30", "mobile", "USA"),
    (2, "sess_002", "/home", "view", "2025-01-20 10:10:00", "desktop", "UK"),
    (3, "sess_003", "/home", "view", "2025-01-20 10:15:00", "mobile", "India"),
    (3, "sess_003", "/cart", "click", "2025-01-20 10:15:30", "mobile", "India"),
    (3, "sess_003", "/checkout", "view", "2025-01-20 10:16:00", "mobile", "India"),
    (4, "sess_004", "/home", "view", "2025-01-20 10:20:00", "tablet", "USA"),
    (5, "sess_005", "/home", "view", "2025-01-20 10:25:00", "mobile", "Germany"),
    (5, "sess_005", "/product", "click", "2025-01-20 10:25:05", "mobile", "Germany"),
    (5, "sess_005", "/cart", "click", "2025-01-20 10:25:10", "mobile", "Germany"),
    (5, "sess_005", "/product", "view", "2025-01-20 10:25:10", "mobile", "Germany"),
    (6, "sess_006", "/home", "view", "2025-01-20 10:30:00", "desktop", "Canada"),
]

log_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
])

# Create DataFrame
log_df = spark.createDataFrame(data=log_data, schema=log_schema)

# Show the DataFrame
log_df = log_df.withColumn(
    "event_timestamp",
    col("event_timestamp").cast(TimestampType())
)

log_df.show()

# COMMAND ----------

#Calculate average session duration per user
log_df = log_df.withColumn(
    "event_timestamp",
    unix_timestamp(col("event_timestamp"))
)
avg_session_per_user = log_df.groupBy("user_id").agg(
    avg("event_timestamp").alias("average_session")
)

updated_df = avg_session_per_user.withColumn(
    "average_session",
    from_unixtime(col("average_session"))
)

updated_df.show()

# COMMAND ----------

spark = SparkSession.builder \
    .appName("HiveTableExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS web_analytics.avg_session_duration (
        user_id INT,
        average_session STRING
    )
""")

# COMMAND ----------

# Save the DataFrame as a Hive table
updated_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("web_analytics.avg_session_duration")

# COMMAND ----------

spark.sql("select * from web_analytics.avg_session_duration").show()

# COMMAND ----------

#Detect bot traffic (same user sending multiple requests in <1 second).

#window specification
windowspec=Window.partitionBy("user_id").orderBy("event_timestamp")

#identifying previous timestamp of the request
log_df_with_lag=log_df.withColumn(
    "previous_event_timestamp",
    lag("event_timestamp").over(windowspec)
)
log_df_with_lag = log_df_with_lag.filter(col("previous_event_timestamp").isNotNull())

log_df_with_time_diff = log_df_with_lag.withColumn(
    "time_diff",
    unix_timestamp(col("event_timestamp")) - unix_timestamp(col("previous_event_timestamp"))
)
bot_traffic_df = log_df_with_time_diff.filter(col("time_diff")<1)

bot_traffic_df.show()

# COMMAND ----------

# Create Hive database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS web_analytics")

# COMMAND ----------

#Identify unique users per day.

#adding event date to dataset
log_df=log_df.withColumn(
    "event_date",
    to_date(col("event_timestamp"))
)
unique_users_per_day_df=log_df.groupBy("event_date").agg(
    countDistinct("user_id").alias("unique_users")
)

# Show the result
unique_users_per_day_df.show(truncate=False)

# COMMAND ----------

#Find the most visited pages in the last 7 days.

#Filter data between the last seven days
today_date = "2025-01-20"
last_seven_days_date = date_add(to_date(lit(today_date)), -7)

# Add a derived column for the event date
log_df = log_df.withColumn("event_date", to_date(col("event_timestamp")))

# Filter data for the last 7 days 
filtered_df=log_df.filter(col("event_date").between(last_seven_days_date, today_date))

# Group by page_url and calculate the total visits
most_visited_df=filtered_df.groupBy("page_url").agg(
    count("*").alias("total_visits")
)

# Order by total visits in descending order
most_visited_df.orderBy(desc("total_visits")).show()

# COMMAND ----------

"""
Supplier and Product Analysis
📌 Scenario: Two datasets:

suppliers: supplier_id, name, location, rating
products: product_id, supplier_id, product_name, price
🔹 Task:

Perform a join to get supplier details for each product.
Identify suppliers who provide more than 10 products.
Use a left anti join to find suppliers who have not supplied any products.
Find the supplier with the highest-rated products (average rating).
Save the enriched dataset in Parquet format."""

# COMMAND ----------

# Define schema for suppliers
suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("rating", FloatType(), True)
])

# Define schema for products
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", FloatType(), True)
])

# Create sample data for suppliers
suppliers_data = [
    (1, "ABC Suppliers", "New York", 4.5),
    (2, "DEF Distributors", "Chicago", 3.8),
    (3, "GHI Wholesalers", "Los Angeles", 4.2),
    (4, "JKL Traders", "Houston", 4.0),
    (5, "MNO Suppliers", "Miami", 3.5)
]

# Create sample data for products
products_data = [
    (101, 1, "Laptop", 800.0),
    (102, 2, "Smartphone", 600.0),
    (103, 1, "Monitor", 150.0),
    (104, 3, "Keyboard", 40.0),
    (105, 3, "Mouse", 25.0),
    (106, 2, "Tablet", 300.0),
    (107, 4, "Printer", 200.0),
    (108, 1, "Speaker", 100.0),
    (109, 2, "Charger", 20.0),
    (110, 1, "Webcam", 80.0)
]

# Create DataFrames
suppliers_df = spark.createDataFrame(suppliers_data, schema=suppliers_schema)
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Show DataFrames
suppliers_df.write.format("csv").option("header", "true").mode("overwrite").save("dbfs:/FileStore/raw_layer/suppliers")
products_df.write.format("csv").option("header","true").mode("overwrite").save("dbfs:/FileStore/raw_layer/products")

# COMMAND ----------

#Perform a join to get supplier details for each product
joined_df=suppliers_df.join(products_df, on="supplier_id", how="inner")
joined_df.show()

#Identify suppliers who provide more than 10 products.
df=joined_df.groupBy("supplier_id").agg(
    count("product_id").alias("total_products")
)
#filtering the suppliers with 10 products
suppliers_with_10_products=df.filter(col("total_products") > 10)
suppliers_with_10_products.show()

#Find the supplier with the highest-rated products (average rating).
df=joined_df.groupBy("supplier_id").agg(
    avg("rating").alias("average_rating")
)
supplier_with_highest_rating=df.orderBy(desc("average_rating")).limit(1)
supplier_with_highest_rating.show()

#Use a left anti join to find suppliers who have not supplied any products.
suppliers_who_not_supplied=suppliers_df.join(products_df, on="supplier_id", how="leftanti")
suppliers_who_not_supplied.show()

# COMMAND ----------

"""
Event Ticket Booking Platform
📌 Scenario: Two datasets:

events: event_id, event_name, location, date, price
bookings: booking_id, user_id, event_id, booking_date
🔹 Task:

Perform a join to get event details for each booking.
Find events with the highest number of bookings.
Use a left anti join to find events that have no bookings.
Calculate the total revenue generated by each event.
Save the enriched dataset in Delta format."""

# Define schema for events
events_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("event_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("date", StringType(), True),
    StructField("price", FloatType(), True)
])

# Define schema for bookings
bookings_schema = StructType([
    StructField("booking_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("booking_date", StringType(), True)
])

# Create sample data for events
events_data = [
    (1, "Music Concert", "New York", "2025-02-15", 100.0),
    (2, "Tech Conference", "San Francisco", "2025-03-10", 200.0),
    (3, "Art Exhibition", "Chicago", "2025-01-30", 50.0),
    (4, "Food Festival", "Los Angeles", "2025-04-05", 80.0),
    (5, "Comedy Show", "Houston", "2025-02-20", 60.0)
]

# Create sample data for bookings
bookings_data = [
    (101, 1, 1, "2025-02-01"),
    (102, 2, 2, "2025-02-05"),
    (103, 3, 1, "2025-02-07"),
    (104, 4, 3, "2025-01-20"),
    (105, 5, 1, "2025-02-10"),
    (106, 6, 2, "2025-03-01"),
    (107, 7, 4, "2025-03-15")
]

# Create DataFrames
events_df = spark.createDataFrame(events_data, schema=events_schema)
bookings_df = spark.createDataFrame(bookings_data, schema=bookings_schema)

# Show DataFrames
events_df=events_df.withColumn("date",to_date(col("date")))
events_df.write.format("csv").option("header",True).mode("overwrite").save("dbfs:/FileStore/raw_layer/events")
bookings_df=bookings_df.withColumn("booking_date",to_date(col("booking_date")))
bookings_df.write.format("csv").option("header", True).mode("overwrite").save("dbfs:/FileStore/raw_layer/bookings")

# COMMAND ----------

#Perform a join to get event details for each booking.
joined_df=events_df.join(bookings_df, "event_id", "inner")
joined_df.show()

#Find events with the highest number of bookings.
df=joined_df.groupBy("event_id").agg(
    count("booking_id").alias("total_bookings")
)
events_with_highest_bookings=df.orderBy(desc("total_bookings"))
events_with_highest_bookings.show()

#Calculate the total revenue generated by each event.
df=joined_df.groupBy("event_id").agg(
    sum("price").alias("total_revenue")
)
df.orderBy(desc("total_revenue")).show()

#Use a left anti join to find events that have no bookings.
events_with_no_bookings=events_df.join(bookings_df, "event_id","leftanti")
events_with_no_bookings.show()

# COMMAND ----------

"""
Airline Flight Delay Prediction
📌 Scenario: An airline wants to analyze flight delays based on flight_id, departure_time, arrival_time, actual_departure_time, actual_arrival_time, airline, origin, destination, weather_conditions.

🔹 Task:

Calculate the delay duration for each flight.
Find flights delayed due to bad weather conditions.
Identify most delayed airlines on average.
Detect flights that departed late but arrived on time (recovery time).
Save cleaned data in Delta format."""

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("flight_id", IntegerType(), True),
    StructField("departure_time", TimestampType(), True),
    StructField("arrival_time", TimestampType(), True),
    StructField("actual_departure_time", TimestampType(), True),
    StructField("actual_arrival_time", TimestampType(), True),
    StructField("airline", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("weather_conditions", StringType(), True)
])

# Define schema
schema = StructType([
    StructField("flight_id", IntegerType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("actual_departure_time", StringType(), True),
    StructField("actual_arrival_time", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("weather_conditions", StringType(), True)
])

# Sample data (ensure order matches schema)
data = [
    (1, "2025-01-25 08:00:00", "2025-01-25 10:00:00", "2025-01-25 08:30:00", "2025-01-25 10:30:00", "AirlineA", "JFK", "LAX", "Clear"),
    (2, "2025-01-25 09:00:00", "2025-01-25 11:00:00", "2025-01-25 09:15:00", "2025-01-25 11:05:00", "AirlineB", "SFO", "ORD", "Rain"),
    (3, "2025-01-25 07:00:00", "2025-01-25 09:00:00", "2025-01-25 07:30:00", "2025-01-25 09:00:00", "AirlineA", "LAX", "DFW", "Fog"),
    (4, "2025-01-25 06:30:00", "2025-01-25 08:30:00", "2025-01-25 06:45:00", "2025-01-25 08:45:00", "AirlineC", "SEA", "DEN", "Snow"),
    (5, "2025-01-25 12:00:00", "2025-01-25 14:00:00", "2025-01-25 12:00:00", "2025-01-25 14:00:00", "AirlineB", "ATL", "MIA", "Clear"),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df = df.withColumn(
    "departure_time",
    col("departure_time").cast(TimestampType())
).withColumn(
    "arrival_time",
    col("arrival_time").cast(TimestampType())
).withColumn(
    "actual_departure_time",
    col("actual_departure_time").cast(TimestampType())
).withColumn(
    "actual_arrival_time",
    col("actual_arrival_time").cast(TimestampType())
)

df.display()

# COMMAND ----------

##Detect flights that departed late but arrived on time (recovery time).
flights_with_different_conditions = df.filter(
    (unix_timestamp(col("actual_departure_time")) > unix_timestamp(col("departure_time"))) &
    (unix_timestamp(col("actual_arrival_time")) == unix_timestamp(col("arrival_time")))
) 

#show the result
flights_with_different_conditions.display() 

# COMMAND ----------

#Calculate the delay duration for each flight.
flight_delay_duration = df.withColumn(
    "delay_duration",
    (unix_timestamp(col("actual_departure_time")) - unix_timestamp(col("departure_time"))) / 60
)
flight_delay_duration.display()

#Find flights delayed due to bad weather conditions.
delayed_flights = flight_delay_duration.filter(
    ((col("weather_conditions") == "Rain") | 
    (col("weather_conditions") == "Fog") | 
    (col("weather_conditions") == "Snow")) & 
    (col("delay_duration") > 0)
)
#show the result    
delayed_flights.display()

#Identify most delayed airlines on average
avg_delayed_airlines = flight_delay_duration.groupBy("airline").agg(
    avg("delay_duration").alias("avg_delay_duration")
)

# Order airlines by the highest average delay
most_delayed_airlines = avg_delayed_airlines.orderBy(desc("avg_delay_duration"))

#show the result
most_delayed_airlines.show()


# COMMAND ----------

