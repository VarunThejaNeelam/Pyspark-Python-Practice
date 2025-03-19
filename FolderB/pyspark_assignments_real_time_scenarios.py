# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

"""
Complex Data Transformations on E-commerce Transactions
Dataset: Simulated e-commerce transactions with fields:
order_id, customer_id, product_id, order_date, quantity, unit_price, category, payment_method.

Tasks:
Load the dataset as a DataFrame.
Calculate total revenue per order.
Identify the top 5 customers based on total spending in the last 3 months.
Determine most popular product categories (rank them using dense_rank() in a window function).
Identify fraudulent transactions (orders where the same customer placed multiple orders within 5 minutes using different payment methods).
💡 Bonus: Save results as a Delta table and apply OPTIMIZE & VACUUM.
"""

# COMMAND ----------

# Define Schema (Only `order_date` is STRING initially)
transactions_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),  # Date column as STRING initially
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("payment_method", StringType(), True)
])

# Sample Data
transactions_data = [
    (1001, "C001", "P100", "2024-03-08 10:05:00", 2, 20.00, "Electronics", "Credit Card"),
    (1002, "C002", "P101", "2024-03-08 11:15:00", 1, 50.00, "Clothing", "PayPal"),
    (1003, "C003", "P102", "2024-03-08 11:17:00", 3, 15.00, "Home", "Credit Card"),
    (1004, "C001", "P103", "2024-03-08 10:07:00", 1, 25.00, "Electronics", "Debit Card"),
    (1005, "C004", "P104", "2024-03-08 12:30:00", 2, 30.00, "Beauty", "Credit Card"),
    (1006, "C005", "P105", "2024-03-08 12:35:00", 1, 100.00, "Furniture", "PayPal"),
    (1007, "C003", "P106", "2024-03-08 11:20:00", 2, 45.00, "Home", "Debit Card"),
    (1008, "C001", "P107", "2024-03-08 10:09:00", 1, 40.00, "Electronics", "Credit Card"),
    (1009, "C002", "P108", "2024-03-08 11:45:00", 2, 35.00, "Clothing", "Credit Card"),
    (1010, "C004", "P109", "2024-03-08 12:32:00", 3, 20.00, "Beauty", "Debit Card"),
]

# Create DataFrame
transactions_df = spark.createDataFrame(transactions_data, transactions_schema)

# Show DataFrame
transactions_df = transactions_df.withColumn(
    "order_date",
    col("order_date").cast("timestamp")
)
transactions_df.show()


# COMMAND ----------

# Identify fraudulent transactions (orders where the same customer placed multiple orders within 5 minutes using different payment methods).

# Used lag to check previous order payment metod for customers
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
customers_checking_df = transactions_df.withColumn(
    "prev_order_method",
    lag("payment_method").over(window_spec)
)

# Checking the transations payment methods 
updated_df = customers_checking_df.withColumn(
    "pay_type_streak"
    when((col("payment_method") != col("prev_order_method")) | col("prev_order_method").isNull(), 1)
    .otherwise(0)
)

# Aggregating assign values 
aggregated_df = updated_df.groupBy(col("customer_id"), window(col("order_date"), "5 minutes")).agg(
    sum("pay_type_streak").alias("payment_types")
)

# Labelling customers
labelled_df = aggregated_df.withColumn(
    "label",
    when(col("payment_types") > 1, "fraud_transaction")
).filter(col("payment_types") > 0)

# Filtering the dataframe
final_df = labelled_df.select("customer_id", "label")
final_df.show()

# COMMAND ----------

# Calculate total revenue per order.

# Aggregating total revenue per order 
aggregated_df = transactions_df.groupBy("order_id").agg(
    sum(col("quantity") * col("unit_price")).alias("total_revenue")
)
aggregated_df.show()

# Identify the top 5 customers based on total spending in the last 3 months.
grouped_df = transactions_df.filter(col("order_date") >= add_months(-3)).groupBy("customer_id").agg(
    sum("unit_price").alias("total_spending")
)

# Ranking customers based on total spending
window_spec = Window.orderBy(desc("total_spending"))
ranking_df = grouped_df.withColumn(
    "rank",
    dense_rank().over(window_spec)
)
top5_customers_df = ranking_df.filter(col("rank") <= 5)
top5_customers_df.show()

# Determine most popular product categories (rank them using dense_rank() in a window function).

# Aggregating total revenue per category
aggregated_df = transactions_df.groupBy("category").agg(
    sum(col("quantity") * col("unit_price")).alias("total_revenue")
)

# Ranking categories based on total revenue
window_rank = Window.orderBy(desc("total_revenue"))
ranking_df = aggregated_df.withColumn(
    "rank",
    dense_rank().over(window_rank)
)
final_df = ranking_df.orderBy("rank")
final_df.show()


# COMMAND ----------

"""
Music Streaming Analytics
Tables:

Users:
user_id (Primary Key)
name
signup_date
Songs:
song_id (Primary Key)
title
artist_id
Plays:
play_id (Primary Key)
user_id (Foreign Key)
song_id (Foreign Key)
play_date
Tasks:
Find the top 5 most-played songs in each month.
Identify users whose song play count increased by at least 50% in the last month compared to the previous month.
Find the longest streak of consecutive days a user has played a song."""

# COMMAND ----------

# Sample Users DataFrame
users_data = [
    (1, "Alice", "2024-01-10"),
    (2, "Bob", "2023-12-20"),
    (3, "Charlie", "2023-11-15"),
    (4, "David", "2024-02-01"),
    (5, "Eve", "2024-02-10"),
]

users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("signup_date", StringType(), True)  # Initially as string
])

users_df = spark.createDataFrame(users_data, schema=users_schema)

# Convert signup_date to DateType
users_df = users_df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

# Sample Songs DataFrame
songs_data = [
    (101, "Song A", 201),
    (102, "Song B", 202),
    (103, "Song C", 203),
    (104, "Song D", 204),
    (105, "Song E", 205),
]

songs_schema = StructType([
    StructField("song_id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("artist_id", IntegerType(), True),
])

songs_df = spark.createDataFrame(songs_data, schema=songs_schema)

# Sample Plays DataFrame
plays_data = [
    (1001, 1, 101, "2024-02-01"),
    (1002, 1, 102, "2024-02-02"),
    (1003, 1, 103, "2024-02-03"),
    (1004, 2, 101, "2024-01-20"),
    (1005, 2, 101, "2024-01-21"),
    (1006, 3, 103, "2024-02-10"),
    (1007, 3, 103, "2024-02-11"),
    (1008, 3, 103, "2024-02-12"),
    (1009, 4, 104, "2024-02-05"),
    (1010, 5, 105, "2024-02-06"),
]

plays_schema = StructType([
    StructField("play_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("song_id", IntegerType(), False),
    StructField("play_date", StringType(), True)  # Initially as string
])

plays_df = spark.createDataFrame(plays_data, schema=plays_schema)

# Convert play_date to DateType
plays_df = plays_df.withColumn("play_date", to_date(col("play_date"), "yyyy-MM-dd"))

# Show DataFrames
users_df.show()
songs_df.show()
plays_df.show()


# COMMAND ----------

# Find the top 5 most-played songs in each month.

plays_df = plays_df.withColumn(
    "year_month",
    date_format(col("play_date"), "yyyy-MM")
)

aggregated_df = plays_df.groupBy("year_month", "song_id").agg(
    count("*").alias("total_play_count")
)

window_spec = Window.partitionBy("year_month").orderBy(desc("total_play_count"))
ranking_df = aggregated_df.withColumn(
    "rank",
    dense_rank().over(window_spec)
)
filtered_df = ranking_df.filter(col("rank") <= 5)
filtered_df.show()

# COMMAND ----------

# Find the longest streak of consecutive days a user has played a song.

# Creating a row number column 
window_spec = Window.partitionBy("user_id").orderBy("play_date")
df = plays_df.withColumn(
    "rn",
    row_number().over(window_spec)
)

# Creating a group for user based on there consequtive behaviour
updated_df = df.withColumn(
    "grp",
    date_sub(col("play_date"), col("rn"))
)

# Aggregating to count longest streak by grouping by userid and grp
aggregated_df = updated_df.groupBy("user_id", "grp").agg(
    count("*").alias("streak_count")
)

# Ranking consequtive groups for each user to get highest consequtive streak
window_rank = Window.partitionBy("user_id").orderBy(desc("streak_count"))
ranking_df = aggregated_df.withColumn(
    "rank",
    dense_rank().over(window_rank)
)

# Filtering highest streak for every user
final_df = ranking_df.filter(col("rank") == 1).select("user_id", "streak_count")
final_df.show()

# COMMAND ----------

# Identify users whose song play count increased by at least 50% in the last month compared to the previous month.

# Joined the both users and plays dataframes
joined_df = users_df.join(plays_df, on="user_id", how="inner")

# Filtered to get only last month and present month to compare play count
joined_df = joined_df.filter(col("play_date") >= add_months(current_date(), -2))

# Creating month column to use it for aggregating
joined_df = joined_df.withColumn(
    "year_month",
    date_format(col("play_date"), "yyyy-MM")
)

# Aggregated song count on each month for users
aggregated_df = joined_df.groupBy("user_id", "year_month").agg(
    count("song_id").alias("last_month_count")
)

# Used lag function to get previous month play count
window_spec = Window.partitionBy("user_id").orderBy("year_month")
user_analysis_df = aggregated_df.withColumn(
    "prev_month_count",
    lag("last_month_count").over(window_spec)
)

# Filtered null value records 
user_analysis_df = user_analysis_df.filter(col("prev_month_count").isNotNull())

# Filter users with at least a 50% increase in play count
filtered_df = user_analysis_df.filter(
    ((col("last_month_count") - col("prev_month_count")) / col("prev_month_count")) >= 0.5
)

# Select required users
selected_users_df = filtered_df.select("user_id")

selected_users_df.show()


# COMMAND ----------

"""
Hotel Booking & Revenue Analysis
Tables:

Hotels:
hotel_id (Primary Key)
hotel_name
city
Bookings:
booking_id (Primary Key)
hotel_id (Foreign Key)
customer_id
check_in_date
check_out_date
booking_amount
Tasks:
Find the hotel with the highest occupancy rate (rooms booked/total available rooms).
Identify customers who have stayed at the same hotel at least 3 times.
Calculate the average revenue per booking for each hotel and rank them.
"""

# COMMAND ----------

# Hotels DataFrame
hotels_schema = StructType([
    StructField("hotel_id", IntegerType(), False),
    StructField("hotel_name", StringType(), False),
    StructField("city", StringType(), False)
])

hotels_data = [
    (1, "Grand Palace", "New York"),
    (2, "Ocean View", "Los Angeles"),
    (3, "Mountain Retreat", "Denver"),
    (4, "Skyline Suites", "Chicago")
]

hotels_df = spark.createDataFrame(hotels_data, schema=hotels_schema)

# Bookings DataFrame
bookings_schema = StructType([
    StructField("booking_id", IntegerType(), False),
    StructField("hotel_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("check_in_date", StringType(), False),  # Will convert later
    StructField("check_out_date", StringType(), False),  # Will convert later
    StructField("booking_amount", DoubleType(), False)
])

bookings_data = [
    (101, 1, 1001, "2024-01-10", "2024-01-15", 500.0),
    (102, 1, 1002, "2024-02-05", "2024-02-10", 600.0),
    (103, 2, 1001, "2024-03-15", "2024-03-18", 300.0),
    (104, 3, 1003, "2024-04-01", "2024-04-05", 450.0),
    (105, 2, 1002, "2024-05-10", "2024-05-12", 250.0),
    (106, 1, 1001, "2024-06-20", "2024-06-25", 700.0),
    (107, 1, 1001, "2024-07-05", "2024-07-08", 400.0),
    (108, 3, 1004, "2024-08-12", "2024-08-15", 350.0),
    (109, 2, 1001, "2024-09-20", "2024-09-22", 320.0),
    (110, 4, 1005, "2024-10-05", "2024-10-10", 800.0)
]

bookings_df = spark.createDataFrame(bookings_data, schema=bookings_schema)


bookings_df = bookings_df.withColumn("check_in_date", to_date(col("check_in_date"), "yyyy-MM-dd")) \
                         .withColumn("check_out_date", to_date(col("check_out_date"), "yyyy-MM-dd"))

# Show the DataFrames
hotels_df.show()
bookings_df.show()


# COMMAND ----------

# Calculate the average revenue per booking for each hotel and rank them.

# Aggregating avg revenue for each hotel
aggregated_df = bookings_df.groupBy("hotel_id").agg(
    avg("booking_amount").alias("avg_revenue")
)

# Ranking the hotels based on these
window = Window.orderBy(desc("avg_revenue"))
ranking_df =aggregated_df.withColumn(
    "rank",
    dense_rank().over(window)
)
ranking_df.show()

# COMMAND ----------

# Identify customers who have stayed at the same hotel at least 3 times

# Counting bookings of each hotel for same customer 
customers_bookings_df = bookings_df.groupBy("customer_id", "hotel_id").agg(
    count("booking_id").alias("total_bookings")
)

# Filtering customers who have stayed at same hotel at 3 times
filtered_df = customers_bookings_df.filter(col("total_bookings") >= 3)
filtered_df.show()

# COMMAND ----------

# Find the hotel with the highest occupancy rate (rooms booked/total available rooms).

# Joined two dataframes hotels and bookings to get hotel details
joined_df = hotels_df.join(bookings_df, on="hotel_id", how="inner")

# Aggregated total bookings for each hotel
aggregated_df = joined_df.groupBy("hotel_id","hotel_name").agg(
    count("booking_id").alias("total_booked_rooms")
)

# Ranking hotels based on there bookings
window_spec = Window.orderBy(desc("total_booked_rooms"))
ranking_df = aggregated_df.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

# Filtering highest booked hotel
final_df = ranking_df.filter(col("rank") == 1)
final_df.show()

# COMMAND ----------

"""
Online Learning Platform Analysis
Tables:

Users:
user_id (Primary Key)
name
signup_date

Courses:
course_id (Primary Key)
course_name
category
Enrollments:
enrollment_id (Primary Key)
user_id (Foreign Key)
course_id (Foreign Key)
enrollment_date
Progress:
progress_id (Primary Key)
user_id (Foreign Key)
course_id (Foreign Key)
progress_percentage
last_activity_date
Tasks:
Find users who have enrolled in more than 3 courses but completed none.
Identify courses with the highest dropout rate (users who enrolled but never made progress).
Find users who have made the most progress in a single month compared to the previous month.
"""

# COMMAND ----------

# Users Table
users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("signup_date", StringType(), True)  # Initially as string
])

users_data = [
    (1, "Alice", "2023-01-15"),
    (2, "Bob", "2023-02-20"),
    (3, "Charlie", "2023-03-05"),
    (4, "David", "2023-04-10"),
    (5, "Eve", "2023-05-25")
]

users_df = spark.createDataFrame(users_data, schema=users_schema) 
users_df = users_df.withColumn("signup_date", users_df["signup_date"].cast(DateType()))


# Courses Table
courses_schema = StructType([
    StructField("course_id", IntegerType(), False),
    StructField("course_name", StringType(), True),
    StructField("category", StringType(), True)
])

courses_data = [
    (101, "Python for Beginners", "Programming"),
    (102, "Data Science with Python", "Data Science"),
    (103, "Machine Learning", "AI"),
    (104, "Cloud Computing", "IT"),
    (105, "Cybersecurity Basics", "Security")
]

courses_df = spark.createDataFrame(courses_data, schema=courses_schema)


# Enrollments Table
enrollments_schema = StructType([
    StructField("enrollment_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("course_id", IntegerType(), False),
    StructField("enrollment_date", StringType(), True)  # Initially as string
])

enrollments_data = [
    (1, 1, 101, "2023-06-01"),
    (2, 1, 102, "2023-06-05"),
    (3, 1, 103, "2023-06-10"),
    (4, 1, 104, "2023-06-15"),
    (5, 2, 101, "2023-07-01"),
    (6, 2, 105, "2023-07-05"),
    (7, 3, 101, "2023-08-01"),
    (8, 3, 102, "2023-08-10"),
    (9, 3, 103, "2023-08-20"),
    (10, 3, 104, "2023-08-30")
]

enrollments_df = spark.createDataFrame(enrollments_data, schema=enrollments_schema) 
enrollments_df = enrollments_df.withColumn("enrollment_date", enrollments_df["enrollment_date"].cast(DateType()))


# Progress Table
progress_schema = StructType([
    StructField("progress_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("course_id", IntegerType(), False),
    StructField("progress_percentage", DoubleType(), True),
    StructField("last_activity_date", StringType(), True)  # Initially as string
])

progress_data = [
    (1, 1, 101, 50.0, "2023-06-20"),
    (2, 1, 102, 20.0, "2023-06-25"),
    (3, 2, 101, 0.0, "2023-07-10"),
    (4, 3, 101, 100.0, "2023-08-15"),
    (5, 3, 102, 50.0, "2023-08-18"),
    (6, 3, 103, 30.0, "2023-08-25")
]

progress_df = spark.createDataFrame(progress_data, schema=progress_schema) 
progress_df = progress_df.withColumn("last_activity_date", progress_df["last_activity_date"].cast(DateType()))

# Show schemas
users_df.printSchema()
courses_df.printSchema()
enrollments_df.printSchema()
progress_df.printSchema()

# Show sample data
users_df.show()
courses_df.show()
enrollments_df.show()
progress_df.show()


# COMMAND ----------

# Find users who have made the most progress in a single month compared to the previous month

# Join enrollments and progress
joined_df = enrollments_df.join(progress_df, on=["user_id", "course_id"], how="left")

# Fill NULL progress_percentage values with 0
joined_df = joined_df.fillna(0, subset=["progress_percentage"])

# Extract month from last_activity_date
joined_df = joined_df.withColumn("activity_month", date_format(col("last_activity_date"), "yyyy-MM"))

# Aggregate total progress per user per month
monthly_progress_df = joined_df.groupBy("user_id", "activity_month").agg(
    sum("progress_percentage").alias("total_monthly_progress")
)

# Define window for month-over-month comparison
window_spec = Window.partitionBy("user_id").orderBy("activity_month")

# Get previous month's progress
monthly_progress_df = monthly_progress_df.withColumn(
    "prev_month_progress", lag("total_monthly_progress").over(window_spec)
)

# Compute month-over-month progress difference
monthly_progress_df = monthly_progress_df.withColumn(
    "progress_difference", col("total_monthly_progress") - col("prev_month_progress")
)

# Remove null values (first month will have null previous progress)
monthly_progress_df = monthly_progress_df.filter(col("progress_difference").isNotNull())

# Find max progress increase
max_progress_increase = monthly_progress_df.agg(max("progress_difference")).collect()[0][0]

# Get users with the highest progress increase
final_df = monthly_progress_df.filter(col("progress_difference") == max_progress_increase).select("user_id", "activity_month", "progress_difference")

# Show the final result
final_df.show()


# COMMAND ----------

# Identify courses with the highest dropout rate (users who enrolled but never made progress).

# Joining both enrollments and progress 
joined_df = enrollments_df.join(progress_df, on=["user_id", "course_id"], how="left")

# Fill NULL progress_percentage values with 0
joined_df = joined_df.fillna(0, subset=["progress_percentage"])

# Assign '1' if the user never made any progress, else '0' (indicating dropout)
course_progresed_df = joined_df.withColumn(
    "dropout_flag",
    when(col("progress_percentage") == 0, 1).otherwise(0)
) 

# Count the number of dropouts per course
aggregated_df = course_progresed_df.groupBy("course_id").agg(
    sum("dropout_flag").alias("dropout_count")
)

# Rank courses by dropout count
window_spec = Window.orderBy(desc("dropout_count"))

ranking_df = aggregated_df.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

# Filter only courses with the highest dropout rate
highest_dropout_course_df = ranking_df.filter(col("rank") == 1)
highest_dropout_course_df.show()

# COMMAND ----------

# Find users who have enrolled in more than 3 courses but completed none.

# Joining both enrollments and progress
joined_df = enrollments_df.join(progress_df, on=["user_id", "course_id"], how="left")

# Created course progress column to check the condition
joined_df = joined_df.withColumn(
    "course_progress",
    when((col("progress_percentage") < 100) | (col("progress_percentage").isNull()), 1).otherwise(0)
) 

# Counting enrollments courses and summing course progress for each user
aggregated_df = joined_df.groupBy("user_id").agg(
    count("course_id").alias("total_courses"),
    sum("course_progress").alias("not_completed_courses")
)

final_df = aggregated_df.filter((col("total_courses") > 3) & (col("total_courses") == col("not_completed_courses"))).select("user_id")

final_df.show()

# COMMAND ----------

"""
Ride-Sharing Platform Data Analysis
Tables:

Drivers:
driver_id (Primary Key)
name
signup_date
Rides:
ride_id (Primary Key)
driver_id (Foreign Key)
customer_id
ride_date
start_location
end_location
fare_amount
Tasks:
Find drivers who completed at least 50 rides in the past month but have a declining trend in rides per week.
Determine the average fare per mile for each driver and rank them in descending order.
Identify customers who have used at least 3 different drivers in the last 2 months."""

# COMMAND ----------

# Define schema for Drivers DataFrame
drivers_schema = StructType([
    StructField("driver_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("signup_date", StringType(), False)  # Will be converted to timestamp later
])

# Define schema for Rides DataFrame
rides_schema = StructType([
    StructField("ride_id", IntegerType(), False),
    StructField("driver_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("ride_date", StringType(), False),  # Will be converted to timestamp later
    StructField("start_location", StringType(), False),
    StructField("end_location", StringType(), False),
    StructField("fare_amount", DoubleType(), False)
])

# Sample data for Drivers DataFrame
drivers_data = [
    (1, "Alice", "2023-01-15 10:00:00"),
    (2, "Bob", "2022-12-20 09:30:00"),
    (3, "Charlie", "2023-02-05 12:45:00"),
    (4, "David", "2022-11-10 08:20:00"),
    (5, "Eve", "2023-03-01 14:10:00")
]

# Sample data for Rides DataFrame
rides_data = [
    (101, 1, 201, "2024-02-01 08:00:00", "Location A", "Location B", 15.0),
    (102, 1, 202, "2024-02-02 09:30:00", "Location B", "Location C", 20.0),
    (103, 2, 203, "2024-02-05 11:15:00", "Location C", "Location D", 25.0),
    (104, 2, 201, "2024-02-07 14:40:00", "Location D", "Location E", 30.0),
    (105, 3, 204, "2024-02-10 16:25:00", "Location E", "Location F", 18.5),
    (106, 3, 205, "2024-02-15 18:50:00", "Location F", "Location G", 22.0),
    (107, 4, 206, "2024-02-18 20:05:00", "Location G", "Location H", 27.0),
    (108, 4, 207, "2024-02-22 21:30:00", "Location H", "Location I", 35.0),
    (109, 5, 208, "2024-02-25 22:45:00", "Location I", "Location J", 40.0),
    (110, 5, 209, "2024-02-28 23:59:00", "Location J", "Location K", 50.0)
]

# Create DataFrames
drivers_df = spark.createDataFrame(drivers_data, schema=drivers_schema)
rides_df = spark.createDataFrame(rides_data, schema=rides_schema)

# Convert date columns to timestamp
drivers_df = drivers_df.withColumn("signup_date", to_timestamp(col("signup_date")))
rides_df = rides_df.withColumn("ride_date", to_timestamp(col("ride_date")))

# Show DataFrames
drivers_df.show(truncate=False)
rides_df.show(truncate=False)


# COMMAND ----------

# Identify customers who have used at least 3 different drivers in the last 2 months.

# Step 1: Filter rides in the last 2 months
filtered_df = rides_df.filter(col("ride_date") >= add_months(current_date(), -2))

# Step 2: Count distinct drivers per customer
aggregated_df = filtered_df.groupBy("customer_id").agg(
    countDistinct("driver_id").alias("total_drivers")  
)

# Step 3: Select customers who used at least 3 different drivers
final_df = aggregated_df.filter(col("total_drivers") >= 3).select("customer_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# Determine the average fare per mile for each driver and rank them in descending order.

# Aggregating avg fare for drivers
avg_fare_df = rides_df.groupBy("driver_id").agg(
    avg("fare_amount").alias("avg_fare")
)
# window spec for ranking
window_spec = Window.orderBy(desc("avg_fare"))
# Creating rank column
ranking_df = avg_fare_df.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

# Find drivers who completed at least 50 rides in the past month but have a declining trend in rides per week.

# Filtered last one month data
filtered_df = rides_df.filter(col("ride_date") >= add_months(current_date(), -1))

# Aggregating total rides for drivers
total_rides_df = filtered_df.groupBy("driver_id").agg(
    count("ride_id").alias("total_rides")
)

# Adding new column week
filtered_rides_df = filtered_df.withColumn("week_number", date_format(col("ride_date"), "w"))

# Aggregating total rides per week for each driver
weekly_rides_df = filtered_rides_df.groupBy("driver_id", "week_number").agg(
    count("ride_id").alias("total_rides_per_week")
)

# Checking declining trend of rides for each week by lag 
window_spec = Window.partitionBy("driver_id").orderBy("week_number")

rides_trends_df = weekly_rides_df.withColumn(
    "prev_week_rides",
    lag("total_rides_per_week").over(window_spec)
)

# Filtered null records
rides_trends_df = rides_trends_df.filter(col("prev_week_rides").isNotNull())

# Creating decline trend column
declining_trend = rides_trends_df.withColumn(
    "decline_trend",
    when(col("total_rides_per_week") < col("prev_week_rides"), 1).otherwise(0)
)

drivers_declined_trend_df = declining_trend.groupBy("driver_id").agg(
    count("*").alias("total_count"),
    sum("decline_trend").alias("trend_count")
)

joined_df = drivers_declined_trend_df.join(total_rides_df, on="driver_id", how="inner")
final_df = joined_df.filter(
    (col("total_rides") >= 50) & 
    (col("total_count") == col("trend_count"))
    )


# COMMAND ----------


# Define schema
patients_schema = StructType([
    StructField("patient_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("dob", StringType(), False),  # Dates as strings initially
    StructField("gender", StringType(), False)
])

# Create DataFrame
patients_data = [
    (1, "Alice Johnson", "1985-04-12", "F"),
    (2, "Bob Smith", "1990-09-23", "M"),
    (3, "Charlie Brown", "1978-06-15", "M"),
    (4, "Diana Prince", "1995-12-07", "F"),
    (5, "Ethan Hunt", "1982-11-02", "M")
]

patients_df = spark.createDataFrame(patients_data, schema=patients_schema)
patients_df = patients_df.withColumn("dob", to_timestamp("dob", "yyyy-MM-dd"))

# Define schema
appointments_schema = StructType([
    StructField("appointment_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), False),
    StructField("appointment_date", StringType(), False),  # Will convert later
    StructField("doctor_id", IntegerType(), False)
])

# Create DataFrame
appointments_data = [
    (101, 1, "2024-01-15", 201),
    (102, 1, "2024-04-10", 201),
    (103, 1, "2024-07-22", 202),
    (104, 1, "2024-10-05", 203),
    (105, 2, "2024-02-17", 202),
    (106, 2, "2024-06-30", 201),
    (107, 3, "2024-03-10", 203),
    (108, 3, "2024-09-05", 204),
    (109, 4, "2024-04-15", 205),
    (110, 4, "2024-07-18", 205)
]

appointments_df = spark.createDataFrame(appointments_data, schema=appointments_schema)
appointments_df = appointments_df.withColumn("appointment_date", to_timestamp("appointment_date", "yyyy-MM-dd"))


# Define schema
medical_records_schema = StructType([
    StructField("record_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), False),
    StructField("record_date", StringType(), False),  # Will convert later
    StructField("diagnosis", StringType(), False),
    StructField("treatment", StringType(), False)
])

# Create DataFrame
medical_records_data = [
    (1001, 1, "2024-01-20", "Flu", "Rest & Hydration"),
    (1002, 1, "2024-04-15", "Flu", "Antibiotics"),
    (1003, 1, "2024-07-25", "Cold", "Rest"),
    (1004, 1, "2024-10-10", "Flu", "More rest"),
    (1005, 2, "2024-02-22", "Back Pain", "Physiotherapy"),
    (1006, 2, "2024-07-05", "Back Pain", "Medication"),
    (1007, 3, "2024-03-15", "Diabetes", "Diet Control"),
    (1008, 3, "2024-09-10", "Diabetes", "Insulin Therapy"),
    (1009, 4, "2024-04-20", "Asthma", "Inhaler"),
    (1010, 4, "2024-07-22", "Asthma", "Steroids")
]

medical_records_df = spark.createDataFrame(medical_records_data, schema=medical_records_schema)
medical_records_df = medical_records_df.withColumn("record_date", to_timestamp("record_date", "yyyy-MM-dd"))

# Verify the changes
patients_df.show()
appointments_df.show()
medical_records_df.show()


# COMMAND ----------



# COMMAND ----------

# For each doctor, determine the average time between a patient’s appointment and their subsequent medical record entry.


# Patients is joining with appointments
patients_with_appiontments_df = patients_df.join(appointments_df, on="patient_id", how="inner")

# Step 2: Define a window to find the next medical record entry for each patient
window_spec = Window.partitionBy("patient_id").orderBy("appointment_date")

# Step 3: Use lead() to get the next medical record date
patients_with_records_df = patients_with_appointments_df.join(medical_records_df, on="patient_id", how="inner") \
    .withColumn("next_record_date", lead("record_date").over(window_spec))

# Step 4: Calculate time difference in minutes
patients_with_records_df = patients_with_records_df.withColumn(
    "time_diff_minutes",
    (unix_timestamp("next_record_date") - unix_timestamp("appointment_date")) / 60
)

# Step 5: Filter out NULL values (i.e., appointments with no next record)
patients_with_records_df = patients_with_records_df.filter(col("time_diff_minutes").isNotNull())

# Step 6: Aggregate to find average time difference per doctor
aggregated_df = patients_with_records_df.groupBy("doctor_id").agg(
    avg("time_diff_minutes").alias("avg_time_diff")
)

# Step 7: Show the result
aggregated_df.show()

# COMMAND ----------

# Define schemas
# Define schemas
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("join_date", StringType(), True),  # Keeping as String initially
    StructField("country", StringType(), True)
])

# Sample data
customers_data = [
    (1, "Alice", "2023-03-15 00:00:00", "USA"),
    (2, "Bob", "2023-06-10 00:00:00", "Canada"),
    (3, "Charlie", "2022-09-25 00:00:00", "UK"),
    (4, "David", "2024-01-05 00:00:00", "Germany"),
    (5, "Emma", "2023-12-20 00:00:00", "France")
]

# Create DataFrame
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Convert string to timestamp
customers_df = customers_df.withColumn("join_date", to_timestamp("join_date"))

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # Initially as String
    StructField("total_amount", FloatType(), True)
])

# Sample data
orders_data = [
    (101, 1, "2024-02-10 15:30:00", 200.0),
    (102, 2, "2024-01-22 12:15:00", 150.0),
    (103, 3, "2023-12-05 09:45:00", 300.0),
    (104, 4, "2024-02-15 18:00:00", 250.0),
    (105, 5, "2023-11-08 22:10:00", 180.0)
]

# Create DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Convert string to timestamp
orders_df = orders_df.withColumn("order_date", to_timestamp("order_date"))

order_items_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True)
])

# Sample data
order_items_data = [
    (1, 101, 201, 2, 50.0),
    (2, 101, 202, 1, 30.0),
    (3, 102, 201, 5, 50.0),
    (4, 103, 203, 3, 20.0),
    (5, 104, 202, 4, 30.0)
]

# Create DataFrame
order_items_df = spark.createDataFrame(order_items_data, schema=order_items_schema)

customers_df.show()
orders_df.show()
order_items_df.show()

# COMMAND ----------

# For each product, determine its average order quantity and rank the products by this average using a window function.

# Aggregating average quantity for each product
aggregated_df = order_items_df.groupBy("product_id").agg(
    avg("quantity").alias("avg_order_quantity")
)
# window specification for ranking products based on avg quantity
window_spec = Window.orderBy(desc("avg_order_quantity"))

# Ranking products
ranking_df = aggregated_df.withColumn("rank", dense_rank().over(window))

ranking_df.show()

# COMMAND ----------

# Identify the top 5 customers by total spending who joined within the last year

# Filtering the customers who joined within last year 
filtered_customers_df = customers_df.filter(col("join_date") >= add_months(current_date(), -12))

# Joiningg with orders to get spend amount
joined_df = filtered_customers_df.join(orders_df, on="customer_id", how="inner")

# Aggregating total spending for each customer
aggregated_df = joined_df.groupBy("customer_id").agg(
    sum("total_amount").alias("total_spend_amount")
)

# Window specification to give ranks for customers
window_spec = Window.orderBy(desc("total_spend_amount"))
ranking_df = aggregated_df.withColumn("rank", dense_rank().over(window_spec))

top5_customers_df = ranking_df.filter(col("rank") <= 5)

top5_customers_df.show()


# COMMAND ----------

# Write a query to calculate the monthly revenue for the current year grouped by country.

#Joining orders and customers dataframes
joined_df = customers_df.join(orders_df, on="customer_id", how="left")

# Creating month column
joined_df = joined_df.withColumn(
    "formatted_date",
    date_format("order_date", "yyyy-MM")
)

# Filtering orders for the current year (last 12 months from today)
filtered_df = joined_df.filter(col("order_date") >= add_months(current_date(), -12))

# Aggregating to calculate total revenue per country per month
aggregated_df = filtered_df.groupBy("country", "formatted_date").agg(
    sum("total_amount").alias("monthly_revenue")
)

aggregated_df.show()

# COMMAND ----------

"""
 Processing IoT Sensor Data (Streaming + Batch Processing)
Dataset: A streaming dataset with device_id, timestamp, temperature, humidity, error_code.

Tasks:

Use Structured Streaming to process incoming sensor data.
Compute average temperature per device every 5-minute window.
Detect faulty sensors (reporting abnormal temperature fluctuations).
Store the aggregated batch data into a Delta table for historical analysis.
Create a real-time dashboard in Databricks."""

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE IoTSensorData
# MAGIC (
# MAGIC   device_id STRING NOT NULL,
# MAGIC   timestamp TIMESTAMP,
# MAGIC   temperature DOUBLE,
# MAGIC   humidity DOUBLE,
# MAGIC   error_code INT,
# MAGIC   CONSTRAINT valid_device_id EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC ) 
# MAGIC AS
# MAGIC SELECT device_id,
# MAGIC       CAST(timestamp AS TIMESTAMP) AS timestamp, 
# MAGIC       temperature,
# MAGIC       humidity,
# MAGIC       error_code
# MAGIC FROM cloud_files("mnt/raw/iotdata", "csv", map("header", "true"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE temperaturePerDevice
# MAGIC AS
# MAGIC SELECT 
# MAGIC     device_id,
# MAGIC     DATE_TRUNC('MINUTE', timestamp) - INTERVAL (MINUTE(timestamp) % 5) MINUTE AS time_window,
# MAGIC     avg(temperature) as avg_temp
# MAGIC FROM STREAM(LIVE.IoTSensorData)
# MAGIC GROUP BY device_id, time_window      

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE faultSensorData
# MAGIC AS
# MAGIC SELECT DISTINCT device_id
# MAGIC FROM STREAM(LIVE.IoTSensorData)
# MAGIC WHERE error_code = 1

# COMMAND ----------

"""
clickstream Analysis for an E-commerce Website
Dataset: Web clicks with user_id, click_time, product_id, category, page_type, session_id.

Tasks:

Identify conversion rates (how many sessions ended in a purchase).
Find the average number of products viewed before a purchase.
Compute bounce rate (sessions where users viewed only 1 page and left).
Detect users who viewed a product multiple times but did not buy it.
Save the insights into an external Parquet table."""

# COMMAND ----------

# Define schema with click_time as StringType
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("click_time", StringType(), True),  # String type initially
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Sample data
data = [
    ("U1", "2025-02-25 10:15:30", "P1", "Electronics", "Product Page", "S1"),
    ("U1", "2025-02-25 10:17:45", "P2", "Electronics", "Product Page", "S1"),
    ("U1", "2025-02-25 10:30:00", "P3", "Electronics", "Checkout", "S1"),
    ("U2", "2025-02-25 11:00:00", "P4", "Clothing", "Product Page", "S2"),
    ("U2", "2025-02-25 11:05:00", "P4", "Clothing", "Product Page", "S2"),
    ("U2", "2025-02-25 11:10:00", "P4", "Clothing", "Checkout", "S2"),
    ("U3", "2025-02-25 12:00:00", "P5", "Books", "Product Page", "S3"),
    ("U3", "2025-02-25 12:05:00", "P5", "Books", "Product Page", "S3"),
    ("U3", "2025-02-25 12:10:00", "P5", "Books", "Product Page", "S3"),
    ("U4", "2025-02-25 13:00:00", "P6", "Gadgets", "Product Page", "S4")
]

# Create DataFrame
clickstream_df = spark.createDataFrame(data, schema=schema)

# Convert click_time to TimestampType
clickstream_df = clickstream_df.withColumn("click_time", col("click_time").cast(TimestampType()))

# Show the DataFrame
clickstream_df.show(truncate=False)


# COMMAND ----------

# Compute bounce rate (sessions where users viewed only 1 page and left).

filtered_df = clickstream_df.filter(col("page_type")=="Product Page")
filtered_df.show()


# COMMAND ----------

# Identify conversion rates (how many sessions ended in a purchase).
# Count total sessions per user
user_total_sessions_df = clickstream_df.groupBy("user_id").agg(
    count("session_id").alias("total_sessions")
)

# Filter sessions that ended in checkout
checkout_df = clickstream_df.filter(col("page_type") == "Checkout")

# Count checkout sessions per user
user_checkout_sessions_df = checkout_df.groupBy("user_id").agg(
    count("session_id").alias("total_checkout_sessions")
)

# Join both DataFrames and handle null values
joined_df = user_total_sessions_df.join(user_checkout_sessions_df, on="user_id", how="left").fillna(0)

# Compute user conversion rate
user_conversion_rate_df = joined_df.withColumn(
    "conversion_rate",
    ((col("total_checkout_sessions") / col("total_sessions")) * 100)
)

user_conversion_rate_df.show()

# --- Overall Conversion Rate ---

# Total sessions across all users
total_sessions = clickstream_df.select(count("session_id")).collect()[0][0]

# Total sessions that ended in checkout
checkout_sessions = clickstream_df.filter(col("page_type") == "Checkout").select(count("session_id")).collect()[0][0]

# Compute overall conversion rate
overall_conversion_rate = (checkout_sessions / total_sessions) * 100 if total_sessions > 0 else 0

print(f"Overall Conversion Rate: {overall_conversion_rate:.2f}%")

# COMMAND ----------

"""
Dataset: Apache web server logs with fields: user_id, timestamp, url, status_code, response_time_ms.

Tasks:

Convert the timestamp into a proper timestamp format.
Identify unique user sessions (a session ends if there is more than 30 minutes of inactivity).
Compute:
The average session duration per user.
The most common pages visited by returning users.
Detect users who experienced slow response times (90th percentile response time).
Save processed sessionized data to a Hive table."""

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder.appName("WebServerLogs").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),  # Will convert to TimestampType
    StructField("url", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("response_time_ms", IntegerType(), True)
])

# Sample data
data = [
    (101, "2024-02-19 10:00:00", "/home", 200, 120),
    (101, "2024-02-19 10:15:00", "/product", 200, 150),
    (101, "2024-02-19 10:50:00", "/checkout", 200, 300),  # New session (more than 30 min gap)
    (102, "2024-02-19 11:05:00", "/home", 200, 90),
    (103, "2024-02-19 11:10:00", "/blog", 404, 180),
    (101, "2024-02-19 11:55:00", "/home", 200, 100),  # Returning user
    (104, "2024-02-19 12:20:00", "/contact", 500, 500),  # Slow response time
    (102, "2024-02-19 12:25:00", "/pricing", 200, 110),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
web_server_logs_df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
web_server_logs_df.show()

# COMMAND ----------

# Identify unique user sessions (a session ends if there is more than 30 minutes of inactivity).
window_spec = Window.partitionBy("user_id").orderBy("timestamp")
web_server_logs_df = web_server_logs_df.select(
    col("user_id"),
    col("url"),
    col("timestamp"),
    lag("timestamp").over(window_spec).alias("prev_timestamp")
)
logs_with_time_diff_df = web_server_logs_df.select(
    col("user_id"),
    col("url"),
    col("timestamp"),
    col("prev_timestamp"),
    ((unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60).alias("time_diff_min")
)
logs_with_ranks = logs_with_time_diff_df.select(
    col("user_id"),
    col("url"),
    col("timestamp"),
    col("time_diff_min"),
    row_number().over(Window.partitionBy("user_id").orderBy("timestamp")).alias("rn")
)
logs_with_sessionID_df = logs_with_ranks.withColumn(
    "sessionID",
    when((col("time_diff_min") < 30) | (col("rn") == 1), concat(col("user_id"), lit("_001"))) 
    .otherwise(concat(col("user_id"), lit("_"), floor(rand() * 1000).cast("int")))
)
logs_with_sessionID_df.show()

# The average session duration per user.
session_df = df.groupBy("user_id", "sessionID").agg(
    (unix_timestamp(max("timestamp")) - unix_timestamp(min("timestamp"))).alias("session_duration_sec")
)

# Compute Average Session Duration Per User
avg_session_duration = session_df.groupBy("user_id").agg(
    avg("session_duration_sec").alias("avg_session_duration_sec")
)

avg_session_duration.show()

# The most common pages visited by returning users.
# Identify Returning Users (More than One Session)
returning_users = web_servers_logs_df.groupBy("user_id").agg(count("sessionID").alias("session_count")).filter(col("session_count") > 1)

# Join with Logs to Find Common Pages for Returning Users
common_pages_df = web_servers_logs_df.join(returning_users, "user_id", "inner").groupBy("user_id", "url").agg(
    count("*").alias("visit_count")
).orderBy(col("visit_count").desc())

common_pages_df.show()

# Detect users who experienced slow response times (90th percentile response time).
slow_users_df = df.groupBy("user_id").agg(
    approx_percentile("response_time_ms", 0.9).alias("slow_response_threshold")
)

slow_users_df.show()



# COMMAND ----------

"""
Multi-Step Aggregation with Window Functions
Problem: You have a sales dataset with sale_id, product_id, sale_date, and amount. Perform the following:
Aggregate the total sales per month for each product.
Calculate the monthly growth rate in sales for each product (i.e., compare the current month's sales with the previous month's sales).
Rank the products by their total sales for the most recent month.
Calculate the average sales per product for the last 6 months.
"""

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Sample data
data = [
    (1, 101, "2024-01-05", 100),
    (2, 102, "2024-01-12", 200),
    (3, 101, "2024-01-15", 150),
    (4, 103, "2024-01-20", 250),
    (5, 102, "2024-02-02", 300),
    (6, 101, "2024-02-10", 200),
    (7, 104, "2024-02-12", 350),
    (8, 103, "2024-02-15", 400),
    (9, 101, "2024-03-01", 250),
    (10, 102, "2024-03-10", 350),
    (11, 103, "2024-03-15", 450),
    (12, 104, "2024-03-20", 500)
]

# Create DataFrame with the schema
df = spark.createDataFrame(data, schema)

# Convert sale_date to DateType
sales_df = df.withColumn("sale_date", col("sale_date").cast("date"))

# Show the DataFrame
sales_df.show()

# COMMAND ----------


class salesAnalysis:
    def __init__(self):
        pass

    def read_data(self,path):
        return spark.read.format('csv').option('header',True).load(path)
    
    # Aggregate the total sales per month for each product.
    def totalSales(self, sales_df, groupByKey, sum_key, date_col):
        # Extracting month from sale_date
        sales_df = sales_df.withColumn(
            "formatted_date",
            date_format(col(date_col), "yyyy-MM")
        )
        # Aggregating total sales for each product
        total_sales_df = sales_df.groupBy(groupByKey, "formatted_date").agg(
            sum(col(sum_key)).alias("total_sales")
        )
        return total_sales_df
    
    # Calculate the monthly growth rate in sales for each product (i.e., compare the current month's sales with the previous month's sales).
    def monthlyGrowthRate(self, sales_df, groupByKey, sum_key, date_col, lag_col, sales_col):
        # Calling totalSales function to get aggregated_df
        total_sales_df = self.totalSales(sales_df, groupByKey, sum_key, date_col)

        # Window specification for findout previous month sales
        window_spec = Window.partitionBy(groupByKey).orderBy(orderBy_col)

        # Creating previous month sales column
        total_sales_df = total_sales_df.withColumn(
            "previous_month_sales",
            lag("total_sales").over(window_spec)
        )
        growth_rate_df = total_sales_df.withColumn(
            "growth_rate_for_month",
            (col("total_sales") - col("previous_month_sales")) / col("previous_month_sales")
        )
        growth_rate_df = growth_rate_df.orderBy(desc( "growth_rate_for_month"))

        return growth_rate_df
    
    # Rank the products by their total sales for the most recent month.
    def rankingProducts(self, sales_df, groupByKey, sum_key, date_col):
        # Extracting month from sale_date
        sales_df = sales_df.withColumn(
            "formatted_date",
            date_format(col(date_col), "yyyy-MM")
        )
        # Aggregating total sales for each product
        total_sales_df = sales_df.groupBy(groupByKey, "formatted_date").agg(
            sum(sum_key).alias("total_sales")
        )
        # Ranking products for each month
        window = Window.partitionBy("formatted_date").orderBy(desc("total_sales"))
        ranked_df = total_sales_df.withColumn(
            "rank",
            rank().over(window)
        )
        # Filtering recent month only
        ranks_df = ranks_df.withColumn(
            "month_rank",
            rank().over(Window.orderBy(desc("formatted_date")))
        )
        # Identify the most recent month by finding the maximum formatted_date
        max_date = total_sales_df.agg({"formatted_date": "max"}).collect()[0][0]
        final_df = ranked_df.filter(col("formatted_date") == max_date)
        return final_df
    
    # Calculate the average sales per product for the last 6 months
    def avgSalesPerProduct(self, sales_df, groupByKey, avg_key):
        # Filter records for the last 6 months.
        # Here, we compare the sale_date with the date 6 months before today.
        sales_df = sales_df.filter(
            (col("sale_date") >= add_months(current_date(), -6)) &
            (col("sale_date") <= current_date()) 
        )
        # Compute the average sales per product over the filtered period.
        aggregated_df = sales_df.groupBy(groupByKey).agg(
            avg(col(avg_key)).alias("avg_sales")
        )

        return aggregated_df
    
# Instantiate the SalesAnalysis class
sales_analysis = SalesAnalysis(spark)

# File path (update this as needed)
sales_path = "dbfs:/FileStore/rawlayer/sales"

# Read the sales data
sales_df = sales_analysis.read_data(sales_path)

# Call the methods
result_df  = sales_analysis.totalSales(sales_df, "product_id", "amount", "sale_date")
result_df2 = sales_analysis.monthlyGrowthRate(sales_df, "product_id", "amount", "sale_date")
result_df3 = sales_analysis.rankingProducts(sales_df, "product_id", "amount", "sale_date")
result_df4 = sales_analysis.avgSalesPerProduct(sales_df, "product_id", "amount")

# COMMAND ----------

"""
Assignment: Sales Data Analysis and Optimization
Scenario:
You work as a Data Engineer for a company that has a large e-commerce platform. The company stores its sales transaction data in a distributed system (e.g., HDFS, ADLS Gen2). You have to process and analyze this data to provide business insights.

Task:
You are given the following sales dataset (you can create a sample dataset or use a real one if available). The dataset has the following columns:

order_id: Unique identifier for each order.
customer_id: Unique identifier for each customer.
product_id: Unique identifier for each product.
quantity: Number of units of a product purchased.
unit_price: Price per unit of the product.
order_date: Date when the order was placed.
payment_method: Method of payment (e.g., 'credit_card', 'paypal').
city: The city where the order was placed.
country: The country where the order was placed.
You need to perform the following tasks:

1. Data Cleansing:
Remove any rows where the order_id, customer_id, or product_id is missing.
Handle rows with quantity or unit_price as negative values.
Correct any rows where the order_date is invalid (outside the range of the last 12 months).
Ensure there are no duplicate order_id values.
2. Exploratory Data Analysis (EDA):
Calculate the total revenue (quantity * unit_price) for each order and add a new column total_revenue.
Find the average unit_price per product and the total quantity sold per product.
Calculate the top 5 most purchased products by quantity, and the top 5 products by total revenue.
Group the data by payment_method and country to find the total revenue, total number of orders, and average order value.
3. Customer Segmentation:
Segment customers into three categories based on total spending (total_revenue):
Low: Customers whose total spending is less than $500.
Medium: Customers whose total spending is between $500 and $5000.
High: Customers whose total spending is more than $5000.
For each customer, calculate their total revenue, the number of orders, and the average order value.
Rank customers within each segment based on the total revenue.
4. Time Series Analysis:
Calculate the total revenue per month for the past 12 months.
Identify trends in revenue: Is there any month with a significant spike or drop in sales?
Calculate the year-over-year growth rate in revenue for the last month compared to the same month of the previous year.
5. Optimization Challenge:
Given that your e-commerce platform generates a large volume of transactions daily, optimize the data processing pipeline for performance:
Use partitioning and bucketing to optimize the storage of your data.
Apply caching or persisting strategies to speed up computations for commonly used transformations.
Optimize the join operations when combining data with customer or product-related information.
6. Reporting:
Generate a summary report (using DataFrame or SQL) showing:
Total sales per country and payment method.
Total revenue per city for the last 6 months.
Top 10 customers by revenue in the last quarter.
7. Bonus (Advanced):
Implement a recommendation engine that predicts the next likely product a customer will purchase based on their past purchase history. You can use collaborative filtering or other techniques if you’re familiar with them.
Additional Requirements:
Use PySpark to implement this assignment.
Use appropriate Spark transformations (e.g., groupBy, agg, join, withColumn, etc.).
Consider performance optimizations like using partitioning, bucketing, caching, and persisting.
If applicable, use PySpark’s built-in functions like window functions, rank, dense_rank, and so on.
""" 

# COMMAND ----------

class eCommerceAnalysis:
    def __init__(self):
        pass
    def read_data(self, format_type, path):
        return spark.read.format(format_type).option('header', True).load(path)
    
    def dataCleaning(self, sales_df, order_id_col, customer_id_col, product_id_col, quantity_col, unit_price_col, order_date_col,groupByKeys):
        # Remove any rows where the order_id, customer_id, or product_id is missing.
        sales_filtered_df = sales_df.filter(
            (col(order_id_col).isNotNull()) &
            (col(customer_id_col).isNotNull()) &
            (col(product_id_col).isNotNull())
            )

        # Handle rows with quantity or unit_price as negative values.
        sales_filtered_df = sales_filtered_df.filter(
            (col(quantity_col) > 0) & 
            (col(unit_price_col) > 0)
            )

        # Correct any rows where the order_date is invalid (outside the range of the last 12 months).
        sales_df = sales_filtered_df.filter(
            col(order_date_col) >= addmonths(current_date(), -12)
            )

        # Ensure there are no duplicate order_id values.
        sales_df = sales_df.dropduplicates["order_id"]

        return sales_df
    
    def exploratoryDataAnalysis(self, sales_df, order_id_col, customer_id_col, product_id_col, quantity_col, unit_price_col, order_date_col, partition_col, avg_key, sum_key):

        sales_df = self.dataCleaning(self, sales_df, order_id_col, customer_id_col, product_id_col, quantity_col, unit_price_col, order_date_col)

        # Calculate the total revenue (quantity * unit_price) for each order and add a new column total_revenue.
        sales_with_rev_df = sales_df.withColumn(
            "total_revenue",
            col(quantity_col) * col(unit_price_col)
        )

        # 2. Average unit price & total quantity per product
        sales_agg_df = sales_with_rev_df.groupBy(product_id_col).agg(
            avg(unit_price_col).alias("avg_unit_price"),
            sum(quantity_col).alias("total_quantity_sold_per_product"),
            sum("total_revenue").alias("total_revenue_per_product")
        )

        # Calculate the top 5 most purchased products by quantity, and the top 5 products by total revenue
        quantity_rank_window = Window.orderBy(desc("total_quantity_sold_per_product"))
        revenue_rank_window = Window.orderBy(desc("total_revenue_per_product"))

        top_products_df = sales_agg_df.withColumn(
            "rank_by_quantity", rank().over(quantity_rank_window)
        ).withColumn(
            "rank_by_revenue", rank().over(revenue_rank_window)
        )
        # Top 5 most purchased products by quantity
        top_5_by_quantity_df = top_products_df.filter(col("rank_by_quantity") <= 5)

        # Top 5 products by total revenue
        top_5_by_revenue_df = top_products_df.filter(col("rank_by_revenue") <= 5)

        # Group the data by payment_method and country to find the total revenue, total number of orders, and average order value.
         
        # ✅ 4. Group by Payment Method & Country
        grouped_df = sales_with_rev_df.groupBy(payment_col, country_col).agg(
            sum("total_revenue").alias("total_revenue"),
            count(order_id_col).alias("total_orders")
        ).withColumn(
            "average_order_value",
            col("total_revenue") / col("total_orders")
        )
        return top_5_by_quantity_df, top_5_by_revenue_df, grouped_df
    
    def customerSegmentation(self, sales_df, customer_id_col, quantity_col, unit_price_col, order_id_col):
        # For each customer calculating total revenue and orders
        aggregated_df = sales_df.groupBy(customer_id_col).agg(
            sum(col(quantity_col) * col(unit_price_col)).alias("total_revenue"),
            count(order_id_col).alias("total_orders")
        ).withColumn(
            "average_order_value",
            col("total_revenue") / col("total_orders")
        ) 
        # Segregating customers according to there spending
        segmented_df = aggregated_df.withColumn(
            "segment",
            when(col("total_revenue") < 500, "Low")
            .when((col("total_revenue") >= 500 & col("total_revenue") <= 5000), "Medium")
            .otherwise("High")  # Use otherwise() instead of another .when()
        )

        # Ranking customers based on total revenue for each segment 
        ranking_window = Window.partitionBy("segment").orderBy(desc("total_revenue"))
        ranking_df = segmented_df.withColumn(
            "rank",
            rank().over(ranking_window)
        )

        return ranking_df
    
    def timeSeriesAnalysis(self, sales_df, quantity_col, unit_price_col, order_date_col):
        # Calculate the total revenue per month for the past 12 months.
        #sales_df = sales_df.filter(col(order_date_col) >= add_months(current_date(), -12))
        # Keep all data but filter to show only the last 12 months in the final result
        formatted_sales_df = sales_df.withColumn(
            "formatted_date",
            date_format(col(order_date_col), "yyyy-MM")
        )

        # Aggregate total revenue per month
        aggregated_df = formatted_sales_df.groupBy("formatted_date").agg(
            sum(col(quantity_col) * col(unit_price_col)).alias("total_revenue")
        )

        # Define window to get previous month's revenue
        window = Window.orderBy("formatted_date")

        # Get previous month's revenue
        sales_with_prev_revenue = aggregated_df.withColumn(
            "previous_month_sales",
            lag("total_revenue").over(window)
        )

        # Calculate percentage change
        sales_with_prev_revenue = sales_with_prev_revenue.withColumn(
            "percentage_change",
            round(((col("total_revenue") - col("previous_month_sales")) / col("previous_month_sales")) * 100, 2)
        )

        # Detect spikes or drops
        sales_report_df = sales_with_prev_revenue.withColumn(
            "spike_or_drop",
            when(col("percentage_change") > 10, "Significant Spike")
            .when(col("percentage_change") < -10, "Significant Drop")
            .otherwise("Stable")
        )

        # Keep only the last 12 months in the final result
        final_report = sales_report_df.filter(col("formatted_date") >= date_format(add_months(current_date(), -12), "yyyy-MM"))


        return final_report

        # Calculate the year-over-year growth rate in revenue for the last month compared to the same month of the previous year.

        # Creating date by extracting some part from date
        sales_df = sales_df.withColumn("formatted_date", date_format(order_date_col, "yyyy-MM"))

        # Aggregating revenue for each month
        grouped_df = sales_df.groupBy("formatted_date").agg(
            sum(col(quantity_col) * col(unit_price_col)).alias("revenue")
        )

        # Define window that orders by formatted_date (ensuring correct chronological order)
        window = Window.orderBy("formatted_date")

        # Add previous year's revenue (lag by 12 months)
        sales_with_previous_year_df = grouped_df.withColumn(
            "previous_year_month_revenue",
            lag("revenue", 12).over(window)  # Lag by exactly 12 months
        )

        # Calculate percentage growth rate
        sales_with_previous_year_df = sales_with_previous_year_df.withColumn(
            "growth_rate",
            (col("revenue") - col("previous_year_month_revenue")) / col("previous_year_month_revenue") * 100
        )
        return sales_with_previous_year_df
    
    def optimization(self, sales_path, customers_path, products_path, cust_key, cust_type, prod_key, prod_type, partitioning_col, order_id_col, quantity_col, unit_price_col):
        # Reading files from path
        fact_sales_df = spark.read.format('csv').option('header', True).load(sales_path)

        dim_customers_df = spark.read.format('csv').option('header', True).load(customers_path)

        dim_products_df = spark.read.format('csv').option('header', True).load(products_path)

        # Repartitioning the sales dataframe to reduce shuffling in join operation
        fact_sales_df = fact_sales_df.repartition(50, cust_key) 

        # Broadcasting the both customers and products because they both are lesser than 10 mb
        fact_sales_with_customers = fact_sales_df.join(broadcast(dim_customers_df), on=cust_key, how=cust_type) 
        fact_sales_with_customers_products = fact_sales_with_customers.join(broadcast(dim_products_df), on=prod_key, how=prod_type)    

        # Persist the dataframe to reduce i/o network and recomputation of large complex code
        fact_sales_with_customers_products.persist(StorageLevel.MEMORY_AND_DISK)    
        
        # Partitioning dataset with country to do aggragations on country level 
        fact_sales_with_customers_products.write.mode('overwrite').partitionBy('country').bucketBy(5,'city').sortBy('customer_id').format('delta').save("dbfs:/FileStore/raw_layer/factsales_with_dim_cus_dim_prod")

        # Aggregating country wise 
        fact_sales_transactions_in_india = fact_sales_with_customers_products.filter((col("country") == "INDIA") & (col("city") == "AndhraPradesh")).groupBy("customer_id").agg(
            sum(col(quantity) * col(unit_price)).alias("total_revenue"),
            count(col(order_id_col)).alias("total_orders") 
        )

        fact_sales_transactions_in_usa = fact_sales_with_customers_products.filter("country" = "USA").groupBy("customer_id").agg(
            sum(col(quantity) * col(unit_price)).alias("total_revenue"),
            count(col(order_id_col)).alias("total_orders") 
        )

        return fact_sales_transaction_in_india, fact_sales_transactions_in_usa

        

        






# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount_spent", FloatType(), True),
    StructField("category", StringType(), True),
    StructField("location", StringType(), True)
])

# Sample data
data = [
    ("CUST_1", "2025-01-01 10:00:00", 250.0, "Electronics", "NY"),
    ("CUST_1", "2025-01-15 12:00:00", 150.0, "Clothing", "NY"),
    ("CUST_2", "2025-01-20 14:00:00", 100.0, "Groceries", "CA"),
    ("CUST_2", "2025-02-01 10:00:00", 200.0, "Electronics", "TX"),
    ("CUST_3", "2025-01-02 11:00:00", 300.0, "Books", "FL"),
    ("CUST_4", "2025-02-05 13:00:00", 450.0, "Furniture", "CA"),
    ("CUST_4", "2025-02-10 16:00:00", 125.0, "Clothing", "FL"),
    ("CUST_5", "2025-01-10 09:00:00", 200.0, "Groceries", "TX"),
    ("CUST_5", "2025-01-15 10:30:00", 350.0, "Books", "NY"),
    ("CUST_6", "2025-02-01 12:00:00", 450.0, "Electronics", "TX")
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
customers_df = df.withColumn("transaction_date", col("transaction_date").cast(TimestampType()))

# Identify repeat customers and new customers.
customers_transactions_df = customers_df.groupBy("customer_id").agg(
    count("*").alias("total_transactions")
)

customers_df = customers_df.join(customers_transactions_df, on="customer_id", how="inner")
customers_df = customers_df.withColumn(
    "customer_type",
    when(col("total_transactions") > 1, "Repeat").otherwise("New")
)

# Calculate monthly spending per customer.
customers_df = customers_df.withColumn(
    "formatted_date",
    date_format(col("transaction_date"), "yyyy-MM")
)
 customers_spending_df = customers_df.groupBy("customer_id", "formatted_date").agg(
     sum("amount_spent").alias("monthly_spending")
 )

 # Total transactions in the last 3 months.
 customers_df = customers_df.filter(col("transaction_date") >= add_months(current_date(), -3))

 # total transactions per customer
 total_transactions_counts_df = customers_df.groupBy("customer_id").agg(
     count("*").alias("total_transactions")
 ) 
 total_transactions_counts_df.show()

 # Average spend per transaction.
 aggregated_df = customers_df.groupBy("customer_id").agg(
     sum("amount_spent").alias("total_spent")
     count("*").alias("total_transactions")
 )
average_spend_df = aggregated_df.select(
    col("customer_id"),
    col("total_spent"),
    col("total_transactions"),
    (col("total_spent") / col("total_transactions")).alias("average_spend_per_transaction")
)

# Number of unique product categories purchased.
aggregated_df = customers_df.withColumn(
    "unique_product_categories",
    countDistinct("category").over(Window.partitionBy("customer_id"))
)
aggregated_df.show()

# Identify high-value customers (top 10% of spenders).
customers_total_spending_df = customers_df.groupBy("customer_id").agg(
    sum("amount_spent").alias("total_amount")
)
ranking_df = customers_total_spending_df.withColumn(
    "rank",
    rank().over(Window.orderBy(desc("total_spent")))
)
top10_spendors = ranking_df.filter(col("rank") <= 10)
top10_spendors.show()



# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("ride_id", IntegerType(), True),
    StructField("driver_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("distance_km", FloatType(), True),
    StructField("fare_amount", FloatType(), True)
])

# Sample data
data = [
    (1, "D1", "C1", "2025-01-15 08:00:00", "2025-01-15 08:30:00", 10.0, 20.0),
    (2, "D2", "C2", "2025-01-16 09:00:00", "2025-01-16 09:25:00", 5.0, 15.0),
    (3, "D1", "C3", "2025-01-16 10:00:00", "2025-01-16 10:45:00", 12.0, 25.0),
    (4, "D3", "C1", "2025-01-17 07:30:00", "2025-01-17 08:00:00", 8.0, 18.0),
    (5, "D2", "C4", "2025-01-17 12:00:00", "2025-01-17 12:25:00", 6.5, 16.0),
    (6, "D3", "C5", "2025-01-18 06:00:00", "2025-01-18 06:30:00", 7.0, 17.0),
    (7, "D1", "C2", "2025-01-19 09:00:00", "2025-01-19 09:30:00", 10.0, 20.0),
    (8, "D2", "C5", "2025-01-19 14:00:00", "2025-01-19 14:30:00", 9.0, 22.0),
    (9, "D1", "C1", "2025-01-20 11:00:00", "2025-01-20 11:45:00", 13.0, 30.0),
    (10, "D3", "C2", "2025-01-20 16:00:00", "2025-01-20 16:30:00", 7.5, 19.0)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Convert string to timestamp
rides_df = df.withColumn("start_time", col("start_time").cast(TimestampType()))\
       .withColumn("end_time", col("end_time").cast(TimestampType()))

# Show sample data
rides_df.show(truncate=False)

# COMMAND ----------

# Find the top 5 busiest drivers in each city based on the number of rides in the last 30 days.
rides_df = rides_df.filter(col("start_time") >=  date_add(current_date(), -30))
busiest_drivers_df = rides_df.groupBy("driver_id").agg(
    count("*").alias("total_rides")
)
window_spec = Window.orderBy(desc("total_rides"))
ranking_drivers_df = busiest_drivers.withColumns(
    rank().over(window_spec)
)
top5_busiest_drivers_df = ranking_drivers_df.filter(col("rank") <= 5)

# Compute moving average fare per driver over the last 7 days using window functions
window = Window.partitionBy("driver_id").orderBy("start_time").rangeBetween(-7 * 86400, 0)
moving_average_df = rides_df.withColumn(
    "moving_average",
    avg("fare_amount").over(window)
)
moving_average_df.show()

# Identify customers who took at least 5 rides in the last month but stopped booking in the last 2 weeks.
rides_df = rides_df.filter(col("start_time") >= add_months(current_date(), -1))
last_month_rides_info_df = rides_df.groupBy("customer_id").agg(
    count(col("ride_id")).alias("total_rides_per_month")
)
rides_df = rides_df.filter(col("start_time") >= date_add(current_date(), -14))
two_weeks_rides_info_df = rides_df.groupBy("customer_id").agg(
    coalesce(count(col("ride_id")), lit(0)).alias("total_rides_per_two_weeks")
) 

joined_df = last_month_rides_info_df.join(two_weeks_rides_info_df, on="customer_id", how="inner")
customers_analysis_df = joined_df.filter((col("total_rides_per_month") >= 5) & (col("total_rides_per_two_weeks") == 0))
customers_analysis_df.show()

# Compute average trip duration and distance per driver.
trip_duration_df = rides_df.withColumn(
    "trip_duration",
    expr("timestampdiff(MINUTE, start_time, end_time)")
)
driver_analysis_df = trip_duration_df.groupBy("driver_id").agg(
    avg("trip_duration").alias("avg_trip_duration")
    avg("distance_km").alias("avg_distance")
)
driver_analysis_df.show() 
