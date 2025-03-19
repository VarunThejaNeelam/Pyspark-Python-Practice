# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# Define schema for rides DataFrame
rides_schema = StructType([
    StructField("ride_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), True)  # Keeping timestamp as string
])

# Define schema for drivers DataFrame
drivers_schema = StructType([
    StructField("driver_id", IntegerType(), True),
    StructField("driver_name", StringType(), True),
    StructField("rating", DoubleType(), True)
])

# Define schema for customers DataFrame
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("signup_date", StringType(), True)  # Keeping signup_date as string
])

# Sample data with timestamps as strings
rides_data = [
    (1, 101, 201, "completed", 25.0, "2025-02-01 14:30:00"),
    (2, 102, 202, "canceled", 0.0, "2025-02-02 15:00:00"),
    (3, 101, 203, "completed", 30.0, "2025-02-03 16:00:00"),
    (4, 103, 204, "completed", 20.0, "2025-02-04 12:00:00"),
    (5, 102, 202, "canceled", 0.0, "2025-02-05 18:00:00"),
    (6, 104, 205, "completed", 15.0, "2025-02-06 09:30:00"),
    (7, 101, 206, "completed", 35.0, "2025-02-07 11:00:00"),
    (8, 103, 207, "canceled", 0.0, "2025-02-08 13:00:00"),
    (9, 104, 208, "completed", 28.0, "2025-02-09 17:00:00"),
    (10, 101, 209, "completed", 40.0, "2025-02-10 20:00:00")
]

drivers_data = [
    (101, "Alice", 4.8),
    (102, "Bob", 4.5),
    (103, "Charlie", 4.7),
    (104, "David", 4.6)
]

customers_data = [
    (201, "Eve", "2024-05-10"),
    (202, "Frank", "2024-06-15"),
    (203, "Grace", "2024-07-20"),
    (204, "Hannah", "2024-08-25"),
    (205, "Ian", "2024-09-30"),
    (206, "Jack", "2024-10-05"),
    (207, "Karen", "2024-11-10"),
    (208, "Leo", "2024-12-15"),
    (209, "Mia", "2025-01-20")
]

# Create DataFrames
rides_df = spark.createDataFrame(rides_data, schema=rides_schema)
drivers_df = spark.createDataFrame(drivers_data, schema=drivers_schema)
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Show DataFrames
rides_df = rides_df.withColumn("timestamp",col("timestamp").cast(TimestampType()))
rides_df.show()
drivers_df.show()
customers_df = customers_df.withColumn("signup_date", to_date(col("signup_date")))
customers_df.show()


# COMMAND ----------


class RideSharingPlatform:
    def __init__(self):
        pass

    def read_data(self,path):
        return spark.read.format('csv').option('header',True).load(path)
     
    # Identify top drivers based on completed rides.
    def topDrivers(self, rides_df, groupBy_key, status_col):
        # Aggregating the completed rides for each driver
        aggregated_df = rides_df.groupBy(groupBy_key).agg(
            sum(when(col("status") == "completed", 1)).alias("total_completed_rides")
        ) 
        top_drivers = aggregated_df.orderBy(desc("total_completed_rides"))
        
        return top_drivers
    
    # Find customers who have canceled more than 3 rides in a month
    def customersCancelledRides(self, rides_df, customers_df, join_type, join_key, groupByKey, sum_key, timestamp_col):
        # joining both datasets
        joined_df = rides_df.join(customers_df, on=join_key, how=join_type)
        # Extracting year and month from timestamp
        joined_df = joined_df.withColumn("formatted_date", date_format(timestamp_col, "yyyy-MM"))
        # Identifying customers who cancelled rides more than 3 times in a month
        aggregated_df = joined_df.groupBy(groupByKey, "formatted_date").agg(
            sum(when(col(sum_key) == "cancelled", 1)).alias("total_cancelled_rides")
        )
        # Filtering the customers who cancelled rides 3 times
        filtered_df = aggregated_df.filter(col("total_cancelled_rides") > 3)
        filtered_df = filtered_df.select(
            col("customer_id"),
            col("customer_name"),
            col("total_cancelled_rides")
        )

        return filtered_df
    
    # Detect rides that have multiple customers and compute total fare split.
    def ridesWithMultipleCustomers(self, rides_df, groupByKey, count_key, join_key, join_type, fare_col):
        # Counting the customers for each ride
        aggregated_df = rides_df.groupBy(groupByKey).agg(
            count(count_key).alias("total_customers")
        )
        aggregated_df = aggregated_df.filter(col("total_customers") > 1)

        joined_df = rides_df.join(aggregated_df, on=join_key, how=join_type)
        # Spliting the fare amount to all customers
        splitting_df = joined_df.withColumn(
            "fare_amount",
            col(fare_col)/col("total_customers")
        )

        return splitting_df
    
ridesharingplatform = RideSharingPlatform()

# File paths
rides = "dbfs:/FileStore/raw_layer/rides"
customers = "dbfs:/FileStore/raw_layer/customers"

# Reading data
rides_df = read_data(rides)
customers_df = read_date(customers)

# Define Parameters
join_type = "inner"
join_key = "customer_id"
groupByKey = "customer_id"

result_df = ridesharingplatform.topDrivers(rides_df, "driver_id", "status")

result_df2 = ridesharingplatform.customersCancelledRides(rides_df, customers_df, join_type, join_key, groupByKey, "status")

result_df3 = ridesharingplatform.ridesWithMultipleCustomers(rides_df, "ride_id", "customer_id", "ride_id", join_type, "fare")



# COMMAND ----------

"""
Event Log Analysis
📌 Scenario: You have server log data with the following fields: event_id, user_id, event_type, event_timestamp, location, and device_type.

🔹 Task:

Identify sessions for each user (a session is a group of events within 30 minutes of each other). Assign a unique session ID to each session.
Find the most common event type for each user during a session.
Detect users who logged in from two or more locations in the same session.
Calculate the average session duration for all users.
Store the enriched data in Delta format for downstream analysis"""

# COMMAND ----------

# Define the schema for the DataFrame
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device_type", StringType(), True)
])

# Sample data
data = [
    ("1", "U001", "login", "2025-01-28 10:00:00", "New York", "mobile"),
    ("2", "U001", "click", "2025-01-28 10:05:00", "New York", "mobile"),
    ("3", "U001", "logout", "2025-01-28 10:20:00", "New York", "mobile"),
    ("4", "U002", "login", "2025-01-28 11:00:00", "San Francisco", "desktop"),
    ("5", "U002", "click", "2025-01-28 11:10:00", "San Francisco", "desktop"),
    ("6", "U002", "logout", "2025-01-28 11:25:00", "Los Angeles", "desktop"),
    ("7", "U003", "login", "2025-01-28 12:00:00", "Chicago", "tablet"),
    ("8", "U003", "click", "2025-01-28 12:30:00", "Chicago", "tablet"),
    ("9", "U003", "logout", "2025-01-28 12:45:00", "Chicago", "tablet")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df=df.withColumn(
    "event_timestamp",
    col("event_timestamp").cast(TimestampType())
)
df.explain(True)

#df.write.format('csv').option("header",True).mode("overwrite").save("dbfs:/FileStore/raw_layer/event_logs")

# COMMAND ----------

df=spark.read.format('csv').option("header",True).load("dbfs:/FileStore/raw_layer/event_logs")

# Group by user_id, session_id, and event_type to count events per type in each session
aggregated_df = df.group("user_id", "session_id", "event_type").agg(
    count("*").alias("total_counts_for_each_type")
)

# Define a window specification for ranking event types per user and session
windowspec=Window.partitionBy("user_id", "session_id").orderBy(desc("total_counts_for_each_type"))

# Add rank column to determine the most common event type
ranking_df = aggregated_df.withColumn(
    "rank",
    rank().over(windowspec)
)

# Filter the rows where rank is 1 to get the most common event type for each user in each session
common_event = ranking_df.filter(col(rank) == 1)

# Show the results
common_event.show()

# COMMAND ----------

#Identify sessions for each user (a session is a group of events within 30 minutes of each other). Assign a unique session ID to each session.
import random

class EventLogAnalysis:
    #output_dfs={}
    def __init__(self,path,partition_col,order_col,concat_col):
        self.path=path
        self.partition_col=partition_col
        self.order_col=order_col
        self.concat_col=concat_col

    def sessionsForEachUser(self):
        # Read the data
        df = spark.read.format('csv').option("header",True).load(self.path)

        # Cast event_timestamp to timestamp
        df = df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))


        # Define a window specification
        window_spec=Window.partitionBy(self.partition_col).orderBy(self.order_col)

        # Add previous timestamp column
        df=df.withColumn(
            "previous_timestamp",
            lag("event_timestamp").over(window_spec)
        )

        # Calculate time difference in minutes
        df=df.withColumn(
            "time_diff",
            (unix_timestamp(col("event_timestamp")) - unix_timestamp(col("previous_timestamp"))) /60
        )

        # Add session ID based on time_diff
        df = df.withColumn(
            "session_id",
            when(col("time_diff").isNull() | (col("time_diff") > 30),
                 concat(lit("session_"), lit(random.randint(1, 100))))
            .otherwise(concat(lit("session_"), self.concat_col))
        )
        
        # Store the output DataFrame
        #EventLogAnalysis.output_dfs["SessionsForUser"] = df

        return df
        
path="dbfs:/FileStore/raw_layer/event_logs"  
partition_col="user_id"
order_col="event_timestamp"
concat_col="user_id"      
events=EventLogAnalysis(path,partition_col,order_col,concat_col)
output_df=events.sessionsForEachUser()

#show the result
output_df.display()


# COMMAND ----------

#Calculate the average session duration for all users.

# Calculate the average session duration for all users
avg_df = output_df.groupBy("session_id").agg(
    avg("time_diff").alias("average_session_duration")
)

# Replace NULL values in average_session_duration with 0
avg_df = avg_df.withColumn(
    "average_session_duration",
    coalesce(col("average_session_duration"), lit(0))
)

# Show the result
avg_df.show()

# COMMAND ----------

#Detect users who logged in from two or more locations in the same session
diff_loc_df = output_df.groupBy("user_id", "session_id").agg(
    countDistinct("location").alias("logged_from_diff_loc")
)

users_logged_from_diff_loc = diff_loc_df.filter(col("logged_from_diff_loc") >= 2)
users_logged_from_diff_loc.select("user_id").show()

# COMMAND ----------

"""
Social Network Friend Recommendation
📌 Scenario: You have a dataset with friend relationships:

friendships: user_id, friend_id
Each row represents a friendship between user_id and friend_id.
🔹 Task:

Identify mutual friends for each pair of users.
Suggest friend recommendations for each user based on mutual friends (exclude already existing friendships).
Count the total number of mutual friends for each recommended friend.
Save the friend recommendations in Parquet format"""

# COMMAND ----------

# Sample Data
data = [
    (1, 2),
    (1, 3),
    (2, 3),
    (2, 4),
    (3, 4),
    (3, 5),
    (4, 5),
    (4, 6),
    (5, 6),
    (6, 7),
    (7, 8),
    (7, 9),
    (8, 9),
    (9, 10)
]

# Define the schema
columns = ["user_id", "friend_id"]

# Create DataFrame
friendships_df = spark.createDataFrame(data, columns)

# Show the DataFrame
friendships_df.show()

# COMMAND ----------

#Identify mutual friends for each pair of users

# Self-join on the DataFrame to find potential relationships
joined_df = friendships_df.alias("friendshipone").join(
    friendships_df.alias("friendshiptwo"),
    col("friendshipone.friend_id") == col("friendshiptwo.friend_id"),
    "inner"
)

# Show the result
df = joined_df.select(
    col("friendshipone.user_id").alias("friendshipone_user_id"),
    col("friendshipone.friend_id").alias("friendshipone_friend_id"),
    col("friendshiptwo.user_id").alias("friendshiptwo_user_id"),
    col("friendshiptwo.friend_id").alias("friendshiptwo_friend_id")
)

# Filter for mutual friends and ensure no self-loop
mutual_friend_df = df.filter(
    (col("friendshipone_user_id") != col("friendshiptwo_user_id")) & (col("friendshipone_friend_id") == col("friendshiptwo_friend_id"))
)

# Final selection with alias for mutual friends
mutual_friend_df = mutual_friend_df.select(
    col("friendshipone_user_id"),
    col("friendshiptwo_user_id"),
    col("friendshipone_friend_id").alias("mutual_friend")
)

# Show the result
mutual_friend_df.show()

# COMMAND ----------

#Suggest friend recommendations for each user based on mutual friends (exclude already existing friendships).

# Self-join to find second-degree connections (friends of friends)
friends_of_friends_df = friendships_df.alias("f1").join(
    friendships_df.alias("f2"),
    col("f1.friend_id") == col("f2.user_id"),
    "inner"
).select(
    col("f1.user_id").alias("user"),
    col("f2.friend_id").alias("recommended_friend")
)

# Exclude direct friends and self-loops
recommendations_df = friends_of_friends_df.join(
    friendships_df,
    (col("user") == col("user_id")) & (col("recommended_friend") == col("friend_id")),
    "left_anti"
)

# Count mutual friends for each recommendation
mutual_friends_df = recommendations_df.groupBy("user", "recommended_friend").agg(
    count("*").alias("mutual_friends_count")
)

# Show final recommendations
mutual_friends_df.show(truncate=False)

# COMMAND ----------

# Define schema for sales data
sales_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("sales_amount", IntegerType(), True),
    StructField("sales_date", StringType(), True)
])

# Sample sales data
sales_data = [
    (101, "P1", 500, "2025-01-01"),
    (101, "P2", 700, "2025-01-01"),
    (101, "P1", 400, "2025-01-02"),
    (102, "P3", 600, "2025-01-02"),
    (102, "P4", 900, "2025-01-03"),
    (103, "P2", 1000, "2025-01-04"),
    (103, "P3", 1200, "2025-01-05")
]

# Create sales DataFrame
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Define schema for expenses data
expenses_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("expense_type", StringType(), True),
    StructField("expense_amount", IntegerType(), True),
    StructField("expense_date", StringType(), True)
])

# Sample expenses data
expenses_data = [
    (101, "Rent", 300, "2025-01-01"),
    (101, "Utilities", 200, "2025-01-01"),
    (101, "Rent", 400, "2025-01-02"),
    (102, "Salaries", 500, "2025-01-02"),
    (102, "Maintenance", 600, "2025-01-03"),
    (103, "Rent", 800, "2025-01-04"),
    (103, "Salaries", 700, "2025-01-05")
]

# Create expenses DataFrame
expenses_df = spark.createDataFrame(expenses_data, schema=expenses_schema)

# Show DataFrames
sales_df = sales_df.withColumn(
    "sales_date",
    to_date(col("sales_date"))
)
expenses_df = expenses_df.withColumn(
    "expense_date",
    to_date(col("expense_date"))
)

sales_df.coalesce(1).write.format('csv').mode("overwrite").option("header",True).save("dbfs:/FileStore/raw_layer/sales_data")
expenses_df.coalesce(1).write.format('csv').mode("overwrite").option("header",True).save("dbfs:/FileStore/raw_layer/expenses_data")

# COMMAND ----------

sales_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/raw_layer/sales_data")
expenses_df = spark.read.format('csv').option('header',True).load("dbfs:/FileStore/raw_layer/expenses_data")
#sales_df.show()
#expenses_df.show()

# Identify stores with negative profits for more than 3 consecutive days.

# identifying total sales per store
sales_df = sales_df.groupBy("store_id","sales_date").agg(
    sum("sales_amount").alias("total_sales")
)

# identifying total expenses per store
expenses_df = expenses_df.groupBy("store_id","expense_date").agg(
    sum("expense_amount").alias("total_expenses")
)

# joining two dataframes
joined_df = sales_df.join(expenses_df, (sales_df.store_id == expenses_df.store_id) & (sales_df.sales_date == expenses_df.expense_date), how="inner")

# finding diff b/w sales and expenses
joined_df = joined_df.withColumn("profit", col("total_sales") - col("total_expenses"))

# filtering only negative profit stores
df = joined_df.filter(col("profit") <0)

# Define window specification for ranking
window_spec = Window.partitionBy("store_id").orderBy("sales_date")

df = df.withColumn("rn", row_number().over(window_spec))
df = df.withColumn("grp", col("sales_date").cast("long") - col("rn"))

# Aggregate to count consecutive days
aggregated_df = df.groupBy("store_id","grp").agg(
    count("*").alias("consequtive_days")
)

# Filter stores with negative profits for more than 3 consecutive days
stores_with_neg_profits_morethan_3 = aggregated_df.filter(col("consequtive_days") >= 3)

# Show result
stores_with_neg_profits_morethan_3.show()


# COMMAND ----------

class RetailStoreProfitAnalysis:
    def __init__(self):
        pass

    def profitForEachStore(self,path1,path2,join_type,join_key,sales_col,expense_col,sales_groupBy_keys,expenses_groupBy_keys):
        # Read CSV files into DataFrames
        sales_df = spark.read.format('csv').option('header',True).load(path1)
        expenses_df = spark.read.format('csv').option('header',True).load(path2)

         # Aggregate total sales by store_id and date
        sales_df = sales_df.groupBy(sales_groupBy_keys).agg(
            sum(sales_col).alias("total_sales")
        )

         # Aggregate total expenses by store_id and date
        expenses_df = expenses_df.groupBy(expenses_groupBy_keys).agg(
            sum(expense_col).alias("total_expenses")
        )

         # Perform join on the given key
        joined_df = sales_df.join(expenses_df,join_key,join_type)

        # Calculate profit (total_sales - total_expenses)
        profits_df = joined_df.withColumn("profit",col("total_sales") - col("total_expenses"))

        # Select required columns
        profits_df = profits_df.select(
            col(sales_groupBy_keys[0]),  #store_id
            col(sales_groupBy_keys[1]),  #sales_date
            col("total_sales"),
            col("total_expenses"),
            col("profit")
        )

        return profits_df    



# COMMAND ----------

# Sample data for readings DataFrame
# Schema for readings
readings_schema = StructType([
    StructField("reading_id", StringType(), True),
    StructField("household_id", StringType(), True),
    StructField("energy_consumption", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Schema for households
households_schema = StructType([
    StructField("household_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("household_size", IntegerType(), True),
    StructField("income_bracket", StringType(), True)
])

# -----------------------
# 2. Create sample data
# -----------------------

readings_data = [
    ("r1", "H1", 10, "2023-01-15 10:00:00"),
    ("r2", "H1", 15, "2023-01-20 09:30:00"),
    ("r3", "H2", 20, "2023-01-15 08:00:00"),
    ("r4", "H2", 25, "2023-02-15 10:00:00"),
    ("r5", "H3", 40, "2023-01-19 07:45:00"),
    ("r6", "H3",  5, "2023-02-14 14:30:00")
]

households_data = [
    ("H1", "CityA", 3, "Low"),
    ("H2", "CityB", 5, "Medium"),
    ("H3", "CityA", 2, "High")
]

# -------------------------------
# 3. Create the DataFrames
# -------------------------------

readings_df = spark.createDataFrame(
    data=readings_data,
    schema=readings_schema
)

households_df = spark.createDataFrame(
    data=households_data,
    schema=households_schema
)

readings_df = readings_df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType())
)

readings_df.show()


households_df.show()

# COMMAND ----------

#Calculate the total energy consumption per household per month.

class EnergyConsumptionAnalysis:
    def __init__(self):
        pass

    def energyConsumption(self,path1,groupBy_key,timestamp_col,sum_agg_key):

        # reading data from file
        readings_df = spark.read.format('csv').option("header",True).load(path1)

        # creating month column for dataframe
        readings_df = readings_df.withColumn("month", month(timestamp_col))

        # aggregating energy consumption for households
        aggregated_df = readings_df.groupBy(groupBy_key, "month").agg(
            sum(sum_agg_key).alias("total_energy_consumption")
        )

        housefold_energy_con_df = aggregated_df.orderBy(desc("total_energy_consumption"))

        return housefold_energy_con_df
    
    def abnormalConsumption(path1, path2, join_key, join_type, household_col, income_bracket_col, avg_key, sum_key):

        # Reading readings file from the path
        readings_df = spark.read.format('csv').option("header",True).load(path1)
        # Reading households file from the path
        households_df = spark.read.format('csv').option("header",True).load(path2)
        # Joining two files to get details
        joined_df = readings_df.join(households_df, on=join_key, how = join_type)
        
        #window spec for avg consumption
        window_spec_sum = Window.partitionBy(household_col)
        window_spec_income = Window.partitionBy(income_bracket_col)

        df = joined_df.withColumn(
            "total_energy_con_per_household",
            sum(sum_key).over(window_spec_sum)
        )

        df = df.withColumn(
            "avg_consumption_per_income_bracket",
            avg(avg_key).over(window_spec_income)
        )
       
       # creating consumption labelling column based on condition
        df = df.withColumn(
            "consumption_labelling",
            when(col("total_energy_con_per_household") > 2*col("avg_consumption_per_income_bracket"),"abnormalConsumption").otherwise("normalConsumption")
        )
       
       # filtering abnormal consumption households
        filtered_df = df.filter(col("consumption_labelling") == "abnormalConsumption")

        
        # Select relevant details, keeping all household information
        household_with_abnormal_df = filtered_df.select(
            household_col,
            "location",
            "household_size",
            income_bracket_col,
            "total_energy_con_per_household",
            "avg_consumption_per_income_bracket",
            "consumption_labelling"
        ).dropDuplicates()
        
        return household_with_abnormal_df


# Defining parameters
path1 = "dbfs:FileStore/rawlayer/readings"
path2 = "dbfs:FileStore/rawlayer/households"
join_key = "household_id"
join_type = "inner"
household_col = "household_id"
income_bracket_col = "income_bracket"
avg_key = "energy_consumption"
sum_key = "energy_consumption"

energy = EnergyConsumption()

df = energycon.abnormalConsumption(path1,path2,join_key,join_type,household_col,income_bracket_col,avg_key,sum_key)


path1 = "dbfs:FileStore/rawlayer/energyconsumption"   
groupBy_key = "household_id"
timestamp_col = "timestamp"
sum_agg_key = "energy_consumption"

energycon = EnergyConsumptionAnalysis()

# Call the function and display results
result_df = energycon.energyConsumption(path1,groupBy_key,timestamp_col,sum_agg_key) 


# COMMAND ----------

"""
Employee Productivity Tracking
📌 Scenario: Two datasets:

employees: employee_id, name, department, hire_date
tasks: task_id, employee_id, task_description, hours_spent, completion_date
🔹 Task:

Identify employees with low productivity (average hours spent < threshold).
Calculate the total hours spent on tasks for each department.
Detect employees who completed multiple tasks on the same day.
Save the enriched dataset for further analysis"""

# COMMAND ----------

# Define schema for employees DataFrame
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("hire_date", StringType(), True)
])

# Create employees DataFrame
employees_data = [
    (1, "Alice", "HR", "2020-06-15"),
    (2, "Bob", "IT", "2019-09-10"),
    (3, "Carol", "Finance", "2021-01-25"),
    (4, "Dave", "IT", "2022-05-20"),
    (5, "Eve", "HR", "2023-03-12")
]

employees_df = spark.createDataFrame(employees_data, schema=employees_schema)

# Define schema for tasks DataFrame
tasks_schema = StructType([
    StructField("task_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("task_description", StringType(), True),
    StructField("hours_spent", IntegerType(), True),
    StructField("completion_date", StringType(), True)
])

# Create tasks DataFrame
tasks_data = [
    (101, 1, "Report Review", 3, "2024-01-01"),
    (102, 2, "Software Upgrade", 5, "2024-01-01"),
    (103, 1, "Policy Update", 2, "2024-01-02"),
    (104, 3, "Budget Analysis", 4, "2024-01-02"),
    (105, 4, "System Maintenance", 6, "2024-01-02"),
    (106, 5, "Employee Training", 3, "2024-01-03"),
    (107, 2, "Code Debugging", 7, "2024-01-03"),
    (108, 3, "Financial Audit", 5, "2024-01-04"),
    (109, 1, "Compliance Review", 2, "2024-01-04"),
    (110, 2, "Security Patching", 4, "2024-01-04")
]

tasks_df = spark.createDataFrame(tasks_data, schema=tasks_schema)

# Show sample DataFrames
employees_df = employees_df.withColumn("hire_date", to_date(col("hire_date")))
employees_df.show()

tasks_df = tasks_df.withColumn("completion_date", to_date(col("completion_date")))
tasks_df.show()

joined_df = employees_df.join(tasks_df, on="employee_id", how="inner")
joined_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sum, countDistinct, desc

class EmployeeProductivityTracking:
    def __init__(self):
        pass

    def read_data(self, path):
        """Reads CSV data from DBFS."""
        return spark.read.format("csv").option("header", True).load(path)

    def employeeProductivityThreshold(self, employees_df, tasks_df, join_type, join_key, groupBy_key, avg_key, threshold):
        """Identifies employees with low productivity (avg hours spent < threshold)."""
        
        # Join both datasets
        joined_df = employees_df.join(tasks_df, on=join_key, how=join_type)

        # Calculate avg productivity per employee
        aggregated_df = joined_df.groupBy(groupBy_key).agg(
            avg(avg_key).alias("avg_productivity")
        )

        # Filter employees below threshold
        low_productivity_df = aggregated_df.filter(col("avg_productivity") < threshold)
        
        low_productivity_df.show()
        return low_productivity_df 

    def totalHoursSpentForTask(self, employees_df, tasks_df, join_type, join_key, groupBy_key, sum_key):
        """Calculates total hours spent on tasks for each department."""
        
        # Join datasets
        joined_df = employees_df.join(tasks_df, on=join_key, how=join_type)

        # Aggregate total hours spent per department
        aggregated_df = joined_df.groupBy(groupBy_key).agg(
            sum(sum_key).alias("total_hours_spent")
        )

        # Order by total hours spent
        hours_spent_df = aggregated_df.orderBy(desc("total_hours_spent"))

        hours_spent_df.show()
        return hours_spent_df

    def multipleTasksEmployees(self, employees_df, tasks_df, join_type, join_key, groupBy_keys, count_key):
        """Detects employees who completed multiple tasks on the same day."""
        
        # Join datasets
        joined_df = employees_df.join(tasks_df, on=join_key, how=join_type)

        # Count distinct tasks per employee per day
        aggregated_df = joined_df.groupBy(groupBy_keys).agg(
            countDistinct(count_key).alias("total_tasks_per_day")
        )

        # Filter employees who completed more than one task per day
        multiple_tasks_df = aggregated_df.filter(col("total_tasks_per_day") > 1)

        multiple_tasks_df.show()
        return multiple_tasks_df


# Instantiate class
employee_productivity = EmployeeProductivityTracking()    

# File paths
path1 = "dbfs:/FileStore/raw_layer/employees"
path2 = "dbfs:/FileStore/raw_layer/tasks"

# Read data once (avoid redundant reads)
employees_df = employee_productivity.read_data(path1)
tasks_df = employee_productivity.read_data(path2)

# Define parameters
join_type = "inner"
join_key = "employee_id"

# Call methods dynamically
# 1️⃣ Identify Low Productivity Employees
result_df = employee_productivity.employeeProductivityThreshold(
    employees_df, tasks_df, join_type, join_key, "employee_id", "hours_spent", threshold=50
)

# 2️⃣ Calculate Total Hours Per Department
result_df2 = employee_productivity.totalHoursSpentForTask(
    employees_df, tasks_df, join_type, join_key, "department", "hours_spent"
)

# 3️⃣ Detect Employees with Multiple Tasks Per Day
result_df3 = employee_productivity.multipleTasksEmployees(
    employees_df, tasks_df, join_type, join_key, ["employee_id", "completion_date"], "task_description"
)


# COMMAND ----------

"""
Supply Chain Optimization
📌 Scenario: Two datasets:

shipments: shipment_id, supplier_id, product_id, quantity_shipped, shipment_date
suppliers: supplier_id, name, location, rating
🔹 Task:

Identify delayed shipments (shipment date > promised delivery date).
Calculate the total quantity shipped by each supplier for each product.
Find suppliers with an average shipment rating below 3.5.
Save the optimized supplier-product mapping in Delta format."""

# COMMAND ----------

# Define schema for shipments DataFrame
shipments_schema = StructType([
    StructField("shipment_id", IntegerType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity_shipped", IntegerType(), True),
    StructField("shipment_date", StringType(), True),
    StructField("promised_delivery_date", StringType(), True)
])

# Create shipments DataFrame
shipments_data = [
    (101, 1, 5001, 100, "2024-01-01", "2024-01-03"),
    (102, 2, 5002, 200, "2024-01-02", "2024-01-05"),
    (103, 1, 5003, 150, "2024-01-04", "2024-01-06"),
    (104, 3, 5001, 300, "2024-01-05", "2024-01-04"),  # Delayed shipment
    (105, 2, 5002, 250, "2024-01-06", "2024-01-06"),
    (106, 3, 5003, 350, "2024-01-07", "2024-01-06")  # Delayed shipment
]

shipments_df = spark.createDataFrame(shipments_data, schema=shipments_schema)

# Define schema for suppliers DataFrame
suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("rating", FloatType(), True)
])

# Create suppliers DataFrame
suppliers_data = [
    (1, "ABC Corp", "New York", 4.2),
    (2, "XYZ Ltd", "Los Angeles", 3.4),  # Below threshold rating
    (3, "Global Supplies", "Chicago", 3.8)
]

suppliers_df = spark.createDataFrame(suppliers_data, schema=suppliers_schema)

# Show sample DataFrames
shipments_df = shipments_df.withColumn("shipment_date",to_date(col("shipment_date")))
shipments_df = shipments_df.withColumn("promised_delivery_date",to_date(col("promised_delivery_date")))

shipments_df.show()
suppliers_df.show()



# COMMAND ----------

# Identify delayed shipments (shipment date > promised delivery date).
delayed_shipments_df = shipments_df.filter(col("shipment_date") > col("promised_delivery_date"))
delayed_shipments_df.show()

# Calculate the total quantity shipped by each supplier for each product.
quantity_shiped_by_supplier_df = shipments_df.groupBy("supplier_id", "product_id").agg(
    sum("quantity_shipped").alias("total_quantity_per_product")
)
quantity_shiped_by_supplier_df.show()

# Find suppliers with an average shipment rating below 3.5.
avg_rating = suppliers_df.filter(col("rating") < 3.5)
avg_rating.show()

# COMMAND ----------

"""
Telecom Customer Behavior Analysis
📌 Scenario: Two datasets:

customers: customer_id, name, age, location, plan_type
call_records: call_id, customer_id, call_duration, call_type, call_timestamp
🔹 Task:

Identify customers with excessive call durations (e.g., total calls > 5 hours in a day).
Find customers who switch between plan types frequently (more than twice in 6 months).
Calculate the average call duration for each plan type.
Save the analysis results for downstream reporting"""

# COMMAND ----------

# Define schema for customers DataFrame
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("plan_change_date", StringType(), True)  # Added to track plan changes
])

# Create customers DataFrame
customers_data = [
    (101, "Alice", 30, "New York", "Premium", "2023-06-10"),
    (102, "Bob", 25, "Los Angeles", "Basic", "2023-07-15"),
    (103, "Carol", 40, "Chicago", "Standard", "2023-05-20"),
    (104, "Dave", 35, "New York", "Basic", "2023-08-12"),
    (105, "Eve", 28, "Los Angeles", "Premium", "2023-06-25"),
    (106, "Frank", 45, "Chicago", "Standard", "2023-09-10")
]

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Define schema for call_records DataFrame
call_records_schema = StructType([
    StructField("call_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("call_duration", FloatType(), True),  # Duration in minutes
    StructField("call_type", StringType(), True),  # e.g., "Local", "International", "Roaming"
    StructField("call_timestamp", StringType(), True)
])

# Create call_records DataFrame
call_records_data = [
    (1001, 101, 120.0, "Local", "2024-01-01 10:30:00"),
    (1002, 101, 200.0, "Local", "2024-01-01 14:45:00"),
    (1003, 101, 50.0, "International", "2024-01-01 18:00:00"),
    (1004, 102, 300.0, "Local", "2024-01-02 09:00:00"),
    (1005, 103, 30.0, "Roaming", "2024-01-02 12:00:00"),
    (1006, 104, 90.0, "Local", "2024-01-02 15:30:00"),
    (1007, 105, 20.0, "International", "2024-01-02 17:45:00"),
    (1008, 106, 600.0, "Local", "2024-01-02 21:00:00"),
    (1009, 101, 50.0, "Roaming", "2024-01-03 08:00:00"),
    (1010, 103, 100.0, "Local", "2024-01-03 14:00:00"),
]

call_records_df = spark.createDataFrame(call_records_data, schema=call_records_schema)

# Show sample DataFrames
customers_df = customers_df.withColumn("plan_change_date", to_date(col("plan_change_date")))
call_records_df = call_records_df.withColumn("call_timestamp", col("call_timestamp").cast(TimestampType()))

customers_df.show()
call_records_df.show()

# COMMAND ----------

# Calculate the average call duration for each plan type.
joined_df = customers_df.join(call_records_df, on="customer_id", how="inner")
joined_df.display()

aggregated_df = joined_df.groupBy("plan_type").agg(
    avg("call_duration").alias("avg_call_duration")
)

avg_duration_per_plan = aggregated_df.orderBy(desc("avg_call_duration"))
avg_duration_per_plan.show()

# COMMAND ----------

# # Identify customers with excessive call durations (e.g., total calls > 5 hours in a day)
call_durations_df = call_records_df.groupBy("customer_id").agg(
    sum("call_duration").alias("total_call_duration_per_customer")
)
customers_with_5hours_duration_df = call_durations_df.filter(col("total_call_duration_per_customer") > 300.0)
customers_with_5hours_duration_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_date, lit, add_months, unix_timestamp
from pyspark.sql.window import Window

# Define start and end date dynamically
start_date = to_date(lit("2023-06-10"))
end_date = add_months(start_date, 6)

# Filter data only for the required date range
filtered_customers_df = customers_df.filter(
    (col("plan_change_date") >= start_date) & (col("plan_change_date") <= end_date)
)

# Convert plan_change_date to a numeric format (days since epoch)
filtered_customers_df = filtered_customers_df.withColumn(
    "plan_change_date_numeric", unix_timestamp("plan_change_date") / 86400
)

# Define window specification for a 6-month rolling period (using ROWS instead of RANGE)
window_spec = Window.partitionBy("customer_id").orderBy("plan_change_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Count distinct plan changes within the rolling 6-month window
aggregated_df = filtered_customers_df.withColumn(
    "plan_count",
    count("plan_type").over(window_spec)  # ✅ FIX: Using ROWS instead of RANGE
)

# Filter customers who changed plans more than twice in 6 months
frequent_switchers_df = aggregated_df.filter(col("plan_count") > 2)

# Show result
frequent_switchers_df.show()


# COMMAND ----------

"""
Transportation Network Optimization
📌 Scenario: Two datasets:

trips: trip_id, driver_id, vehicle_id, start_time, end_time, distance
vehicles: vehicle_id, vehicle_type, capacity
🔹 Task:

Identify trips that exceed the maximum capacity of the assigned vehicle.
Calculate the average trip distance for each vehicle type.
Detect vehicles that were assigned to multiple trips overlapping in time.
Save the optimized trip schedule in Parquet format."""

# COMMAND ----------

# Sample trips DataFrame
trips_data = [
    (1, 101, 201, "2024-02-01 08:00:00", "2024-02-01 09:30:00", 15.2),
    (2, 102, 202, "2024-02-01 08:45:00", "2024-02-01 10:15:00", 20.5),
    (3, 103, 203, "2024-02-01 09:00:00", "2024-02-01 10:00:00", 12.3),
    (4, 101, 201, "2024-02-01 09:15:00", "2024-02-01 10:45:00", 18.7),  # Overlapping trip
    (5, 104, 204, "2024-02-01 10:00:00", "2024-02-01 11:30:00", 25.4),
]

trips_schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("vehicle_id", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("distance", DoubleType(), True),
])

trips_df = spark.createDataFrame(trips_data, schema=trips_schema)

# Sample vehicles DataFrame
vehicles_data = [
    (201, "Truck", 2),
    (202, "Bus", 40),
    (203, "Car", 4),
    (204, "Van", 8),
]

vehicles_schema = StructType([
    StructField("vehicle_id", IntegerType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("capacity", IntegerType(), True),
])

vehicles_df = spark.createDataFrame(vehicles_data, schema=vehicles_schema)

# Display DataFrames
trips_df = trips_df.withColumn("start_time", col("start_time").cast(TimestampType()))
trips_df = trips_df.withColumn("end_time", col("end_time").cast(TimestampType()))
trips_df = trips_df.withColumn("passenger_load", lit(30))

trips_df.show()

vehicles_df.show()

# COMMAND ----------

class Transportation:
    def __init__(self):
        pass

    def read_file(self,path):
        return spark.read.format('csv').option('header',True).load(path)

    def tripsExceddedCapacity(self, trips_df, vehicles_df, join_type, join_key):
        # Joining both trips and vehicles 
        joined_df = trips_df.join(vehicles_df, join_key, join_type)

        # Checking if passenger_load column exists before filtering
        if "passenger_load" in joined_df.columns:
            filtered_df = joined_df.filter(col("passenger_load") > col("capacity"))
        else:
            raise ValueError("Column 'passenger_load' is missing in trips dataset. Unable to check capacity.")

        return filtered_df

    def avgTripDistance(self, trips_df, groupBy_key, avg_key):

        # Aggregating avg trip distance for each vehicle
        aggregated_df = trips_df.groupBy(groupBy_key).agg(
            avg(avg_key).alias("avg_trip_distance")
        )
        avg_trip_dis_df = aggregated_df.orderBy(desc("avg_trip_distance"))

        return avg_trip_dis_df
    
    def vehiclesWithOverlappingTimes(self, trips_df, partition_col, orderBy_col, lead_col1, lead_col2):
        # window specification for start and end time
        window_spec = Window.partitionBy(partition_col).orderBy(orderBy_col)

        trips_df = trips_df.withColumn(
            "next_trip_start_time",
             lead(lead_col1).over(window_spec)
            )
        
        trips_df = trips_df.withColumn(
            "next_trip_end_time",
             lead(lead_col2).over(window_spec)
            )
        
        filtered_df = trips_df.filter(
            (col("next_trip_start_time").between(col("start_time"), col("end_time"))) |
            (col("next_trip_end_time").between(col("start_time"), col("end_time")))
        )

        vehicle_with_overlaptrips_df = filtered_df.select(
            col("vehicle_id")
        )

        return vehicle_with_overlaptrips_df
    
# Instantiate the class    
transportation = Transportation()

# File paths 
trips_path = "dbfs:/FileStore/raw_layer/trips"
vehicles_path = "dbfs:/FileStore/raw_layer/vehicles"

# Read data
trips_df = transportation.read_file(trips_path)
vehicles_df = transportation.read_file(vehicles_path)

# Define parameters
join_type = "inner"
join_key = "vehicle_id"

vehicles_exceded_capacity = transportation.tripsExceddedCapacity(trips_df, vehicles_df, join_type, join_key)
vehicles_exceded_capacity.show()

avg_trip_dis_df = transportation.avgTripDistance(trips_df, "vehicle_id", "distance")
avg_trip_dis_df.show()

vehicles_with_overlap_trips_df = transportation.vehiclesWithOverlappingTimes(trips_df, "vehicle_id", "start_time", "start_time", "end_time")
vehicles_with_overlap_trips_df.show()


# COMMAND ----------

"""
Banking Fraud Detection
📌 Scenario: You have transaction data:

transactions: transaction_id, account_id, amount, transaction_type, timestamp, location
🔹 Task:

Detect suspicious transactions where the same account has transactions in different locations within 10 minutes.
Identify accounts with more than 5 transactions marked as "failed" in a single day.
Calculate the total daily withdrawal amount for each account.
Save the flagged transactions for further analysis."""

# COMMAND ----------

# Sample transactions DataFrame
transactions_data = [
    (1, 101, 500.0, "withdrawal", "2024-02-03 08:00:00", "New York"),
    (2, 101, 200.0, "withdrawal", "2024-02-03 08:05:00", "Los Angeles"),  # Suspicious: Different location within 10 mins
    (3, 102, 1500.0, "deposit", "2024-02-03 09:00:00", "Chicago"),
    (4, 103, 100.0, "failed", "2024-02-03 10:15:00", "Boston"),
    (5, 103, 50.0, "failed", "2024-02-03 10:30:00", "Boston"),
    (6, 103, 75.0, "failed", "2024-02-03 10:45:00", "Boston"),
    (7, 103, 125.0, "failed", "2024-02-03 11:00:00", "Boston"),
    (8, 103, 90.0, "failed", "2024-02-03 11:15:00", "Boston"),  # More than 5 failed transactions
    (9, 104, 250.0, "withdrawal", "2024-02-03 12:00:00", "San Francisco"),
    (10, 104, 350.0, "withdrawal", "2024-02-03 15:30:00", "San Francisco"),
]

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True),
])

transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

# Display DataFrame
transactions_df = transactions_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
transactions_df.show()

# COMMAND ----------

# Calculate the total daily withdrawal amount for each account
transactions_df = transactions_df.withColumn("date", to_date(col("timestamp")))

df = transactions_df.groupBy("account_id", "date").agg(
    coalesce(sum(when(col("transaction_type") == "withdrawal", col("amount"))), lit(0)).alias("total_withdraw_amount")
)
df.show()

# COMMAND ----------

# Identify accounts with more than 5 transactions marked as "failed" in a single day
transactions_df = transactions_df.withColumn("date", to_date(col("timestamp")))

# Count only withdrawal transactions
df = transactions_df.groupBy("account_id", "date").agg(
    count(when(col("transaction_type") == "withdrawal", True)).alias("total_withdrawl_trans")
)

accounts_with_5withdraw_trans_df = df.filter(col("total_withdrawl_trans") > 5)
accounts_with_5withdraw_trans_df.show()

# COMMAND ----------

 # Detect suspicious transactions where the same account has transactions in different locations within 10 minutes.

window_spec = Window.partitionBy("account_id").orderBy("timestamp")

# Add Previous Location and Timestamp Columns
transactions_df = transactions_df.withColumn(
    "prev_location", lag("location").over(window_spec)
).withColumn(
    "prev_timestamp", lag("timestamp").over(window_spec)
)

# Calculate Time Difference in Minutes
transactions_df = transactions_df.withColumn(
    "time_diff_minutes",
    (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp"))) / 60
)

# Filter: Different Locations & Within 10 Minutes
suspicious_transactions_df = transactions_df.filter(
    (col("prev_location").isNotNull()) &  # Ensure not first transaction
    (col("location") != col("prev_location")) &  # Different locations
    (col("time_diff_minutes") <= 10)  # Within 10 minutes
)

suspicious_transactions_df.display()

# COMMAND ----------

"""
E-commerce Order Processing
📌 Scenario: Two datasets:

orders: order_id, customer_id, product_id, order_date, quantity
deliveries: delivery_id, order_id, delivery_status, delivery_date
🔹 Task:

Perform a join to find undelivered orders (delivery status is NULL).
Calculate the average delivery time for each product.
Identify products with late deliveries (delivery date > 7 days after order date).
Save the enriched dataset for downstream use."""

# COMMAND ----------

# Define schema for orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("order_date", StringType(), False),  # Initially as StringType to convert to DateType
    StructField("quantity", IntegerType(), False)
])

# Sample orders data
orders_data = [
    (101, 1, 501, "2024-01-01", 2),
    (102, 2, 502, "2024-01-03", 1),
    (103, 3, 503, "2024-01-05", 4),
    (104, 4, 501, "2024-01-07", 3),
    (105, 5, 504, "2024-01-08", 2),
    (106, 6, 505, "2024-01-10", 1),
]

# Create Orders DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Convert order_date to DateType
orders_df = orders_df.withColumn("order_date", to_date(col("order_date")))

# Show orders DataFrame
orders_df.show()
# Define schema for deliveries
deliveries_schema = StructType([
    StructField("delivery_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("delivery_status", StringType(), True),
    StructField("delivery_date", StringType(), True)  # Initially as StringType to convert to DateType
])

# Sample deliveries data
deliveries_data = [
    (201, 101, "Delivered", "2024-01-05"),
    (202, 102, "Delivered", "2024-01-10"),
    (203, 103, "Delivered", "2024-01-15"),
    (204, 104, "Delivered", "2024-01-12"),
    (205, 105, None, None),  # Undelivered Order
    (206, 106, "Delivered", "2024-01-20"),
]

# Create Deliveries DataFrame
deliveries_df = spark.createDataFrame(deliveries_data, schema=deliveries_schema)

# Convert delivery_date to DateType
deliveries_df = deliveries_df.withColumn("delivery_date", to_date(col("delivery_date")))

# Show deliveries DataFrame
deliveries_df.show()


# COMMAND ----------

# Identify products with late deliveries (delivery date > 7 days after order date).

# joined both orders and deliveries datsets
joined_df = orders_df.join(deliveries_df, on = "order_id", how = "left")

# filtering orders which delivered after seven days
late_deliveries_df = joined_df.filter(col("delivery_date") > date_add(col("order_date"), 7))

# show the result
late_deliveries_df = late_deliveries_df.select(
    col("order_id"),
    col("order_date"),
    col("delivery_date")
)
late_deliveries_df.show()

# COMMAND ----------

# Perform a join to find undelivered orders (delivery status is NULL).

#joined both orders and deliveries datsets
joined_df = orders_df.join(deliveries_df, orders_df["order_id"] == deliveries_df["order_id"], "left")

# filtering orders which not delivered
orders_not_delivered_df = joined_df.filter(col("delivery_status").isNull())

#show the result
orders_not_delivered_df.show()

# Calculate the average delivery time for each product.
avg_delivery_df = joined_df.withColumn(
    "avg_delivery_time",
    datediff(col("delivery_date"), col("order_date"))
)
# show the result
avg_delivery_df.display()

# COMMAND ----------

# Sample Inventory Data
inventory_data = [
    (1, 101, 50, "2024-01-10"),
    (1, 102, 30, "2024-01-11"),
    (2, 101, 20, "2024-01-12"),
    (2, 103, 40, "2024-01-13"),
    (3, 104, 60, "2024-01-14")
]

inventory_schema = StructType([
    StructField("store_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity_available", IntegerType(), False),
    StructField("last_updated", StringType(), False)
])

inventory_df = spark.createDataFrame(inventory_data, schema=inventory_schema)
inventory_df = inventory_df.withColumn("last_updated", inventory_df["last_updated"].cast(DateType()))

# Sample Sales Data
sales_data = [
    (1, 1, 101, 20, "2024-01-15"),
    (2, 1, 102, 30, "2024-01-16"),
    (3, 2, 101, 20, "2024-01-17"),
    (4, 2, 103, 10, "2024-01-18")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity_sold", IntegerType(), False),
    StructField("sale_date", StringType(), False)
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df = sales_df.withColumn("sale_date", sales_df["sale_date"].cast(DateType()))

# Show DataFrames
inventory_df.show()
sales_df.show()

# COMMAND ----------

# Perform a join to match inventory data with sales data.
joined_df = inventory_df.join(sales_df, on=["store_id", "product_id"], how="left")
joined_df.show()

# Identify products that are out of stock after sales.
df = inventory_df.join(sales_df, on=["store_id", "product_id"], how="left")

# creating column to differentiate stock
product_with_stocks_df = df.withColumn(
    "stock_available",
    col("quantity_available") - col("quantity_sold")
)
# show the result
product_out_of_stock = product_with_stocks_df.select(
    col("store_id"),
    col("product_id")
).filter(col("stock_available") == 0)

product_out_of_stock.show()

# Calculate the total quantity sold for each store.
total_quantity_sold_per_store = sales_df.groupBy("store_id").agg(
    sum("quantity_sold").alias("total_quantity_sold")
)
final_df = total_quantity_sold_per_store.orderBy(desc("total_quantity_sold"))
final_df.show()

# Use a left anti join to find products that have never been sold.
products_not_sold = inventory_df.join(sales_df, on=["store_id", "product_id"], how="leftanti")
products_not_sold.show()