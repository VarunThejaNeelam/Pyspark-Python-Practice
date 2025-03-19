# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import*
from pyspark.sql.functions import* 
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

"""
Clickstream Analysis (Sessionization)
Dataset: Web clickstream data with fields:
user_id, timestamp, url, session_id.

Tasks:
Read and parse timestamp values.
Identify user sessions (a session ends if there is no activity for more than 30 minutes).
Compute session duration for each user.
Find most visited URLs per session.
Detect users who visited the website but didn’t perform any actions."""

# COMMAND ----------

# Sample data
data = [
    ("user_1", "2024-03-11 10:00:00", "/home", None),
    ("user_1", "2024-03-11 10:15:00", "/product", None),
    ("user_1", "2024-03-11 10:50:00", "/cart", None),
    ("user_2", "2024-03-11 11:00:00", "/home", None),
    ("user_2", "2024-03-11 11:10:00", "/checkout", None),
    ("user_3", "2024-03-11 12:00:00", "/home", None),
    ("user_3", "2024-03-11 13:00:00", "/product", None),  # 1-hour gap triggers a new session
    ("user_4", "2024-03-11 14:00:00", "/home", None)  # Only visited the home page
]

# Schema definition
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # String initially, will convert to TimestampType
    StructField("url", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Creating DataFrame
df = spark.createDataFrame(data, schema)

# Converting timestamp column to TimestampType
df = df.withColumn("timestamp", to_timestamp("timestamp"))

df.show()

# COMMAND ----------

# Detect users who visited the website but didn’t perform any actions.

filtered_users_df = df.filter(
    (col("timestamp").isNotNull()) &
    (col("url").isNull())
)
filtered_users_df.select("user_id").show()

# COMMAND ----------

# Find most visited URLs per session.

# Window Specification
windowSpec = Window.partitionBy("user_id").orderBy("timestamp")

# Calculate time difference
df = df.withColumn("time_diff", (F.unix_timestamp("timestamp") -
                                 F.unix_timestamp(F.lag("timestamp").over(windowSpec))) / 60)

# Assign session group logic
df = df.withColumn(
    "session_group",
    when(col("time_diff") <= 30, lit(None))  # Same session, no new ID needed
    .when((col("time_diff") > 30) & (lag("time_diff").over(windowSpec) <= 30),
          concat(F.col("user_id"), lit("_"), expr("uuid()")))  # New session
    .when((col("time_diff") > 30) & (lag("time_diff").over(windowSpec) > 30),
          concat(F.col("user_id"), lit("_"), expr("uuid()")))  # Consecutive 30+ gaps
)

# Fill forward session_group for same-session records
df = df.withColumn("session_group", last("session_group", ignorenulls=True).over(windowSpec))

# Aggregating each url count for each user by pivoting it
pivoted_df = df.groupBy("user_id", "session_group").pivot("url").agg(count("*"))

# Unpivoting it to get url's as seperate columns and there counts
unpivoted_df = pivoted_df.select(
    "user_id",
    "session_group",
    expr("stack(4, '/home', home, '/product', product, '/cart', cart, '/checkout', checkout) AS (url, visit_count)")
)

# Ranking the urls for each user session
window_rank = Window.partitionBy("user_id", "session_group").orderBy(desc("visit_count"))
ranking_df = unpivoted_df.withColumn(
    "rank",
    dense_rank().over(window_rank)
)

# Filtered the top url by rank
filtered_df = ranking_df.filter(col("rank") == 1)
filtered_df.show()


# COMMAND ----------

"# Compute session duration for each user.

# Create DataFrame
df = spark.createDataFrame(data, ["user_id", "timestamp"]).withColumn(
    "timestamp", F.to_timestamp("timestamp")
)

# Window Specification
windowSpec = Window.partitionBy("user_id").orderBy("timestamp")

# Calculate time difference
df = df.withColumn("time_diff", (F.unix_timestamp("timestamp") -
                                 F.unix_timestamp(F.lag("timestamp").over(windowSpec))) / 60)

# Assign session group logic
df = df.withColumn(
    "session_group",
    when(col("time_diff") <= 30, lit(None))  # Same session, no new ID needed
    .when((col("time_diff") > 30) & (lag("time_diff").over(windowSpec) <= 30),
          concat(F.col("user_id"), lit("_"), expr("uuid()")))  # New session
    .when((col("time_diff") > 30) & (lag("time_diff").over(windowSpec) > 30),
          concat(F.col("user_id"), lit("_"), expr("uuid()")))  # Consecutive 30+ gaps
)

# Fill forward session_group for same-session records
df = df.withColumn("session_group", last("session_group", ignorenulls=True).over(windowSpec))

# Aggrgateing total records for each session to filter
window_spec = Window.partitionBy("user_id", "session_group")
counting_df = df.withColumn(
    "total_records",
    count("*").over(window_spec)
)

# Filtering only more than one record session
filtered_df = aggregated_df.filter(col("total_records") > 1)
total_duration_df = filtered_df.groupBy("user_id","session_group").agg(
    sum("time_diff").alias("total_duration")
)

# Joining the both total duration df and non filtered df counting df
joined_df = filtered_df.join(total_duration_df, on=["user_id", "session_group"], "left").fillna(0,subset="total_duration")

final_df = joined_df.select(
    distinct(col("user_id")),
    col("session_id"),
    col("total_duration")
)
final_df.show()


# COMMAND ----------

# Simulate streaming data ingestion (use .readStream() on an existing dataset).

# Window specification for sessionization
windowSpec = Window.partitionBy("user_id").orderBy("timestamp")

# Identify session start points
df = df.withColumn("prev_timestamp", lag("timestamp").over(windowSpec))
df = df.withColumn("time_diff", (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60)
df = df.withColumn("new_session_flag", when(F.col("time_diff") >= 30, 1).otherwise(0))

df = df.withColumn("session_group", sum("new_session_flag").over(windowSpec))
df = df.withColumn("session_id", expr("uuid()"))

# Assign session IDs based on session grouping
df = df.withColumn("final_session_id", first("session_id").over(Window.partitionBy("user_id", "session_group").orderBy("timestamp")))
df = df.drop("prev_timestamp", "time_diff", "new_session_flag", "session_group", "session_id")

df.show()

# COMMAND ----------

"""
 Processing IoT Sensor Data (Streaming Simulation)
Dataset: Simulated sensor data:
device_id, timestamp, temperature, humidity, status.

Tasks:
Simulate streaming data ingestion (use .readStream() on an existing dataset).
Filter faulty sensors (devices reporting temperature spikes).
Compute moving average temperature per device.
Write output as a table, updating results every 5 seconds."""

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE RawIotSensorData(
# MAGIC   CONSTRAINT valid_device_id EXPECT(device_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC TBLPROPERTIES(quality = 'bronze')
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   device_id,
# MAGIC   timestamp,
# MAGIC   temperature,
# MAGIC   humidity,
# MAGIC   status
# MAGIC FROM cloud_files("dbfs://FileStore/raw_layer/iot", "csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE FaultySensorAnalysis
# MAGIC TBLPROPERTIES(quality = 'silver')
# MAGIC AS
# MAGIC WITH cte AS(
# MAGIC   SELECT device_id,
# MAGIC          timestamp,
# MAGIC          temperature,
# MAGIC          LAG(temperature)OVER(PARTITION BY device_id ORDER BY timestamp) prev_temperature
# MAGIC   FROM STREAM(LIVE.RawIotSensorData)
# MAGIC ),
# MAGIC cte2 AS(
# MAGIC   SELECT device_id,
# MAGIC          SUM(CASE WHEN temperature > prev_temperature THEN 1 else 0 END) AS total_temperature_spikes,
# MAGIC          COUNT(*) AS total_count 
# MAGIC   FROM cte
# MAGIC   WHERE prev_temperature IS NOT NULL
# MAGIC   GROUP BY device_id 
# MAGIC )
# MAGIC SELECT device_id
# MAGIC FROM cte2
# MAGIC WHERE total_temperature_spikes = total_count

# COMMAND ----------

# MAGIC %sql
# MAGIC --Filter faulty sensors (devices reporting temperature spikes).
# MAGIC CREATE MATERIALIZED VIEW FaultySensors
# MAGIC TBLPROPERTIES (quality = "gold")
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM STREAM(LIVE.FaultySensorAnalysis)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE MovingAverageAggregation
# MAGIC TBLPROPERTIES(quality = 'silver')
# MAGIC AS
# MAGIC SELECT device_id,
# MAGIC       AVG(temperature)OVER(PARTITION BY device_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS moving_avg
# MAGIC FROM STREAM(LIVE.RawIotSensorData)      

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE MATERIALIZED VIEW MovingAverage
# MAGIC TBLPROPERTIES (quality = 'gold')
# MAGIC AS
# MAGIC SELECT device_id,
# MAGIC       moving_avg
# MAGIC FROM STREAM(LIVE.MovingAverageAggregation)       