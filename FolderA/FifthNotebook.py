# Databricks notebook source
#RDD
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

rdd=sc.parallelize([('tv',20000,),('laptop',40000),('tv',10000),('fridge',10000),('laptop',30000)])
grouped_rdd=rdd.groupBy(lambda a:a[0])
for key,value in grouped_rdd.collect():
    print(key,list(value))

# COMMAND ----------

rdd=sc.parallelize([('sale1',2),('sale2',4),('sale3',3),('sale1',3)])
co_rdd=sc.parallelize([('sale1','varun'),('sale2','venu'),('sale4','suresh')])
co_group_rdd=rdd.cogroup(co_rdd)
for key,value in co_group_rdd.collect():
    print(key,(list(value[0]), list(value[1])))

# COMMAND ----------

rdd=sc.parallelize([('student321','varun'),('student330','venu'),('student324','lokesh'),('student321',95)])
rdd2=sc.parallelize([('student330',95),('student324',100)])
rdd3=sc.parallelize([('student321','mech'),('student330','cse'),('student324','ece')])
group_multiple_rdd=rdd.groupWith(rdd2,rdd3)

for key,value in group_multiple_rdd.collect():
    print(key,list(value[0]),list(value[1]),list(value[2]))

# COMMAND ----------

rdd = sc.parallelize([('apple', 2), ('orange', 3)])
mapped_rdd=rdd.mapValues(lambda x:x*10)
print(mapped_rdd.collect())

# COMMAND ----------

rdd=sc.parallelize([('student321','varun'),('student302','suresh')])
mapped_rdd=rdd.mapValues(lambda x:x.upper() if isinstance(x,str) else x+"_precedding")
print(mapped_rdd.collect())

# COMMAND ----------

rdd=sc.parallelize([('student321','varun'),('student302',95)])
mapped_rdd=rdd.mapValues(lambda x:x.upper() if isinstance(x,str) else str(x)+"_precedding")
print(mapped_rdd.collect())

# COMMAND ----------

#python list comprehension
squares=[x*2 if x%2==0 else x**2 for x in range(1,10)]
print(squares)

# COMMAND ----------

dbutils.widgets.dropdown(name="student_name", defaultValue="Varun", choices=["Varun", "Suresh", "Venu"], label="Select Student")

# COMMAND ----------

#Data Filtering with Dropdown
#Imagine you have a DataFrame containing students data, and you want to filter it based on a selected Studentname:
selected_name=dbutils.widgets.get("student_name")

data=[("Varun","Mech"),("Venu","Cse"),("Suresh","Bcom")]
col=["StudentName","Major"]

#filtered data from dataframe
df=spark.createDataFrame(data,col)
df_filtered=df.filter(df.StudentName==selected_name)
df_filtered.show()