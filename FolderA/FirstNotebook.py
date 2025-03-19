# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS emp;
# MAGIC CREATE TABLE emp USING CSV OPTIONS(path"/FileStore/tables/emp.csv",header="true")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM emp

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

df=spark.read.format("csv").option("header","true").load("/FileStore/tables/emp.csv")

# COMMAND ----------

data=[(1,"varun"),(2,"suresh"),(3,"venu"),(4,"lokesh")]
columns=['id','name']

df=spark.createDataFrame(data,columns)
display(df)

# COMMAND ----------

dbutils.fs.mv('FileStore/tables/sale1.csv','FileStore/data')

# COMMAND ----------

dbutils.help('meta')

# COMMAND ----------

def sum_even_odd(n):
    sum_even=0;
    sum_odd=0;
    for i in range(1,n+1):
        if i%2 ==0:
            sum_even+=i
        else:
            sum_odd+=i
    return sum_even,sum_odd        
                  
result_even, result_odd = sum_even_odd(10)
print(f"Sum of even numbers: {result_even}")
print(f"Sum of odd numbers: {result_odd}")                  


# COMMAND ----------

#python code to find out maximum value in list
def find_max(list):
    maximum=list[0]
    for i in list:
        if i>maximum:
            maximum=i
            
    print(f"Maximum number in list is:{maximum}")
    return maximum       

print(find_max([4, 7, 1, 9, 2]))            


# COMMAND ----------

#python code to calculate positive and negative numbers in list
def count_pos_neg(list):
    count_pos=0;
    count_neg=0;
    for i in list:
        if i>0:
            count_ps +=1
        else:
            count_neg +=1
    return count_pos,count_neg 

result_count_pos,result_count_neg=count_pos_neg([1, -2, 3, -4, 5]])  
print(f"Positive count is :{result_count_pos}")
print(f"Negative count is :{result_count_neg}")              

# COMMAND ----------

data=[(1,'Varun',45000),(2,'Suresh',50000),(3,'Venu',60000)]
columns=StructType([StructField('EmpID',IntegerType(),True),
                    StructField('Name',StringType(),True),
                    StructField('Salary',IntegerType(),True)])
         


# COMMAND ----------

df=spark.createDataFrame(data,columns)

df.show()