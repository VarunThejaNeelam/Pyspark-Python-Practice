# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

# #Write a program to print the multiplication table of a number using a for loop.

def multiplication(n):
    for i in range(1,11):
        print(f"Multiplication table for {n} : {n} * {i} =  {n*i}")
multiplication(2)

# COMMAND ----------

#Write a program to print the first 10 Fibonacci numbers using a while loop

def fibonacci(n):
    a = 0
    b = 1
    for i in range(1,n+1):
        fb = a + b
        print(a,end=' ')
        a = b
        b = fb

fibonacci(10)        


# COMMAND ----------

#Write a program to find the factorial of a number using a for loop.

def factorial(n):
    fact = 1
    for i in range(1,n+1):
        fact = fact * i 
    print(f"Factorial number or {n} is : {fact}")

factorial(5)        

# COMMAND ----------

#Write a program to find the sum of even numbers between 1 and 50 using a do-while loop

def sum_of_even():
    sum_of_even = 0
    n = 1

    while True:
        if n % 2 == 0:
            sum_of_even = sum_of_even + n

        n += 1

        if n > 50:
            break

    return sum_of_even

print("Sum of even numbers between 1 and 50 is:", sum_of_even())   



# COMMAND ----------

#Write a program to check if a number is prime or not using a for loop

def isPrime(n):
    fac = 0
    for i in range(1, n+1):
        if n % i == 0:
            fac += 1
    if fac == 2:
        print("Number is prime")
    else:
        print("Number is not prime")


def isPrime(n):
    if n < 2:  
        print("Number is not prime")
        return  # Stops the function here if n < 2

    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            print("Number is not prime")
            return  # Stops further checks as we already know it's not prime

    print("Number is prime")  # Only executes if no factors are found





# COMMAND ----------

#Write a program to find the GCD of two numbers using a for loop.

def greatestCommonDivisor(a, b):
    gcd_num = 0
    for i in range(1, min(a,b)+1):
        if (a % i == 0 and b % i == 0):
            gcd_num = i
    return gcd_num
greatestCommonDivisor(20, 35)

# COMMAND ----------

#Write a program to find the reverse of a number using a while loop

def reverse(num):
    rev = 0
    while num > 0:
        i = num % 10
        rev = rev * 10 + i
        num = num // 10
    return rev     

reverse(123)            

# COMMAND ----------

# Write a program to find the sum of squares of first n natural numbers using a for loop

def sum_of_squares(n):
    sumOfSquares = 0
    for i in range(1,n+1):
        sumOfSquares = sumOfSquares + i * i
    return sumOfSquares

sum_of_squares(5)    


# COMMAND ----------

"""
Assignment 1: Data Source Connector Class
Objective: Create a class that connects to different data sources like Azure Blob Storage, ADLS Gen2, and SQL Database.

Requirements:
✅ Use inheritance to define multiple connector types.
✅ Implement polymorphism to read data from different sources with a common method like .read_data().
✅ Add error handling using exception handling.

Example Structure:

DataConnector (Base Class)
BlobStorageConnector (Child Class)
SQLDatabaseConnector (Child Class)
"""

# COMMAND ----------

import ABC

class DataConnector(ABC):
    def __init__(self, container, storage_account, server, database):
        self.container = container
        self.storage_account = storage_account
        self.server = server
        self.database = database

    @AbstractMethod
    def authenticate(self):
        pass    

    @AbstractMethod
    def read_date(self,*args):
        pass
    
    @AbstractMethod
    def transform_data(self, *args):
        pass

    @AbstractMethod
    def save_data(self, *args):
        pass

    class BlobStorageConnector(DataConnector):
        def __init__(self,container,storage_account):
            super().__init__(container=container, storage_account=storage_account)

        def authenticate(self):
            configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": "66c54b55-0bb1-4e41-bd21-fd2a07b1eb50",
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="secrets_for_storage", key="secret"),
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b53c9692-2c72-4d25-8d97-f29f73dfd6e1/oauth2/token",
            }
            
            self.mount_point = "/mnt/gen2bypy"
            dbutils.fs.mount(
            source=f"abfss://{self._container_name}@{self._storage_account_name}.dfs.core.windows.net/",
            mount_point=self.mount_point,
            extra_configs=configs,
            )    

        def read_data(self, folder_name, filename, file_format):
            try:
                filepath = f"{self.mount_point}/{folder_name}/{filename}"
                df = spark.read.format(file_format).option("header", True).load(filepath)
                return df
            except Exception e:
                print(f"Error while reading the file: {e}")

        def transform_data(self, df, key):
            transformed_df = df.dropDuplicates([key])
            return transformed_df

        def save_data(self, df, folder_name, file_name, file_format):
            output_path = f"{self.mount_point}/{folder_name}/{file_name}"
            df.write.format(file_format).option("header", True).save(output_path)
            print(f"Transformed data saved at: {output_path}")


blobinst = BlobStorageConnector('raw', 'rbcriskmanage')
read_df = blobinst.read_data('netting', 'customer.csv', 'csv')            
transformed_df = blobinst.transform_data(read_df, "customer_id")
saved_df = save_data(transformed_df, "netting_analysis", "trans_cust_data", "delta")

class SqlDatabaseConnector(DataConnector):
    def __init__(self, server, database):
       super().__init__(server=server, database=database)

    def authenticate(self):
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.server}.database.windows.net:1433;"
            f"database={self.database};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )
        self.connection_properties = {
            "user": dbutils.secrets.get(scope="secrets_for_sql", key="username"),
            "password": dbutils.secrets.get(scope="secrets_for_sql", key="password"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        print("SQL Database authentication completed.")

    def read_data(self, table_name):
        try:
            df = spark.read.jdbc(
                url=self.jdbc_url, 
                table=table_name, 
                properties=self.connection_properties
                )
            return df 
        except Exception e:
            print(f"Error occured when reading table: {e}")    

    def transform_data(self, df, groupBy_keys, sum_key):
        transformed_df = df.groupBy(groupBy_keys).agg(
            sum(sum_key).alias("total_amount")
        )    
        return transformed_df

    def save_data(self, df, table_name):
        try:
            df.write.jdbc(
                url = self.jdbc_url,
                table = table_name,
                mode = "append",
                properties = self.connection_properties
            )    
        except Exception e:
            print(f"Error occured when saving data to table: {table_name}")

sqlinst = SqlDatabaseConnector("dbser", "rbc_db")                
read_df = sqlinst.read_data("transactions")
transformed_df = sqllinst.transform_data(read_df, ["customer_id", "year_month"], "amount")
saving_df = sqlinst.save_data(transformed_df, "customer_monthly_transactions")

# Function to read data dynamically
# Factory Method to dynamically instantiate connectors
def get_data(connector_type, **kwargs):
    if connector_type == "blob":
        return BlobStorageConnector(kwargs['container'], kwargs['storage_account'])
    elif connector_type == "sql":
        return SqlDatabaseConnector(kwargs['server'], kwargs['database'])
    else:
        raise ValueError("Invalid connector type. Please use 'blob' or 'sql'.")

# Example Usage
connector_ins = get_data("sql", server="dbser", database="rbc_db")
read_df = connector_ins.read_data("transactions")


# COMMAND ----------

"""
Squares of Numbers
➤ Create a list of squares for numbers from 1 to 10.
Output: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]"""

def squares(n):
    squares_list =  [i**2 for i in range(1,n+1)]
    return squares_list

squares(10)    

# COMMAND ----------

"""
Odd Numbers in Reverse
➤ Create a list of odd numbers from 20 to 1 in descending order.
Output: [19, 17, 15, 13, 11, 9, 7, 5, 3, 1]"""

def listOfOdd(n):
    list_of_odd = [i for i in range(20, 0, -1) if i % 2 != 0]
    return list_of_odd

listOfOdd(20)    

# COMMAND ----------

"""
Length of Each Word
➤ Given words = ['apple', 'banana', 'cherry'], create a list of word lengths.
Output: [5, 6, 6]"""

def lenOfWord(list_of_words):
    len_of_words = [len(element) for element in list_of_words]
    return len_of_words

words = ['apple', 'banana', 'cherry']
lenOfWord(words)    


# COMMAND ----------

"""
Numbers Divisible by Both 3 and 5
➤ Create a list of numbers between 1 and 50 that are divisible by both 3 and 5.
Output: [15, 30, 45]"""

def is_divisible(n):
    list_of_numbers = [i for i in range(1,n+1) if i % 3 == 0 and i % 5 == 0]
    return list_of_numbers

is_divisible(50)    

# COMMAND ----------

[[expression for inner_loop in iterable if condition] for outer_loop in iterable if condition]


# COMMAND ----------

"""
Words Starting with 'A'
➤ From words = ['apple', 'ant', 'bat', 'avocado', 'ball'], select words starting with "A" (case-insensitive).
Output: ['apple', 'ant', 'avocado']"""

def isChecking(words):
    list_of_words = [word for word in words if word[0].lower() == 'a']
    return list_of_words

words = ['apple', 'ant', 'bat', 'avocado', 'ball']    
isChecking(words)    

# COMMAND ----------

"""
Flatten a Nested List
➤ Given nested_list = [[1, 2], [3, 4], [5, 6]], flatten it into [1, 2, 3, 4, 5, 6]."""

def flattening(nested_list):
    flattened_list = [num for sublist in nested_list for num in sublist]
    return flattened_list

input_list = [[1, 2], [3, 4], [5, 6]]
flattening(input_list)    

# COMMAND ----------

"""
Squares of Numbers
➤ Create a dictionary with numbers 1 to 5 as keys and their squares as values.
Output: {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}"""

def dictSquares(n):
    list_of_squares = {i: i**2 for i in range(1,n+1)}
    return list_of_squares

dictSquares(5)    

# COMMAND ----------

"""
Word Length Mapping
➤ From words = ['apple', 'banana', 'cherry'], create a dictionary mapping each word to its length.
Output: {'apple': 5, 'banana': 6, 'cherry': 6}"""

def dict_length(words):
    len_of_words = {word: len(word) for word in words}
    return len_of_words

words = ['apple', 'banana', 'cherry']
dict_length(words)    

# COMMAND ----------

"""
Character Frequency
➤ Given "banana", create a dictionary showing the frequency of each character.
Output: {'b': 1, 'a': 3, 'n': 2}"""
from collections import Counter

def frequency(word):
    frequenct_dict = {char: word.count(char)for char in word}
    return frequenct_dict

frequency("banana")    

# COMMAND ----------

"""
Even-Odd Mapping
➤ Create a dictionary mapping numbers 1 to 10 as keys, and "even" or "odd" as values.
Output: {1: 'odd', 2: 'even', 3: 'odd', 4: 'even', ...}"""

def even_odd(n):
    even_odd_dict = {i: "even" if i % 2 == 0 else "odd" for i in range(1,n+1)}
    return even_odd_dict

print(even_odd(10))    

# COMMAND ----------

"""
Word Count from a Sentence
➤ From "I love Python Python is great", count each word’s frequency.
Output: {'I': 1, 'love': 1, 'Python': 2, 'is': 1, 'great': 1}"""

def word_count(sentence):
    result = sentence.split(" ")
    word_count = {word: result.count(word) for word in result}
    return word_count

word_count("I love Python Python is great")    

# COMMAND ----------

"""
ASCII Mapping
➤ Create a dictionary mapping lowercase letters from "a" to "z" to their ASCII values."""
import string

def mapping(letters):
    letter_mapping = {char: ord(char) for char in letters}
    return letter_mapping

letters = string.ascii_lowercase
print(mapping(letters))

# COMMAND ----------

"""
Create a custom exception called NullValueError that raises an error if a DataFrame has NULL values in any of the key columns. Add proper logging and retry logic.

If you’d like guidance on that, let me know!"""

class NullValueError(Exception):
    def __init__(self,df):
        super().__init__(f"Datafarme as null for key columns")


def checking_nulls(df):
    null_checking={key:"value_is_null" for key value in df.items() if value.isNull() }
    if value in null_checking == "value_is_null":
        raise NullValueError(df)
    print("There is no null for values")

data = [
    {"id":None, "name":"varun"},
    {"id":2, "name":"suresh"}
]
json_schema = StructType([
    StructField("id", IntegerType(),True),
    StructField("name", StringType(),True)
])
df = spark.createDataFrame(data, json_schema)

try:
    checking_null(df)
except NullValueError as e:
    print(e)    