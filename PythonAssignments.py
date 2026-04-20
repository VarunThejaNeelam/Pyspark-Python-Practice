# Databricks notebook source
from pyspark.sql.types import*
from pyspark.sql.functions import*
import copy


# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 1: Shallow Copy Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC Shallow copy shares same memory for both original and copy lists

# COMMAND ----------

#original list
lists=[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
#copy list
copy_list=copy.copy(lists)
copy_list[0][1]=20
print(f"Original list:{lists}")
print(f"Copyied list:{copy_list}")

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 2: Combining Deep and Shallow Copies

# COMMAND ----------

#original dictionary
dict={'a':1,'b':{'c':2,'d':3}}
#shallow copy
shallow_dict=copy.copy(dict)
shallow_dict['b']['d']=30
print(f"Original shallow dictionary:{dict}")
print(f"Shallow copy dictionary:{shallow_dict}")
#deep copy
deep_dict=copy.deepcopy(dict)
deep_dict['b']['c']=20
print(f"Original deep dictionary:{dict}")
print(f"Deep copy dictionary{deep_dict}")

# COMMAND ----------

# MAGIC %md
# MAGIC Assignment 3: Creating a Simple Decorator

# COMMAND ----------

def greet(name):
  con_upper=name.upper
  return con_upper

# COMMAND ----------

#Converting greet to uppercase decorator
def uppercase_decorator(func):
    def wrapper(name):
        # Call the original function with the argument and convert the result to uppercase
        return "Uppercase decorator of greet: "+ func(name).upper()
    return wrapper

@uppercase_decorator
def greet(name):
  return f"hello {name}" # Added a space for readability

result = greet('world')  
print(result)

# COMMAND ----------

"""
2. Inheritance
Task:

Create a parent class AzureServiceHandler with a method authenticate() to simulate authentication.
Create child classes:
BlobHandler for handling Azure Blob Storage operations.
SQLHandler for handling Azure SQL Database operations.
Add methods in each child class:
BlobHandler: upload_blob(blob_name, data) and download_blob(blob_name).
SQLHandler: insert_data(table_name, data) and fetch_data(table_name).
Use the authenticate() method in both child classes."""

# COMMAND ----------

class AzureServiceHandler:
    def __init__(self,container_name,storage_account_name):
        self._container_name = container_name
        self._storage_account_name = storage_account_name

    def authenticate(self):
         raise NotImplementedError("This method should be implemented by subclasses")

class SqlHandler(AzureServiceHandler):
     def __init__(self, server_name, database_name, user_name, password):
        super().__init__()
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password

     def authenticate(self):
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.server_name}.database.windows.net:1433;"
            f"database={self.database_name};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )
        self.connection_properties = {
            "user": self.user_name,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        print("SQL Database authentication completed.")

     def fetch_data(self,table_name):
        try:
            df=spark.read.jdbc(url=self.jdbc_url,table=table_name,properties=self.connection_properties)
            return df
        except Exception as e:
            print(f"Error reading data: {e}")    
            return None

     def insert_data(self,table_name,data):
        try:
            df = spark.createDataFrame(data)  # Assuming `data` is a list of dictionaries
            df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="append",
                properties=self.connection_properties
            )
            print(f"Data inserted into table {table_name}.")
        except Exception as e:
            print(f"Error inserting data: {e}")


class BlobHandler(AzureServiceHandler):
    def __init__(self, container_name, storage_account_name):
        super().__init__(container_name, storage_account_name)

    def authenticate(self):
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": "66c54b55-0bb1-4e41-bd21-fd2a07b1eb50",
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="secrets_for_storage", key="secret"),
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b53c9692-2c72-4d25-8d97-f29f73dfd6e1/oauth2/token",
        }

        dbutils.fs.mount(
            source=f"abfss://{self._container_name}@{self._storage_account_name}.dfs.core.windows.net/",
            mount_point="/mnt/gen2bypy",
            extra_configs=configs,
        )
        print("Blob Storage authentication and mounting completed.")           
    
    def download_blob(self, folder_name, file_name):
        try:
            file_path = f"/mnt/gen2bypy/{folder_name}/{file_name}"
            df = spark.read.format("csv").option("header", True).load(file_path)
            print(f"Data downloaded from {file_path}.")
            return df
        except Exception as e:
            print(f"Error downloading blob: {e}")
            return None
    
    def upload_blob(self, folder_name, file_name, data):
        try:
            file_path = f"/mnt/gen2bypy/{folder_name}/{file_name}"
            df = spark.createDataFrame(data)  # Assuming `data` is a list of dictionaries
            df.write.format("csv").option("header", True).save(file_path)
            print(f"Data uploaded to {file_path}.")
        except Exception as e:
            print(f"Error uploading blob: {e}")

# Example Usage
# sql_handler = SqlHandler("server_name", "database_name", "user_name", "password")
# sql_handler.authenticate()
# sql_handler.insert_data("table_name", [{"col1": "value1", "col2": "value2"}])

# blob_handler = BlobHandler("container_name", "storage_account_name")
# blob_handler.authenticate()
# blob_handler.upload_blob("folder_name", "file_name.csv", [{"col1": "value1", "col2": "value2"}])


# COMMAND ----------

"""1. Encapsulation
Task:

Create a class AzureBlobHandler that encapsulates:
Connection string and container name as private attributes.
Methods for uploading and downloading blobs.
Ensure that the connection logic is private and not directly accessible from outside the class.
Use the class to upload and download a file named data.csv."""

# COMMAND ----------

class AzureBlobHandler:
    def __init__(self,mount_point,directory_name):
        self.__mount_point = mount_point
        self.__directory_name = directory_name

    def read_data(self,file_name,file_format="csv"):
        try:
           df=spark.read.format(file_format).option('header',True).load(
               f"{self.__connection_string}/{self.__container_name}/{file_name}"
            )
           return df
        except Exception as e:
            print(f"Error reading data: {e}")
            return None
    
    def write_data(self,df,file_name,file_format="csv"):
        try:
           df.write.format(file_format).option('header',True).save(
               f"{self.__connection_string}/{self.__container_name}/{file_name}"
            )
           return "Data is written to {self.__connection_string}/{self.__container_name}/{file_name}"
        except Exception as e:
            print(f"Error writing data: {e}")
            return None
        
    def get_mount_point(self):
        return self.__mount_point

    def set_mount_point(self, mount_point):
        self.__mount_point = mount_point
        return "Mount point updated."

    def get_directory_name(self):
        return self.__directory_name

    def set_directory_name(self, directory_name):
        self.__directory_name = directory_name
        return "Directory name updated."
    
# Example usage
azure_blob = AzureBlobHandler("dbfs:/mnt/gen2byspn", "FolderA")
df = azure_blob.read_data("sales_data.csv")
if df is not None:
    print(df.show())

write_status = azure_blob.write_data(df, "sales_updated_data.csv")
print(write_status) 




# COMMAND ----------

"""
 Polymorphism
Task:

Create an abstract class DataTransformer with a method transform(data).
Implement the class for the following file types:
CSVTransformer: Add logic for transforming CSV data.
JSONTransformer: Add logic for transforming JSON data.
ParquetTransformer: Add logic for transforming Parquet data.
Write a pipeline function that accepts a list of DataTransformer objects and applies the transform method to different file types."""

# COMMAND ----------

class DataTransformer(ABC):
    def __init__(self,path,format_type):
        self.path=path
        self.format_type=format_type
        
    @abstractmethod    
    def transformer(self):
        pass
        
# CSVTransformer class
class csvTransformer(DataTransformer):
    def transformer(self):
        df=spark.read.format(self.format_type).load(self.path)
        csv_df=df.fillna(0)
        print(f"Transformation done for csv file {self.path}")
# JSONTransformer class    
class jsonTransformer(DataTransformer):
    def transformer(self):
        df=spark.read.format(self.format_type).load(self.path)

        # Flatten JSON structure
        def flatten_json_data(json_data,parentkey='',sep="_"):
            items={}
            for key,value in json_data.items():
                #create new key
                new_key=f"{parent_key}{sep}{key} if parent_key else key"
                #check if value is a dictionary
                if isinstance(value,dict):
                    items.update(flatten_json_data(value,new_key,sep=sep))
                elif instance(value,list):
                    for i,item in enumerate(value):
                        if instance(item,dict):
                            items.update(flatten_json_data(item,f"{new_key}{sep}{i}",sep=sep))
                        else:
                            items[f"{new_key}{sep}{i}"]=item 
                else:
                    items[new_key]=value
             return items
        
         # Assume df is a DataFrame with JSON rows, flatten each row
        json_rdd = df.rdd.map(lambda row: flatten_json(row.asDict()))
        flattened_df = spark.read.json(json_rdd)

        print(f"Transformation done for JSON file at {self.path}")
        return flattened_df
    

class parquetTransformer(DataTransformer):
    def transformer(self):
        df=spark.read.format(self.format_type).load(self.path)
        parquet_df=df.dropDuplicates()
        print(f"Transformation done for Parquet file at {self.path}")
        return parquet_df

# Factory function to get the appropriate transformer
def get_data_source(datatype,file_path):
    if datatype="csv":
        return csvTransformer(file_path,"csv")
    elif datatype="json":
        return jsonTransformer(file_path,"json")
    elif datatype="parquet":
        return parquetTransformer(file_path,"parquet")
    else:
        raise ValueError(f"Unsupported datatype: {datatype}")  

# Pipeline function
def pipeline(transformers):
    results = []
    for transformer in transformers:
        transformed_data = transformer.transform()
        results.append(transformed_data)
    return results

# Example usage
if __name__ == "__main__":
    csv_transformer = CSVTransformer("/path/to/csvfile.csv", "csv")
    json_transformer = JSONTransformer("/path/to/jsonfile.json", "json")
    parquet_transformer = ParquetTransformer("/path/to/parquetfile.parquet", "parquet")

    transformers = [csv_transformer, json_transformer, parquet_transformer]
    transformed_data = pipeline(transformers)

    for data in transformed_data:
        data.show()

# COMMAND ----------

"""
Combined Project: ETL Pipeline
Task:

Create a parent class ETLFramework with methods:
extract(): To extract data from a source.
transform(data): To transform the data.
load(data): To load the data into a target.
Create child classes:
BlobToSQL for extracting data from Azure Blob Storage and loading it into Azure SQL Database.
BlobToLake for extracting data from Azure Blob Storage and loading it into Azure Data Lake.
SQLToBlob for extracting data from Azure SQL Database and saving it to Azure Blob Storage.
Implement specific logic in each class for extracting, transforming, and loading data.
Write a script to dynamically call one of these pipelines based on user input """

# COMMAND ----------

class ETLFramework:
    def __init__(self,mounting_point,folder,fileName):
        self.mounting_point=mounting_point
        self.folder=folder
        self.fileName=fileName

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

class BlobToSql(ETLFramework):
    def __init__(self, server_name, database_name, user_name, password, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.server_name}.database.windows.net:1433;"
            f"database={self.database_name};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )
        self.connection_properties = {
            "user": self.user_name,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

    def extract(self): 
        path = f"{self.mounting_point}/{self.folder}/{self.fileName}"
        df=spark.read.format("csv").load(path)
        df.show()

        inputdf={
        "dynamic_df":df
        }
        return inputdf 
    
    def transformer(self,inputdf):
        # Get the DataFrame from the input
        df=inputdf.get("df")
        # Get the schema of the DataFrame
        schema=df.schema

         # Dynamically create new records as a list of Row objects
        # Ensure the schema matches the original DataFrame
        new_data = [
            Row(**{col.name: None for col in schema.fields}),  # Example empty row
            Row(**{col.name: f"New_{col.name}_Value" for col in schema.fields})  # Example new record
        ]

        #creating dataframe with new data
        insert_df=spark.createDataFrame(new_data,schema)

        combined_df=df.union.(insert_df)

        inputdf["df"]=combined_df
        return inputdf
    

    def load(self,table_name,inputdf):
        df = inputdf.get("df")
        try:  
            df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="append",
                properties=self.connection_properties
            )
            print(f"Data inserted into table {table_name}.")
        except Exception as e:
            print(f"Error inserting data: {e}")

        return f"Dataframe is inserted into tabler:{self.table_name}"

class BlobToLake(ETLFramework):
    def __init__(self, gen2_mounting_point, directory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gen2_mounting_point = gen2_mounting_point
        self.directory = directory
    
    def extract(self):
        #path to blob to get file
        path=f"{self.mounting_point}/{self.folder}/{self.fileName}"

        df=spark.read.format("csv").load("path")
        df.show()

        inputdf={
            "df":df
        }            
        return inputdf

    def transform(self,inputdf):
        #getting dataframe from inputdf
        df=inputdf.get("df")
        #Handling nulls
        transformed_df=df.fillna(0)

        inputdf["df"]=transformed_df
        return inputdf
    
    def load(self, inputdf,filename):
        df=inputdf.get("df")
        #writing dataframe to data lake
        loading_path=f"{self.gen2_mounting_point}/{self.directory}/{filename}" 
        df.write.format("delta").save(loading_path)
        return f"File is loaded to destination :{loading_path}"
    
class SqlToBlob(ETLFramework):
    def __init__(self, server_name, database_name, user_name, password, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.jdbc_url = (
            f"jdbc:sqlserver://{self.server_name}.database.windows.net:1433;"
            f"database={self.database_name};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )
        self.connection_properties = {
            "user": self.user_name,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }


    def extract(self,table_name):
        df=spark.read.jdbc(url=self.jdbc_url,table=table_name,properties=self.connection_properties)
        df.show()

        inputdf={
            "df":df
        }
        return inputdf
    
    def transform(self,inputdf):
        # Get the DataFrame from the input
        df=inputdf.get("df")
        # Get the schema of the DataFrame
        schema=df.schema

        # Dynamically create new records as a list of Row objects
        # Ensure the schema matches the original DataFrame
        new_data = [
            Row(**{col.name: None for col in schema.fields}),  # Example empty row
            Row(**{col.name: f"New_{col.name}_Value" for col in schema.fields})  # Example new record
        ]

        #creating dataframe with new data
        insert_df=spark.createDataFrame(new_data,schema)

        combined_df=df.union.insert_df

        inputdf["df"]=combined_df
        return inputdf
    
    def load(self,inputdf,filename):
        #getting dataframe
        df=inputdf.get("df")
        #path to destination
        path=f"{self.mounting_point}/{self.folder}"

        df.write.format("delta").save(path)
    


def main():
    pipeline_type = input("Enter pipeline type (BlobToSql, BlobToLake, SqlToBlob): ")

    if pipeline_type == "BlobToSql":
        pipeline = BlobToSql(
            server_name="server",
            database_name="database",
            user_name="user",
            password="password",
            mounting_point="/mnt/blob",
            folder="data",
            fileName="file.csv",
        )
        input_data = pipeline.extract()
        transformed_data = pipeline.transform(input_data)
        pipeline.load(transformed_data, "target_table")

    elif pipeline_type == "BlobToLake":
        pipeline = BlobToLake(
            gen2_mounting_point="/mnt/lake",
            directory="processed",
            mounting_point="/mnt/blob",
            folder="data",
            fileName="file.csv",
        )
        input_data = pipeline.extract()
        transformed_data = pipeline.transform(input_data)
        pipeline.load(transformed_data, "processed_file.csv")

    elif pipeline_type == "SqlToBlob":
        pipeline = SqlToBlob(
            server_name="server",
            database_name="database",
            user_name="user",
            password="password",
            mounting_point="/mnt/blob",
            folder="output",
        )
        input_data = pipeline.extract("source_table")
        transformed_data = pipeline.transform(input_data)
        pipeline.load(transformed_data, "output_file.csv")

    else:
        print("Invalid pipeline type")

if __name__ == "__main__":
    main()



# COMMAND ----------

#Write a program to print the multiplication table of a number using a for loop.
def multiplication_table(n):
    for i in range(1,n+1):
        for j in range(1, n+1):
            print(f"Multiplication table for {i}: {i} * {j} = {i * j}")

multiplication_table(10)        

# COMMAND ----------

def multi_table(n):
    i = 1
    while i <= 10:
        print(f"Multiplication table for {n}: {n} * {i} = {n * i}")
        i += 1
multi_table(5)


# COMMAND ----------

#Write a program to print the first 10 Fibonacci numbers using a while loop
def fibonacci_numbers():
    a=0
    b=1
    count=0
    while count < 10:
        print(a, end=" ")
        fibonacci_number = a + b
        a = b
        b = fibonacci_number
        count += 1
        
fibonacci_numbers()

# COMMAND ----------

#Write a program to find the factorial of a number using a for loop.
def factorial_number(n):
    fac_num=1
    for i in range(1,n+1):
        fac_num = fac_num * i
    return fac_num     
 
# Input from user
number = int(input("Enter a number: "))

factorial_number(number)

# COMMAND ----------

#Write a program to find the sum of even numbers between 1 and 50 using a do-while loop
def sum_of_even():
    i=1
    sumofeven=0
    while True:
        if i%2 == 0:
            sumofeven = sumofeven + i
        i += 1
        if i > 50:
            break;
    return sumofeven         


# COMMAND ----------

#Write a program to check if a number is prime or not using a for loop
def prime_number(n):
    if n <= 1:  # Handle special cases
        print("Given number is not prime")
        return
    
    factor_count = 0
    for i in range(1, n + 1):  # Loop from 1 to n
        if n % i == 0:  # Check divisibility
            factor_count += 1
    
    if factor_count == 2:  # Prime numbers have exactly two factors
        print(f"{n} is a prime number")
    else:
        print(f"{n} is not a prime number")

# Test the function
num = int(input("Enter a number: "))
prime_number(num)

#Efficient version to check upto square root
def prime_number(n):
    if n <= 1:
        print(f"{n} is not a prime number")
        return
    
    for i in range(2, int(n**0.5) + 1):  # Check up to square root of n
        if n % i == 0:
            print(f"{n} is not a prime number")
            return
    
    print(f"{n} is a prime number")

# Test the function
num = int(input("Enter a number: "))
prime_number(num)



# COMMAND ----------

"""
Find Prime Numbers from 1 to 100
Use loops to print all prime numbers between 1 and 100.
"""

def prime_numbers(n):
    prime_num = []
    for i in range(2, n + 1):
        fact_count = 0
        for j in range(2, i + 1):
            if i % j == 0:
                fact_count += 1

        if fact_count == 1:
            prime_num.append(i)       

    return prime_num

result = prime_numbers(100)            
print(result)

# COMMAND ----------

"""
Find Prime Numbers from 1 to 100
Use loops to print all prime numbers between 1 and 100.
"""

def prime_numbers(n):
    prime_num = []
    for i in range(2, n + 1):
        is_prime = True
        for j in range(2, int(i ** 0.5) + 1):
            if i % j == 0:
                is_prime = False
                break

        if is_prime:
            prime_num.append(i)       

    return prime_num

result = prime_numbers(100)            
print(result)

# COMMAND ----------

"""
Matrix Transpose (Without using NumPy)
Given a 2D list (matrix), transpose it using nested loops.
[[1, 2, 3],
[4, 5, 6]]

output:
[[1, 4],
[2, 5],
[3, 6]]
"""

def transposeMatrix(matrix):
    transpose = []
    for i in range(0, len(matrix[0])):
        temp = []
        for sublist in matrix:
            temp.append(sublist[i])
        transpose.append(temp)    

    return transpose 

matrix = [[1, 2, 3], [4, 5, 6]]
result = transposeMatrix(matrix)        
print(result)

# COMMAND ----------

#Write a program to find the GCD of two numbers using a for loop.
def gcdOfTwoNumbers(a,b):
    gcd=0
    for i in range(1, min(a, b) + 1):
        if(a % i == 0 and b % i == 0):
            gcd = i # Update gcd
    return f"GCD of two numbers is: {gcd}"        

gcdOfTwoNumbers(48,18)

# COMMAND ----------

#Write a program to find the reverse of a number using a while loop
def reverseNumber(n):
    rev = ""
    while n > 0:
        i = n % 10
        rev += str(i)
        n = n // 10
    return int(rev)  

reverseNumber(123)    

# COMMAND ----------

#Write a program to find the reverse of a number using a while loop
def reverseNumber(n):
    rev = 0
    while n > 0:
        i = n % 10
        rev = rev * 10 + i
        n = n // 10
    return rev  

reverseNumber(123)    

# COMMAND ----------

#Write a program to find the sum of squares of first n natural numbers using a for loop
def sumOfSquares(n):
    sum = 0
    for i in range(1,n+1): # Use range(1, n+1) to iterate from 1 to n
        sum = sum + i * i   # Add the square of the current number
    return f"Sum of squares of first {n} natural numbers is: {sum}"

sumOfSquares(10)    


# COMMAND ----------

#Write a program to check if a number is Armstrong or not using a for loop.
def armStrongOrNot(n):
    sum = 0
    c = n
    num_digits = len(str(n))  # Find the number of digits in n

    for digit in str(n):  # Loop through each digit of the number
        rem = int(digit)  # Convert each character back to an integer
        sum += rem ** num_digits  # Add the power of the digit to sum

    if c == sum:
        print("Number is Armstrong")
    else:
        print("Number is not Armstrong")

# Test the function
armStrongOrNot(153)  # Example Armstrong number
armStrongOrNot(123)  # Example non-Armstrong number



# COMMAND ----------

"""
Advanced Assignment: Dynamic Pipeline with Factory Pattern
Task:

Implement a factory pattern to dynamically create instances of different pipeline classes (BlobToSQL, BlobToLake, etc.).
Add functionality to:
Extract the pipeline type and configuration from a JSON file.
Instantiate the appropriate pipeline class using the factory pattern.
Ensure that all pipeline classes follow the structure defined in the ETLFramework parent class."""

# COMMAND ----------

json_data=[(
    {
  "pipelines": [
    {
      "pipeline_type": "BlobToSQL",
      "source": "dbfs/gen2mnt",
      "destination": "SQLDatabase",
      "config": { "folder": "salesdata", "fileName": "sales.csv", "serverName":"etlser", "database":"etldb", "user_name":"varun", "password":"varun321" "table":"sales" }
    },
    {
      "pipeline_type": "BlobToLake",
      "source": "dbfs/blobmnt",
      "destination": "dbfs/gen2mnt",
      "config": { "source_folder": "raw-data", "source_filename":"emp.csv", "des_folder":"empdata", "format": "parquet" }
    }
  ]
}

)]

json_schema = StructType([
    StructField("pipelines", ArrayType(
        StructType([
            StructField("pipeline_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("config", StructType([
                StructField("container", StringType(), True),
                StructField("table", StringType(), True),
                StructField("folder", StringType(), True),
                StructField("format", StringType(), True)
            ]), True)
        ])
    ), True)
])

json_df=spark.createDataFrame(json_data,json_schema)

json_df_exploded=json_df.select(explode(col("pipelines")).alias("pipeline"))

# Flatten the JSON by selecting fields
json_flat = json_df_exploded.select(
    col("pipeline.pipeline_type").alias("pipeline_type"),
    col("pipeline.source").alias("source"),
    col("pipeline.destination").alias("destination"),
    col("pipeline.config.container").alias("container"),
    col("pipeline.config.table").alias("table"),
    col("pipeline.config.folder").alias("folder"),
    col("pipeline.config.format").alias("format")
)

pipeline_input={
    "pipeline_df":json_flat
}



# COMMAND ----------

class ETLFramework:
  def __init__(self,source,destination,config):
    self.source=source
    self.destination=destination
    self.config=config

  def run(self):
    raise NotImplementedError("Subclasses should implement the run method.")

class BlobToSql(ETLFramework):
  def __init__(self,source,destination,config):
    super().__init__(source, destination, config)
    self.jdbc_url = (
            f"jdbc:sqlserver://{self.config['serverName']}.database.windows.net:1433;"
            f"database={self.config['database']}];"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )
        self.connection_properties = {
            "user": self.config["user_name"],
            "password": self.config["password"],
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }    

  def run(self):
    file_path=f"{self.source}/{self.config['folder']}/{self.config['fileName']}"
    #reading the file as dataframe 
    df=spark.read.format(self.config["format"]).load(file_path)

    #transformations on dataframe 
    transformed_df=df.dropduplicates()
    #loading transformed dataframe
    transformed_df.write.jdbc(
      url=self.jdbc_url,
      table=self.config["table"],
      mode="append",
      properties=self.connection_properties
      )
    print("BlobToSQL pipeline executed successfully!")
  
class BlobToLake(ETLFramework):
  def __init__(self,source,destination,config):
    super().__init__(source, destination, config)

  def run(self):
    #extracting file as dataframe
    file_path = f"{self.source}/{self.config['source_folder']}/{self.config['source_filename']}"
    df=spark.read.format("csv").load(file_path) 

    #transforming the loading dataframe
    transformed_df=df.fillna(0)   
    #loading file in destination
    output_path = f"{self.destination}/{self.config['des_folder']}/output.parquet"
    transformed_df.write.format(self.config["format"]).save(output_path)
    
# Factory Pattern    
pipeline_classes = {
    "BlobToSQL": BlobToSQL,
    "BlobToLake": BlobToLake
}

# Execute pipelines dynamically
for row in json_flat.collect():
    pipeline_type = row["pipeline_type"]
    source = row["source"]
    destination = row["destination"]
    config = row["config"]

    if pipeline_type in pipeline_classes:
        pipeline_instance = pipeline_classes[pipeline_type](source, destination, config)
        pipeline_instance.run()
    else:
        print(f"Unsupported pipeline type: {pipeline_type}")



# COMMAND ----------

"""
Find the Largest Number
Write a function find_largest(numbers) that takes a list of numbers and returns the largest number. Use a loop and conditions.
"""
def largest_number(list_of_numbers):
    #assuming first value in list has large number
    large_num=list_of_numbers[0]
    #iterating through the list
    for i in list_of_numbers:
        if i > large_num:
            large_num = i
    return f"Largest number in list is {large_num}"        

numbers=[2,1,3,5,4,6,8,7,11,9]
largest_num_instance=largest_number(numbers)
print(largest_num_instance)

# COMMAND ----------

"""
FizzBuzz
Write a function fizz_buzz(n) that prints numbers from 1 to n.
Print "Fizz" for multiples of 3, "Buzz" for multiples of 5, and "FizzBuzz" for multiples of both.
"""
def fizzbuzz(n):
    for i in range(1, n + 1):
        if i % 3 == 0 and i % 5 == 0:  # Check for multiples of both 3 and 5 first
            print("FizzBuzz")
        elif i % 3 == 0:  # Check for multiples of 3
            print("Fizz")
        elif i % 5 == 0:  # Check for multiples of 5
            print("Buzz")
        else:  # For all other numbers
            print(f"Number is {i}")
fizzbuzz(15)

# COMMAND ----------

"""
Count Vowels and Consonants
Write a function count_vowels_and_consonants(string) that counts the number of vowels and consonants in a given string."""

def vowels_and_consonants(name):
    vowel_count = 0
    consonant_count = 0
    vowels = ['A', 'E', 'I', 'O', 'U', 'a', 'e', 'i', 'o', 'u']
    
    for n in name:
        if n.isalpha():  # Check if the character is alphabetic
            if n in vowels:  # Check if it is a vowel
                vowel_count += 1
            else:  # If not a vowel, it's a consonant
                consonant_count += 1
    
    print("Vowel count is:", vowel_count)
    print("Consonant count is:", consonant_count)

# Test the function
vowels_and_consonants('Varun123!')

vowels_and_consonants('Varun')            

# COMMAND ----------

def fibonacci(n):
    a = 0  # First term of the Fibonacci series
    b = 1  # Second term of the Fibonacci series
    for i in range(1, n + 1):  # Loop through n terms
        print(a, end=" ")  # Print the current term
        fibonacci_num = a + b  # Calculate the next term
        a = b  # Update a to the next term
        b = fibonacci_num  # Update b to the next term

fibonacci(15)     

# COMMAND ----------

"""
Palindrome Checker
Write a function is_palindrome(string) that checks if a given string is a palindrome (reads the same backward as forward)."""

def is_palindrome(string):
    # Convert the string to lowercase to make it case insensitive
    string = string.lower()
    
    # Initialize two pointers
    left = 0
    right = len(string) - 1
    
    while left < right:
        # Skip non-alphanumeric characters
        if not string[left].isalnum():
            left += 1
            continue
        if not string[right].isalnum():
            right -= 1
            continue
        
        # Check if the characters at the pointers are the same
        if string[left] != string[right]:
            return False
        
        # Move the pointers inward
        left += 1
        right -= 1
    
    return True

# Test the function
test_string = "A man, a plan, a canal, Panama!"
print(is_palindrome(test_string))  # Output: True


# COMMAND ----------

"""
Armstrong Number
Write a function is_armstrong(number) to check if a number is an Armstrong number (e.g., 153 = 1³ + 5³ + 3³).
"""
def is_armstrong(number):
    c = number  # Store the original number
    armstrong_num = 0
    
    while number > 0:
        n = number % 10  # Extract the last digit
        armstrong_num += n ** 3  # Cube the digit and add to the sum
        number //= 10  # Remove the last digit
    
    # Compare the calculated Armstrong sum with the original number
    if c == armstrong_num:
        print(f"{c} is an Armstrong number")
    else:
        print(f"{c} is not an Armstrong number")

# Test the function
is_armstrong(153)  # Output: 153 is an Armstrong number
is_armstrong(9474) # Output: 9474 is an Armstrong number
is_armstrong(123)  # Output: 123 is not an Armstrong number
        




# COMMAND ----------

"""
Pattern Printing
Write a function print_pattern(n) to print the following pattern for a given number n:
"""
def print_pattern(n):
    for i in range(1, n + 1):  # Outer loop for rows
        j = 1
        while j <= i:  # Inner loop for printing stars
            print('*', end=" ")  # Print '*' on the same line
            j += 1
        print()  # Move to the next line after printing stars for the row

# Test the function
print_pattern(5)
     



# COMMAND ----------

def print_pattern(n):
    for i in range(1, n + 1):
        j = 1
        k = 1
        while j <= n - i:  #  condition to print space values upto n - i
            print(' ', end=" ")
            j += 1
        
        while k <= 2 * i - 1:  #  This loop was to print stars
            print('*', end=" ")
            k += 1 
        
        print()  # ✅ Move to the next line after printing one row

print_pattern(5)              

# COMMAND ----------

#Task: Sum of Natural Numbers
#Write a Python program that takes a number n as input and calculates the sum of the first n natural numbers using a for loop.    

def sumOfNaturalNum(n):
    for i in range(1,n+1):
        sum = sum +i
    return f"Sum of {n} natural numbers: {sum}"

# COMMAND ----------

"""
While Loop - Guessing Game
Task: Number Guessing Game
Write a program that:

Picks a random number between 1 and 20.
Lets the user guess the number.
Gives hints:
"Too High!" if the guess is greater.
"Too Low!" if the guess is smaller.
Ends when the user guesses correctly or runs out of attempts (max 5)."""

import random 

def numberGuessingGame():

    num = random.randint(1, 20)
    i=1

    print("Welcome to the Number Guessing Game!")
    print("You have 5 attempts to guess the number between 1 and 20.")

    while i<=5:
        # Take user input
        guessing_num=int(input(f"Attempt {i}: Enter your guess: "))

        # Check if the guessed number is correct
        if guessing_num == num:
            print(f"Congrats! You've guessed the number {num} correctly in {i} attempts!")
            break;
        elif guessing_num > num:
            print("Too High")
        else:
            print("Too Low")

        i+= 1

         # If attempts are exhausted
        if i > 5:
            print(f"Sorry, you've run out of attempts. The number was {num}. Better luck next time!")
            
numberGuessingGame()        

        

# COMMAND ----------

"""
Task: Print a Triangle Pattern
Write a program to print the following triangle pattern using nested loops"""

def trianglePattern(n):
    for i in range(1, n+1):
        j = 1
        while j <= i:
            print('*', end=' ')
            j += 1
        print()    

trianglePattern(5)            

# COMMAND ----------

"""
Task: Print Number Pyramid
Write a program to print the following number pyramid:"""

def NumberPyramid(n):
    for i in range(1, n+1):
        j = 1
        while j <= i:
            print(f"{i}",end=' ')
            j += 1
        print()    

NumberPyramid(5)     

# COMMAND ----------

"""
Find Even Numbers
Write a program that:

Takes a list of numbers as input.
Finds and prints all even numbers in the list."""

def evenNumbers(n):
    even_numbers=[] # Initialize an empty list to store even numbers
    for i in n:
        if i%2 == 0: # Check if the number is even
            even_numbers.append(i) # Add the even number to the list
    return f"Even numbers in the list: {even_numbers}"     

n=[1,2,3,4,5,6]       
print(evenNumbers(n))

# COMMAND ----------

"""
Fibonacci Sequence
Task: Generate Fibonacci Numbers
Write a program to generate the first n Fibonacci numbers using a while loop.

Fibonacci sequence: 0, 1, 1, 2, 3, 5, 8, ...
Input: n = 7
Output: 0, 1, 1, 2, 3, 5, 8"""

def fibonacciSequence(n):
    a = 0
    b = 1
    fb = 0
    for i in range(1, n + 1):
        fb= a + b
        print(a,end=' ')
        a = b
        b = fb

fibonacciSequence(7)        

#with while loop
def fibonacciSequence(n):
    a, b = 0, 1  # Initialize the first two Fibonacci numbers
    count = 0    # Counter to keep track of numbers generated

    while count < n:  # Loop until we generate n Fibonacci numbers
        print(a, end=' ')  # Print the current number
        a, b = b, a + b    # Update to the next Fibonacci numbers
        count += 1         # Increment the counter

# Example usage
fibonacciSequence(7)


    

# COMMAND ----------

"""
Find Largest in List
Task: Largest Number in a List
Write a program that:

Takes a list of numbers as input.
Finds and prints the largest number using a loop (without using the max() function).
Example:

Input: [10, 20, 4, 45, 99]
Output: Largest = 99
"""

def largestInList(n):
    largestNumber=n[0]
    for i in n:
        if i > largestNumber:
            largestNumber = i
    return f"Largest number in list {n} is {largestNumber}"

n=[2,1,5,4,8,7]
print(largestInList(n))            

# COMMAND ----------

def factorial(n):
    result = 1
    for i in range(1,n+1):
        result = result * i
    print(f"factorial of number with for loop of {n} is :{result}")  
    

def factorialTwo(n):
    i = 1
    result =1
    while n >= i:
        result = result * i 
        i+= 1
    print(f"factorial of number with while loop of {n} is :{result}")

factorial(5)      
factorialTwo(7)         


# COMMAND ----------

"""
Count Characters in a String
Task: Character Frequency
Write a program to count the frequency of each character in a string using a loop.

Example:

Input: "hello"
Output:

h: 1
e: 1
l: 2
o: 1
"""
def countCharacters(text):
    frequency = {}  # Dictionary to store character counts
    
    for char in text:  # Loop through each character in the string
        if char in frequency:
            frequency[char] += 1  # Increment count if character already in dictionary
        else:
            frequency[char] = 1  # Initialize count for new character
    
    # Print character frequencies
    for char, count in frequency.items():
        print(f"{char}: {count}")

# Example usage
input_text = "hello"
countCharacters(input_text)

        

# COMMAND ----------

# MAGIC %md
# MAGIC List Comprehension Assignments

# COMMAND ----------

"""1. Square of numbers
Create a list of squares of numbers from 1 to 10 using list comprehension.
Expected Output: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]"""

list_of_squares = [i*i for i in range(1, 11)]
print(list_of_squares)

# COMMAND ----------

"""2. Even numbers filter
From the given list numbers = [3, 7, 8, 12, 15, 20, 25], create a new list containing only even numbers using list comprehension.
Expected Output: [8, 12, 20]"""

def even_numbers(list_of_elements):
    list_of_even = [i for i in list_of_elements if i % 2 == 0]
    return list_of_even

list_of_elements = [3, 7, 8, 12, 15, 20, 25]
print(even_numbers(list_of_elements))


# COMMAND ----------

"""Vowel extraction
Given a string "python programming", extract all the vowels using list comprehension.
Expected Output: ['o', 'o', 'a', 'i']"""

def vowel_extraction(string):
    vowels = {'a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U'}
    vowel_list = [char for char in string if char in vowels]
    return vowel_list
string = "python programming"
print(vowel_extraction(string))    

# COMMAND ----------

"""Multiplication table
Generate a 5 × 5 multiplication table using nested list comprehension."""

def multiplication():
    multiplication_list = [[i * j for j in range(1, 6)] for i in range(1, 6)]
    return multiplication_list
print(multiplication())    

# COMMAND ----------

"""
Extract words with length > 4
From the given sentence "List comprehensions are very useful in Python", create a list of words that have more than 4 letters.
Expected Output: ['comprehensions', 'useful', 'Python']
"""

sentence = "List comprehensions are very useful in Python"
words = sentence.split()  # Splitting sentence into words
long_words = [word for word in words if len(word) > 4]  # Filtering words longer than 4 characters

print(long_words)




# COMMAND ----------

"""
Replace negative numbers with zero
Given a list numbers = [-2, 5, -10, 8, 0, -3], create a new list where all negative numbers are replaced with 0.
Expected Output: [0, 5, 0, 8, 0, 0]"""

def replacingNegativeValues(nums):
    list_of_values = [0 if i < 0 else i for i in nums]
    return list_of_values
nums = [-2, 5, -10, 8, 0, -3]
print(replacingNegativeValues(nums))    

# COMMAND ----------

"""
Square dictionary
Create a dictionary where keys are numbers from 1 to 5 and values are their squares using dictionary comprehension.
Expected Output: {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}"""

def square_dict():
    square_dict = {i: i*i for i in range(1, 6)}
    return square_dict

print(square_dict())    

# COMMAND ----------

"""
Word length dictionary
Given a list of words ["apple", "banana", "cherry", "kiwi"], create a dictionary where keys are words and values are their lengths.
Expected Output: {'apple': 5, 'banana': 6, 'cherry': 6, 'kiwi': 4}"""

def length_of_words(list_of_words):
    word_length = {word: len(word) for word in list_of_words}
    return word_length
list_of_words = ["apple", "banana", "cherry", "kiwi"]    
print(length_of_words(list_of_words))    


# COMMAND ----------

"""
Invert a dictionary
Given a dictionary {1: 'one', 2: 'two', 3: 'three'}, create a reversed dictionary where values become keys and keys become values.
Expected Output: {'one': 1, 'two': 2, 'three': 3}"""

def inverting(invert_dict):
    reverse_dict = {value: key for key, value in invert_dict.items()}
    return reverse_dict
invert_dict = {1: 'one', 2: 'two', 3: 'three'}
print(inverting(invert_dict))    

# COMMAND ----------

"""
Count character occurrences
Given a string "databricks", create a dictionary where keys are characters and values are their occurrence counts.
Expected Output: {'d': 1, 'a': 2, 't': 1, 'b': 1, 'r': 1, 'i': 1, 'c': 1, 'k': 1, 's': 1}"""
 
def char_occurences(name):
    char_occur_dict = {}
    char_occur_dict = {char: char_occur_dict[char] += 1 if char in char_occur_dict else char_occur_dict[char] = 1 for char in name} 
    return char_occur_dict
name = "databricks"
print(char_occurences(name))    

# COMMAND ----------

import csv
file_path = "/dbfs/FileStore/raw_layer/products/part-00000-tid-8362351972064636292-e014970b-3f8a-4498-bfbe-c5363dd2a903-16-1-c000.csv"

with open(file_path, "r") as file:
    reader = csv.reader(file)  # Create a CSV reader object
    for row in reader:
        print(row)  # Each row is a list of values


# COMMAND ----------

df = spark.read.format('csv').option('header', True).load("dbfs:/FileStore/raw_layer/products/part-00000-tid-8362351972064636292-e014970b-3f8a-4498-bfbe-c5363dd2a903-16-1-c000.csv")
df.show()

# COMMAND ----------

nums = [1, 2, 3, 4, 5]
squares = list(map(lambda x: x*x, nums))
print(f"Squares of elements {squares}")

# COMMAND ----------

nums = [1, 2, 3, 4, 5, 6]
even_numbers = list(filter(lambda x: x % 2 == 0, nums))
print(f"Even numbers from list: {even_numbers}")

# COMMAND ----------

from functools import reduce  # ✅ Import this

nums = [10, 20, 30]
sum_of_elements = reduce(lambda x, y: x + y, nums)
print(sum_of_elements)



# COMMAND ----------

words = ["apple", "banana", "cherry"]
uppercase_letters = list(map(lambda x: x.upper(), words))
print(uppercase_letters)

# COMMAND ----------

words = ["apple", "banana", "apricot", "grape"]
filtered_words = list(filter(lambda x: x.startswith('a'), words))
print(filtered_words)

# COMMAND ----------

from functools import reduce

words = ["dog", "elephant", "cat"]
longest_word = reduce(lambda x, y: x if len(x) > len(y) else y, words)
print(longest_word)


# COMMAND ----------

def long_word(words):
    longWord = words[0]
    for word in words:
        if len(word) > len(longWord):
            longWord = word 
    return longWord

words = ["dog", "elephant", "cat", "BloodyDinosuar"]
long_word(words)


# COMMAND ----------

def multi_table(n):
    multi_list = []
    for i in range(1, n+1):
        temp = []
        for j in range(1, n+1):
            temp.append(i * j)
        multi_list.append(temp)    
    return multi_list

multi_table(10)        


# COMMAND ----------

def rec_stars(r, c):
    for i in range(0, r):
        for j in range(0, 5):
            print("*", end=" ")
        print()    
rec_stars(4, 5)            

# COMMAND ----------

def number_grid(n):
    for i in range(0, n):
        for j in range(0, n):
            print(j + 1, end=" ")
        print()

number_grid(3)            

# COMMAND ----------

def left_aligned_triangle(n):
    for i in range(1, n+1):
        j = 1
        while j <= i:
            print("*", end=" ")
            j += 1
        print()

left_aligned_triangle(5)            

# COMMAND ----------

def right_aligned_triangle(n):
    for i in range(1, n+1):
        j = 1
        k = 1
        while j <= n - i:
            print(" ", end=" ")
            j += 1

        while k <= i:
            print("*", end=" ")    
            k += 1

        print()

right_aligned_triangle(5)            

# COMMAND ----------

def multi_table_in_grid(n):
    for i in range(1, n + 1):
        for j in range(1, n + 1):
            print(i * j, end=" ")
        print()

multi_table_in_grid(10)            


# COMMAND ----------

def floyd_triangle(n):
    num = 1
    for i in range(1, n + 1):
        j = 1
        while j <= i:
            print(num, end=" ")
            j += 1
            num += 1
        print()

floyd_triangle(7)                


# COMMAND ----------

def floyd_triangle(n):
    num = 1
    for i in range(1, n + 1):             # Outer loop for rows
        for j in range(1, i + 1):         # Inner loop for columns
            print(num, end=" ")
            num += 1
        print()                           # New line after each row

floyd_triangle(5)


# COMMAND ----------

def reverse_list(n):
    temp=[]
    i = len(n)-1
    while i >= 0:
        temp.append(n[i])
        i -= 1
    return temp    

n = [10, 20, 30, 40]
print(reverse_list(n))

# COMMAND ----------

# Merge two lists l1 = ["my", "varun"] and l2 = ["name", "is"] to print: "my name is varun".
l1 = ["my", "varun"] 
l2 = ["name", "is"]

def merging(l1, l2):
    result = []
    # first word from l1
    result.append(l1[0])
    # then both words from l2
    result.extend(l2)
    # finally last word from l1
    result.append(l1[1])
    
    final_res = " ".join(result)
    return final_res

print(merging(l1, l2))

# COMMAND ----------

# Given two lists names = ["Alice", "Bob", "Charlie"] and scores = [85, 92, 78], iterate to print: "Alice scored 85", etc.

def sentence(names, scores):
    for i in range(len(names)):
        print(f"{names[i]} scored {scores[i]}") 

names = ["Alice", "Bob", "Charlie"] 
scores = [85, 92, 78]                

sentence(names, scores)

# COMMAND ----------

l1 = [1, 2, 3, 4]
l2 = [3, 4, 5, 6]

common = []
for a in l1:
    if a in l2:
        common.append(a)
print(common)


# COMMAND ----------

s = "varun"
rev = ""
for ch in s:
    rev = ch + rev
print(rev)


# COMMAND ----------

# Create a loop that counts how many vowels are in a string "interview".

def vowelsChecking(value):
    count = 0
    vowels = 'aeiou'
    for ch in value:
        if ch in vowels:
            count += 1
    return f"Vowels count is {count}"

print(vowelsChecking("varuntheja"))            

# COMMAND ----------

# Write nested loops to print a multiplication table (1–5)

def multiplicationTables(n, y):
    multi = []
    for i in range(1, n + 1):
        temp = []
        for j in range(1, y + 1):
            temp.append(i*j)
        multi.append(temp)

    return multi 

print(multiplicationTables(5, 10))            

# COMMAND ----------

# Write nested loops to print a multiplication table (1–5)

def multiplicationTables(n, y):
    for i in range(1, n + 1):
        for j in range(1, y + 1):
           print(f"{i} * {j} = {i*j}", end="\t")
        print("\n")   

multiplicationTables(5, 10)      

# COMMAND ----------

list1 = [10, 20, 30, 40, 50]
list2 = ['a', 'b', 'c']

def pairs(l1, l2):
    length = min(len(list1), len(list2))
    result = []
    for i in range(length):
        result.append((list1[i], list2[i]))

    return result    

print(pairs(l1, l2))        

# COMMAND ----------

numbers = [42, 7, 19, 3, 25, 11]

def sorting(n):
    for i in range(len(n)):
        for j in range(len(n)-i-1):
            if n[j] > n[j+1]:
                n[j], n[j+1] = n[j+1], n[j]
    return n 

print(sorting(numbers))                


# COMMAND ----------

"""
ATM Withdrawal System
Create a function withdraw(balance, amount).
Raise exceptions in these cases:
If amount is more than balance → raise InsufficientFundsError.
If amount is negative → raise InvalidAmountError.
Handle these exceptions gracefully and always print "Transaction finished." in finally.
"""

# COMMAND ----------



# COMMAND ----------

class InsufficientFundsError(Exception):
    def __init__(self, message, balance, amount):
        super().__init__(message)
        self.balance = balance
        self.amount = amount


class InvalidAmountError(Exception):
    def __init__(self, message, amount):
        super().__init__(message)
        self.amount = amount

class ATM:
    def withdraw(self, balance, amount):
        if amount > balance:
            raise InsufficientFundsError(f"Withdraw amount {amount} is not sufficient for the available balance {balance}", balance, amount)
        elif amount < 0:
            raise InvalidAmountError(f"Amount {amount} is invalid", amount)
        else:
            return balance - amount 

# Instantiating Cass
atm = ATM()

    try:
        new_balance = atm.withdraw(50000, 60000)   # This will raise InsufficientFundsError
        print(f"Withdrawal successful. Remaining balance: {new_balance}")
    except InsufficientFundsError as err:
        print(f"Error caught {err}")
        print(f"Balance: {err.balance}, Tried to withdraw: {err.amount}") 
    except InvalidAmountError as err2:
        print(f"Invalid amount entered: {err2.amount}")   
    finally:
        print("Transaction Finished")    

# COMMAND ----------

"""
File Processing with Fallback
Ask the user for a file name.
Try to open and read the file.
If FileNotFoundError occurs, create a new file with default content.
If PermissionError occurs, print "You don't have permission to read this file."
Ensure the program doesn’t crash in any case.
"""

# COMMAND ----------

class PermissionError(Exception):
    def __init__(self, message, file_name):
        super().__init__(message)
        self.file_name = file_name

class FileProcessing:
    names = ["Varun", "Suresh"]

    def fileReading(self):
        user = input("Enter the name")
        file_name = input("Enter the file name")

        if user not in self.names:
            raise PermissionError(f"You don't have permission to read this file {file_name}", file_name)
        else:
            with open(file_name, "r") as f:
                for line in f:
                    print(line.strip())
        return f"File read succesfully: {file_name}"

# Instantiating the class
file_inst = FileProcessing()

# Reading the file

try:
    result = file_inst.fileReading()
    print(result)
except FileNotFoundError as err:
    print(f"File {err.filename} not found. Creating it...")
    with open(err.filename, "w") as f:
        f.write("Hello, this is my new file!\n")
        f.write("I'm a Data Engineer.")
except PermissionError as err2:
    print(f"Permission error for file: {err2.file_name}")        
finally:
    print("File processing finished.")    

# COMMAND ----------

def wordFrequency(sentence):
    words = sentence.lower().split()
    print(words)
    frequency = {}
    for word in words:
        if word not in frequency:
            frequency[word] = 1
        else:
            frequency[word] += 1  
    return frequency    
            

sentence = "Data engineering makes big data processing faster and data analysis easier"
wordsFre = wordFrequency(sentence)

for word, count in wordsFre.items():
    print(f"{word} count is {count}")

# COMMAND ----------

def squareOfStars(n):
    for i in range(1, n+1):
        for j in range(1, n+1):
            print("*", end=" ")
        print()


squareOfStars(4)                

# COMMAND ----------

def RightTriangle(n):
    for i in range(1, n+1):
        for j in range(1, i+1):
            print(j, end=" ")
        print()


RightTriangle(5)            

# COMMAND ----------

def reverseRightTriangle(n):
    for i in range(n, 0, -1):
        for j in range(i, 0, -1):
            print(j, end=" ")
        print()

reverseRightTriangle(5)            

# COMMAND ----------

def sumOfNumbers(numbers_list):
    total_sum = 0
    for row in numbers_list:
        for num in row:
            total_sum = total_sum + num
    return total_sum

data = [[1, 2], [3, 4], [5, 6]]
result = sumOfNumbers(data)            
print(f"Total sum of numbers is {result}")

# COMMAND ----------

def pyramidOfStars(n):
    l = 1
    for i in range(n, 0, -1):
        j = 1
        k = 1
        while j <= i-1:
            print(" ", end=" ")
            j += 1

        while k <= 2 * l - 1:
            print("*", end=" ")
            k += 1

        l += 1
        print()    
pyramidOfStars(5)        


# COMMAND ----------

def pyramidOfStars(n):
    for i in range(1, n + 1):

        for j in range(n - i):
            print(" ", end=" ")

        for k in range(2 * i - 1):
            print("*", end=" ")

        print()       

pyramidOfStars(5)            

# COMMAND ----------

def diamondOfStars(n):
    l = 1
    for i in range(1, n + 1):

        for j in range(n - i):
            print(" ", end=" ")

        for k in range(2 * i - 1):
            print("*", end=" ")

        l += 1
        if l == 4:
            break    
        print()

    print()

    o = 2
    for m in range(2, 0, -1):
        g = 1 # g is reseting here

        for p in range(n - m):
            print(" ", end=" ")    

        while g <= 2 * o -1:
            print("*", end=" ")
            g += 1

        o -= 1    
        print()



diamondOfStars(5)                    


# COMMAND ----------

def diamondOfStars(n):
    mid = (n // 2) + 1  # middle row

    # Top half (including middle row)
    for i in range(1, mid + 1):
        # Print leading spaces
        for j in range(mid - i):
            print(" ", end=" ")
        # Print stars
        for k in range(2 * i - 1):
            print("*", end=" ")
        print()

    # Bottom half (excluding middle row)
    for i in range(mid - 1, 0, -1):
        # Print leading spaces
        for j in range(mid - i):
            print(" ", end=" ")
        # Print stars
        for k in range(2 * i - 1):
            print("*", end=" ")
        print()

# Example:
diamondOfStars(5)
print()
diamondOfStars(7)


# COMMAND ----------

def diamondOfStars(n):
    mid = (n // 2) + 1  # middle row
    
    for i in range(1, n + 1):
        # Number of spaces: distance from middle
        spaces = abs(mid - i)
        # Number of stars: 2*(mid - spaces) - 1
        stars = 2 * (mid - spaces) - 1

        # Print spaces
        for s in range(spaces):
            print(" ", end=" ")
        # Print stars
        for s in range(stars):
            print("*", end=" ")
        print()

# Example:
diamondOfStars(5)
print()
diamondOfStars(7)


# COMMAND ----------

def hallowSquare(n):
    for i in range(1, n + 1):
        if i == 2:
            break
        for j in range(1, n + 1):
            print("*", end=" ")

        print()    

    # print()

    for i in range(1, n-1):
        j = 1
        k = 1
        while j <= k:
            print("*", end=" ")
            j += 1

        m = n - 2
        p = 1
        while p <= m:
            print(" ", end=" ")
            p += 1

        o = 1
        l = 1
        while l <= o:
            print("*", end=" ")
            l += 1       

        print()

    for i in range(1, n + 1):
        if i == 2:
            break
        for j in range(1, n + 1):
            print("*", end=" ")

        print()        

hallowSquare(5)
print("--------------")

hallowSquare(7)
print("---------------")

hallowSquare(6)

# COMMAND ----------

def hallowsqr(n):
    for i in range(n):
        for j in range(n):
            # Print star for border positions
            if i == 0 or i == n-1 or j == 0 or j == n-1:
                print("*", end=" ")
            else:
                print(" ", end=" ")
            
        print()  # new line after each row

hallowsqr(5)      

hallowsqr(7)

# COMMAND ----------

def repeatingCharacter(word):
    count = {}
    char = None
    for ch in word:
        if ch not in count:
            count[ch] = 1
        else:
            count[ch] += 1      
    return count

result = repeatingCharacter("swiss")         
print(result)           

for key, value in result.items():
    if value == 1:
        char = key
        print(f"First non repeated character from {word} is {char}")
        break

# COMMAND ----------

def first_non_repeated_character(word):
    count = {}
    
    # Step 1: Count frequency of each character
    for ch in word:
        if ch not in count:
            count[ch] = 1
        else:
            count[ch] += 1      

    # Step 2: Find the first character with frequency == 1
    for ch in word:   # iterate again in original order
        if count[ch] == 1:
            return ch   # return immediately when found
    
    return None  # if no non-repeated character found


# Test the function
word = "swiss"
result = first_non_repeated_character(word)

if result:
    print(f"First non-repeated character from '{word}' is '{result}'")
else:
    print(f"No non-repeated character found in '{word}'")


# COMMAND ----------

def characterFrequency(word):
    char_freq = {}
    for char in word:
        if char not in char_freq:
            char_freq[char] = 1
        else:
            char_freq[char] += 1

    return char_freq

result = characterFrequency("banana")
for key, value in result.items():
    print(f"Character '{key}' frequency is {value}")

# COMMAND ----------

def wordFrequency(sentence):
    words = sentence.lower().split()
    print(words)
    frequency = {}
    for word in words:
        if word not in frequency:
            frequency[word] = 1
        else:
            frequency[word] += 1  
    return frequency    
            

sentence = "Data engineering makes big data processing faster and data analysis easier"
wordsFre = wordFrequency(sentence)

for word, count in wordsFre.items():
    print(f"{word} count is {count}")

# COMMAND ----------

def removingDuplicatesFromString(word):
    result = ""
    for ch in word:
        if ch not in result:
            result = result + ch 
    return result        

result = removingDuplicatesFromString("vvarun")    
print(result)

# COMMAND ----------

def removingDuplicatesFromList(nums):
    unique_numbers = []
    for num in nums:
        if num not in unique_numbers:
            unique_numbers.append(num)
    return unique_numbers  

nums = [1, 2, 3, 3, 1, 4, 5, 2]
unique_numbers = removingDuplicatesFromList(nums)          
print(f"Unique numbers from duplicated list of numbers {nums} are {unique_numbers}")

# COMMAND ----------

"""
Checking if num not in unique_numbers is O(n) → a bit slow when list gets large.

To improve performance, use a set to track seen numbers:
"""

def removingDuplicatesFromList(nums):
    unique_numbers = []
    seen = set()

    for num in nums:
        if num not in seen:
            unique_numbers.append(num)
            seen.add(num)

    return unique_numbers
"""
How membership checks work
1) List version (if x not in unique_list)

Python must scan the list from left to right and compare x to each element until it finds a match or reaches the end.
If your unique_list currently has k elements, this check costs O(k) comparisons.
As you process n input items, k grows from 0 to ~n.
Total work ≈ 1 + 2 + 3 + ... + n = O(n²).
Concrete example: input [1,2,3,1,2]
Check 1 in [] → 0 comparisons
Check 2 in [1] → up to 1 compare
Check 3 in [1,2] → up to 2 compares
Check 1 in [1,2,3] → finds at position 0 (best case here, but often later)
Check 2 in [1,2,3] → finds at position 1
Even with some early finds, across large data this grows roughly quadratically.

2) Set version (if x not in seen)
Python sets are hash tables.
To check membership:
Compute hash(x) (constant-time).
Jump directly to a calculated slot in the table using the hash.
If that slot is empty → not present.
If occupied by something with the same hash, compare equality (rare) and probe a few nearby slots if needed.
Average membership check is O(1) (constant time).
Doing this for n items gives O(n) total time.

Visual intuition
List: “Is x here?” → look at every element until you’re sure. Time grows with the list size each time → quadratic overall.
Set: “Is x here?” → jump to its address using the hash, usually confirm immediately. Time stays roughly constant per item → linear overall.

Why hash tables are fast (in practice)
Python keeps the set’s table sparse (resizes before it fills up), keeping collisions low.
For numbers and strings, hashing is very fast; equality checks are cheap.
Worst-case can be O(n) if many collisions happen, but Python’s design makes that rare in normal use.

Trade-offs and notes
Memory: The set version uses extra memory (you keep both a list for order and a set for membership).
Requirements: Items must be hashable (e.g., ints, strings, tuples). Mutable types like lists are not hashable.
Order: You still preserve order by appending to the list only the first time you see each item; the set is just for fast “seen?” checks.
"""

# COMMAND ----------

"""
Find the Largest Number
Write a function find_largest(numbers) that takes a list of numbers and returns the largest number. Use a loop and conditions.
"""
def largest_number(list_of_numbers):
    #assuming first value in list has large number
    large_num=list_of_numbers[0]
    #iterating through the list
    for i in list_of_numbers:
        if i > large_num:
            large_num = i
    return f"Largest number in list is {large_num}"        

numbers=[2,1,3,5,4,6,8,7,11,9]
largest_num_instance=largest_number(numbers)
print(largest_num_instance)

# COMMAND ----------

def minMaxSecondLargestInList(nums):
    # Step 1: Find minimum and maximum
    min_num = nums[0]
    max_num = nums[0]

    # Find min and max
    for num in nums:
        if num < min_num:
            min_num = num
        if num > max_num:
            max_num = num

    # Step 2: Remove the maximum from a copy of the list (to avoid modifying original list)
    nums_copy = nums.copy()
    nums_copy.remove(max_num)

    # Step 3: Find the second largest (which is now the max of the new list)
    second_largest = nums_copy[0]
    for num in nums_copy:
        if num > second_largest:
            second_largest = num

    return min_num, max_num, second_largest


nums = [2, 3, 4, 5, 1, 10]
min_num, max_num, second_largest = minMaxSecondLargestInList(nums)
print(f"Minimum is {min_num}, Maximum is {max_num}, and Second Largest is {second_largest}")


# COMMAND ----------

def minMaxSecondLargestInList(nums):
    min_num = nums[0]
    max_num = nums[0]
    second_lar_num = float('-inf')

    for num in nums:
        if num < min_num:
            min_num = num

        if num > max_num:
            second_lar_num = max_num   # old max becomes second max
            max_num = num
        elif num > second_lar_num and num != max_num:
            second_lar_num = num

    return min_num, max_num, second_lar_num

nums = [2, 3, 4, 5, 1, 10]
min_num, max_num, second_lar_num = minMaxSecondLargestInList(nums)    
print(f"Minimum number from list is {min_num} and maximum number is {max_num} and second largest number is {second_lar_num}")        

# COMMAND ----------

import csv

def employees_data():
    dept_count = {}
    high_sals = {}
    with open("emps.csv", "r")  as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["dept"] not in dept_count:
                dept_count[row["dept"]] = 1
            else:
                dept_count[row["dept"]] += 1

            if int(row["salary"]) > 60000:
                high_sals[row["name"]] = salary


    first_key = next(iter(high_sals))
    largest_key = first_key
    largest_value = data[first_key]          

    for key, value in high_sals.items():
        if value > largest_value:
            largest_key = key
            largest_value = value

    print(f"Highest salary employee is {largest_key} with salary of {largest_value}")

    return dept_count, high_sals        



# COMMAND ----------

from collections import defaultdict

def json_grouping(data):
    adult_customers = []
    for cust in data["customers"]:
        if cust["age"] >= 18:
            adult_customers.append(cust)

    # Group by country
    country_group = {}
    for cust in data["customers"]:
        country = cust["country"]
        if country not in country_group:
            country_group[country] = []
        country_group[country].append(cust["name"])

    # Print outputs
    print("Adults:", adult_customers)
    print("Group by country:", country_group)    

data = {
  "customers": [
    {"id": 1, "name": "Arjun", "age": 28, "country": "India"},
    {"id": 2, "name": "Teja", "age": 17, "country": "USA"},
    {"id": 3, "name": "Rahul", "age": 35, "country": "India"}
  ]
}
    

result = json_grouping(data)        
print(result)

# COMMAND ----------

import json

def metadata_config(config):
    for f in config["files"]:
        if f["type"] == "csv":
            print(f"Reading CSV file")

        elif f["type"] == "json":
            with open(f["path"], "r") as jf:
                data = json.load(jf)
                count = 0
                for name in data["names"]:
                    count += 1
                print(f"Number of customers inside file is {count}")    
                
        elif f["type"] == "text":
            line_count = 0
            with open(f["path"], "r") as tf:
                for line in tf:
                    line_count += 1
                print(f"Line count in file is {line_count}")    

config = {
    "files": [
        {"path": "sales.csv", "type": "csv"},
        {"path": "customers.json", "type": "json"},
        {"path": "products.txt", "type": "text"}
    ]
}

metadata_config(config)

# COMMAND ----------

def reading_txt(file_name):
    # Open output file once in write mode
    with open("transactions_clean.csv", "w") as outfile:

        # Extract → read input file
        with open(file_name, "r") as infile:
            for line in infile:

                cols = line.strip().split(",")

                cust_id = cols[0]
                amount = float(cols[1])
                currency = cols[2]

                # Transform → convert all to INR
                if currency == "USD":
                    amount = amount * 83
                    currency = "INR"
                elif currency == "EUR":
                    amount = amount * 90
                    currency = "INR"
                elif currency == "INR":
                    amount = amount * 1

                # Prepare a CSV row
                transformed_row = f"{cust_id},{int(amount)},{currency}\n"

                # Load → write to output file
                outfile.write(transformed_row)

    # Optional: Read back and print
    with open("transactions_clean.csv", "r") as f:
        print(f.read())


reading_txt("transactions.txt")


# COMMAND ----------

def combos(s):
    val = ""
    temp = []
    for i, ch in enumerate(s):
        val = ch
        for j, c in enumerate(s):
            if i < j:
                val = val + c
                temp.append(val)
    return temp


def  is_palindrome(value):
    c = value
    result = ""
    is_palindrome = False
    for i in range(len(value)-1, -1, -1):
        result = result + value[i]

    if result == c:
       is_palindrome = True

    return is_palindrome   


def longest_palindrome_check(s):
    temp = []
    longest_pal = ""
    for value in combos(s):
        result = is_palindrome(value)

        if result:
            temp.append(value)

    for palindrome in temp:
        if len(palindrome) > len(longest_pal):
            longest_pal = palindrome

    return longest_pal

result = longest_palindrome_check("babad")            
print(f"Longest palindrome substring in value is {result}")



# COMMAND ----------


def  is_palindrome(value):
    c = value
    result = ""

    for i in range(len(value)-1, -1, -1):
        result = result + value[i]

    if result == c:
       print("It is a palindrome")



# COMMAND ----------

# InvalidPriceException
import logging
# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("product.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class InvalidPriceException(Exception):
    def __init__(self, message, price, product_id):
        self.price = price
        self.product_id = product_id
        super().__init__(message)

products = [
 {"product_id":101, "price":200},
 {"product_id":102, "price":-30},
 {"product_id":103, "price":500}
] 


class products_validating():

    def validate_products(self, products):
        valid_products = []
        for product in products:
            try:
                if product["price"] < 0:
                    raise InvalidPriceException(f"Invalid amount for product {product['product_id']} : {product['price']}", product["price"], product["product_id"])
                else:
                    valid_products.append(product)
            except InvalidPriceException as err:
                logger.error(f"Invalid price detected for product {err.product_id} price {err.price}")
        return valid_products

# COMMAND ----------

class AmountNotValid(Exception):
    def __init__(self, message, amount, product_id):
        self.amount = amount
        self.product_id = product_id
        super().__init__(message)

data = [
 {"product_id":1,"amount":100},
 {"product_id":1,"amount":200},
 {"product_id":2,"amount":300},
 {"product_id":3,"amount":400},
 {"product_id":2,"amount":100}
]

class productSalesAggregations():
    def aggregate_product_sales(self, data):
        product_sales = {}

        for product in data:
            try:
                if product["amount"] < 0:
                    raise AmountNotValid(f"Amount is not valid {product['amount']}", product["amount"], product["product_id"])
                elif product["product_id"] not in product_sales:
                    product_sales[product["product_id"]] = product["amount"]
                else:
                    product_sales[product["product_id"]] += product["amount"]
            except AmountNotValid as err:
                print(f"For product {err.product_id} amount is not valid {err.amount}")       

        return product_sales            


# COMMAND ----------

import logging
import requests
import time

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("apiData.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class APIRetryFailedException(Exception):
    def __init__(self, message):
        super().__init__(message)


class FetchingApiData:

    def fetch_api_data(self, url):

        headers = {
            "Authorization": "Bearer <token>",
            "Content-Type": "application/json"
        }

        retries = 3
        i = 1

        while i <= retries:

            response = requests.get(url, headers=headers)
            status_code = response.status_code

            if status_code == 200:
                return response.json()

            elif status_code in [500, 502, 503, 504]:

                logger.error(f"Retry attempt {i} due to server error {status_code}")

                if i == retries:
                    raise APIRetryFailedException("API retry failed after 3 attempts")

                time.sleep(10)
                i += 1

            else:
                raise APIRetryFailedException(f"Unexpected status code {status_code}")


api = FetchingApiData()

try:
    data = api.fetch_api_data("https://example.com/api")
    print(data)

except APIRetryFailedException as err:
    logger.error(f"API completely failed: {err}")            

# COMMAND ----------

class SchemaMismatchException(Exception):
    def __init__(self, message, tablename):
        self.tablename = tablename 
        super().__init__(message)

class TableProcessing():

    def dataframeCreations(self, tables):
        result = {}
        for table in tables.keys():
            df = spark.read.table(table)
            result[table] = df

        return result
    
    def validate_schema(self, tablename, df, columns):

        actual_cols = df.columns
        validate = True

        for a, e in zip(columns, actual_cols):
            if a != e:
                validate = False
                break
        return validate

    def tableValidations(self, tables):

        result = self.dataframeCreations(tables)

        for tablename, df in result.items():

            columns = tables[tablename]

            try:
                is_valid  = self.validate_schema(tablename, df, columns)
             
                if not is_valid:
                    raise SchemaMismatchException(f"Schema is mismatched for table {tablename}", tablename)

                print(f"{tablename} schema validated")

            except SchemaMismatchException as err:
                print(f"Schema is not matched for table {err.tablename}")

tbl_inst = TableProcessing()
tables = {
 "customers":["customer_id","name","email"],
 "transactions":["transaction_id","customer_id","amount"],
 "products":["product_id","product_name","price"]
}

tbl_inst.tableValidations(tables)

    


# COMMAND ----------

import logging

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("DuplicateRecords.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class DuplicateDetection:

    def remove_duplicates(self, data):

        unique_records = []
        duplicate_records = []

        seen = set()

        for record in data:

            key = (record["id"], record["name"])

            if key not in seen:
                seen.add(key)
                unique_records.append(record)

            else:
                duplicate_records.append(record)
                logger.debug(f"{record} is a duplicate record")

        return unique_records, duplicate_records
    
dup_inst = DuplicateDetection()

data = [
 {"id":1,"name":"A"},
 {"id":2,"name":"B"},
 {"id":1,"name":"A"},
 {"id":3,"name":"C"}
]

unique_records, duplicate_records = dup_inst.remove_duplicates(data)


# COMMAND ----------

class FileProcessing:

    def process_files(self, files):
        for file in files:
            result = file.split("_")
            file_name = result[0]
            year = result[1]
            month = result[2].split(".")[0]
            form = result[2].split(".")[1]

            df = spark.read.format(form).load(f"/mnt/Bronze/{file}")

            df.write.format("parquet").save(f"/mnt/data/year={year}/month={month}/")

file_inst = FileProcessing()

files = [
 "transactions_2024_01.csv",
 "transactions_2024_02.csv",
 "transactions_2024_03.csv"
]

file_inst.process_files(files)


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import * 

class DataQualityFramework:

    def check_nulls(self, dataset, rules):
        null_validation_report = {
            dataset : "Validated"
        }

        df = spark.read.format("csv").option("header", True).load(f"/mnt/bronze/{dataset}")

        for col_name in rules.keys():
            if df.filter(col(col_name).isNull()).take(1):
                null_validation_report[dataset] = "NotValidated"
                break

        return dataset_validation_report        
    

    def check_negative_values(self, dataset, rules):
        negatives_validation_report = {
            dataset : "Validated"
        }

        df = spark.read.format("csv").option("header", True).load(f"/mnt/bronze/{dataset}")

        for col_name in rules.keys():
            if df.filter(col(col_name) < 0).take(1):
               negatives_validation_report[dataset] = "NotValidated"
               break

        return negatives_validation_report


    def check_duplicates(self, dataset, rules):     
        duplicates_validation_report = {
            dataset : "Validated"
        }

        df = spark.read.format("csv").option("header", True).load(f"/mnt/bronze/{dataset}")

        for col_name in rules.keys():
            duplicates_df = df.groupBy(col_name).agg(
                count("*").alias("duplicates_count")
            ).filter(col("duplicates_count") > 1)

            if duplicates_df.take(1):
               duplicates_validation_report[dataset] = "NotValidated" 
               break

        return duplicates_validation_report


validation_inst = DataQualityFramework()

null_rules = {
    "amount" : "not_null",
    "product_id" : "not_null"
}

null_valid_report = validation_inst.check_nulls(products, null_rules)
print(null_valid_report)

negatives_rules = {
    "amount" : "non_negative",
    "tax" : "non_negative"
}

negatives_valid_report = validation_inst.check_negative_values(products, negatives_rules)
print(negatives_valid_report)

duplicates_rules = {
    "product_id" : "not_duplicate"
}

duplicates_valid_report = validation_inst.check_duplicates(products, duplicates_rules)
print(duplictes_valid_report)

# COMMAND ----------


import logging
import requests

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("DataIngestion.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class DataIngestionEngine:

    def retry(max_retries, delay):
        def decorator(func):
            def wrapper(*args, **kwargs):
                attempt = 0
                while attempt < max_retries:
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        attempt += 1
                        if attempt == max_retries:
                            print(f"Function failed after {attempt} retries")

                        print(f"Retrying {func.__name__}") 
                        time.sleep(delay) 
            return wrapper              
        return decorator
                
    @retry(max_retries = 5, delay = 10) 
    def apiCall(self, endpoint, target):
        url = f"https://dummy.{endpoint}"

        response = requests.get(url)
        status = response.status_code

        if status != 200:
            raise Exception(f"API failed with status {response.status_code}")
            
        data = response.json()
        df = spark.createDataFrame(data)

        df.write.mode("append").saveAsTable(target)

        logger.info(f"API data ingested into {target}")
        return True

    def fileReading(self, path, target):
        df = spark.read.format("csv").load(path)

        df.write.mode("append").saveAsTable(target)

        logger.info(f"File data ingested into {target}")
        return True
        
    def dataProcessing(self, config):
    result = {}

    for item in config:
        target = item["target"]

        try:
            if item["source"] == "api":
                self.apiCall(item["endpoint"], target)

            elif item["source"] == "file":
                self.fileReading(item["path"], target)

            df = spark.read.table(target)

            if not df.isEmpty():
                logger.info(f"Data ingested into {target} successfully")
                result[target] = "Success"
            else:
                logger.warning(f"No data found in {target}")
                result[target] = "Empty"

        except Exception as e:
            logger.error(f"Ingestion failed for {target}: {str(e)}")
            result[target] = "Failed"

    return result      


# COMMAND ----------

import logging
import requests
import json
from datetime import datetime

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("ErrorsInformation.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def log_error(errortype, name, message, timestamp):
    log_data = {
    "error_type": errortype,
    "function": name,
    "message": message,
    "timestamp": str(timestamp)
    }

    logger.info(json.dumps(log_data))


def handleErrors(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)

            # ✅ Handle API error based on result
            if isinstance(result, dict) and result.get("status") >= 400:
                log_error("APIError", func.__name__, result.get("message"), datetime.now())

            return result

        except (KeyError, ValueError) as e:
            log_error("ValidationError", func.__name__, str(e), datetime.now())

        except (ZeroDivisionError, FileNotFoundError, PermissionError) as e:
            log_error("SystemError", func.__name__, str(e), datetime.now())

        except requests.exceptions.RequestException as e:
            log_error("APIError", func.__name__, str(e), datetime.now())

        except Exception as e:
            log_error("UnknownError", func.__name__, str(e), datetime.now())

    return wrapper



class DataProcessing:

    @handleErrors
    def dataReading(self, data):
        return data["salary"]

    @handleErrors
    def inputData(self, a):
        data = int(input("Enter the value"))    
        return data + a
    
    @handleErrors
    def division(self,input1, input2):
        return input1 / input2
    
    @handleErrors
    def fileReading(self, path):
        return spark.read.format("csv").load(path)
    
    @handleErrors
    def apiCall(self, endpoint):
        url = f"https://dummy.{endpoint}"

        response = requests.get(url)

        return {
            "status": response.status_code,
            "message": response.text
        }
    


# COMMAND ----------

class ValidationEngine:

    def dynamicCondition(self, rules):
        cond = None
        for key in rules.keys():
            if key == "non_negative":
                for col_name in rules[key]:
                    cond = col(col_name) >= 0 if cond is None else cond & (col(col_name) >= 0)
            elif key == "not_null":
                for col_name in rules[key]:
                    cond = col(col_name).isNotNull() if cond is None else cond & (col(col_name).isNotNull())
        return cond
    

    def dataValidation(self, path, rules):
        transactions_df = spark.read.format("csv").option("header", True).load(path)

        validation_report = {
            "total_records" : transactions_df.count()
        }

        condition = self.dynamicCondition(rules)

        valid_df = transactions_df.filter(condition)

        invalid_df = transactions_df.filter(~condition)

        validation_report["valid_records"] = valid_df.count()

        validation_report["invalid_records"] = validation_report["total_records"] - validation_report["valid_records"]

        return valid_df, invalid_df, validation_report
    

data_inst = ValidationEngine()

rules = {
            "non_negative":["amount"],
            "not_null":["email", "phonenumber", "address"]
        }
validation_report = data_inst.dataValidation("/mnt/raw/customers.csv", rules)
print(validation_report)

# COMMAND ----------

import requests
import time
import logging

# logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler("JobExecutions.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class PipelineExecutor:

    def __init__(self):
        self.completed_jobs = set()

    def triggerJob(self, job):
        result = None
        url = "https://<databricks-instance>/api/2.1/jobs/run-now"

        token = dbutils.secrets.get(scope = "credentials", key = "token")

        headers = {
            "Authorization" : "Bearer <token>",
            "Content-Type" : "application.json"
        }

        payload = {
            "job_id" = job
        }

        response = requests.post(url, headers = headers, josn = payload)
        run_id = response.json()["run_id"]

        status_url = "https://<databricks-instance>/api/2.1/jobs/runs/get"

        params = {
            "run_id" : run_id
        }

        while True:
            response = requests.get(status_url, headers = headers, params = params)

            data = response.json()

            if data["life_cycle_state"] == "TERMINATED" and data["result_state"] == "SUCCESS":
                print(f"Job completed succesfully {job}")
                result = "Succeed"
                break
            elif life_cycle_state == "INTERNAL_ERROR":
                result = "Failed"
                print(f"Job failed due to internal error ❌{job}")
                break
            else:
                print(f"Job failed {job}")    

            time.sleep(10)

        return result


    def jobDependencies(self, jobs):
        
        for parent, deps in jobs.items():
            # Step 1: Run dependencies first
            for dep in deps:
                if dep not in self.completed_jobs:
                    result = self.triggerJob(job)

                    if result == "Failed":
                        print(f"Dependency failed: {dep}")
                        return "Failed"
                    
                    self.completed_jobs.add(dep)

            if int(parent) not in self.completed_jobs:
                result = self.triggerJob(int(parent))

                if result == "Failed":
                    print(f"Parent failed: {parent}")
                    return "Failed"
                
                self.completed_jobs.add(int(parent))

        return "Succeed"        

pipeline_inst = PipelineExecutor()

jobs = {
 "12345":[],
 "34567":[12345],
 "56787":[34567]
}

pipeline_status = pipeline_inst.jobDependencies(jobs)

print(pipeline_status)



# COMMAND ----------


