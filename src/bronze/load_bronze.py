import argparse 
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, to_timestamp, current_timestamp)   

parser = argparse.ArgumentParser()
parser.add_argument("--storage_account", required=True)
parser.add_argument("--container", required=True)
parser.add_argument("--storage_key", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

storage_account = args.storage_account
source_container = args.container
target_container = 'service-desk'
storage_key = args.storage_key
catalog = "service_desk"


spark.sql (f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql (f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql (f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

storage_key = args.storage_key

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

file = {
    "customers": "Customers.csv",
    "departments": "Departments.csv",
    "employees": "Employees.csv",
    "Services": "Services.csv",
    "tickets_raw": "tickets_raw.csv"
}

for table_name, file_name in file.items():
   input_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/{file_name}"
   output_path = f"abfss://{target_container}@{storage_account}.dfs.core.windows.net/bronze/{table_name}/"

   df = (
      spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(input_path)
      .withColumn("_ingested_at", current_timestamp())
   )

   df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("path", output_path) \
      .saveAsTable(f"{catalog}.bronze.{table_name}")

   print(f"Loaded {file_name} into {catalog}.bronze.{table_name}")
     
    