# Flattening a dataset with nested JSON structures into a tabular format using PySpark.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import json

spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()

# 1. Load the Dataset:
# Read the JSON data into a PySpark DataFrame using Spark's built-in JSON reader:
# df = spark.read.json("data.json", multiLine=True)

# OR emulate json
json_string = """
{
  "id": 1,
  "name": "Alice",
  "address": {
    "city": "New York",
    "zip": "10001"
  },
  "orders": [
    {"order_id": 101, "amount": 250},
    {"order_id": 102, "amount": 450}
  ]
}
"""
print([json.loads(json_string)])
# [{'id': 1, 'name': 'Alice', 'address': {'city': 'New York', 'zip': '10001'},
#   'orders': [{'order_id': 101, 'amount': 250}, {'order_id': 102, 'amount': 450}]}]
print(len([json.loads(json_string)]))
# 1

df = spark.createDataFrame([json.loads(json_string)])
df.show()
# +--------------------+---+-----+--------------------+
# |             address| id| name|              orders|
# +--------------------+---+-----+--------------------+
# |{zip -> 10001, ci...|  1|Alice|[{order_id -> 101...|
# +--------------------+---+-----+--------------------+

# 2. Inspect the Nested Structure:
# Print the schema to understand the hierarchical structure:
df.printSchema()
# root
#  |-- address: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- orders: array (nullable = true)
#  |    |-- element: map (containsNull = true)
#  |    |    |-- key: string
#  |    |    |-- value: long (valueContainsNull = true)

# 3. Flatten the Nested Columns:
# Use pyspark.sql.functions.col() and the dot notation to access nested fields directly.
# For nested structures like {"key": {"subkey": "value"}}, extract them into individual columns:

flattened_df = df.select(
    col("id"),
    col("address.city").alias("address_city"),
    col("address.zip").alias("address_zip"),
    col("name"),
    col("orders")
)
flattened_df.show()
# +---+------------+-----------+-----+--------------------+
# | id|address_city|address_zip| name|              orders|
# +---+------------+-----------+-----+--------------------+
# |  1|    New York|      10001|Alice|[{order_id -> 101...|
# +---+------------+-----------+-----+--------------------+

# For arrays, you can use explode() to expand repeated values:
exploded_df = flattened_df.withColumn("orders_exploded", explode(col("orders")))
exploded_df.show()
# +---+------------+-----------+-----+--------------------+--------------------+
# | id|address_city|address_zip| name|              orders|     orders_exploded|
# +---+------------+-----------+-----+--------------------+--------------------+
# |  1|    New York|      10001|Alice|[{order_id -> 101...|{order_id -> 101,...|
# |  1|    New York|      10001|Alice|[{order_id -> 101...|{order_id -> 102,...|
# +---+------------+-----------+-----+--------------------+--------------------+

# 4. Iterative Flattening:
# If there are multiple levels of nesting, repeat the process recursively. You can inspect the schema after each step
# to identify columns that still need flattening:
exploded_df.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- address_city: string (nullable = true)
#  |-- address_zip: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- orders: array (nullable = true)
#  |    |-- element: map (containsNull = true)
#  |    |    |-- key: string
#  |    |    |-- value: long (valueContainsNull = true)
#  |-- orders_exploded: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: long (valueContainsNull = true)

result = exploded_df.select(
    col("id"),
    col("address_city"),
    col("address_zip"),
    col("name"),
    col("orders_exploded.order_id").alias("orders_orderId"),
    col("orders_exploded.amount").alias("orders_amount")
)
result.show()
# +---+------------+-----------+-----+--------------+-------------+
# | id|address_city|address_zip| name|orders_orderId|orders_amount|
# +---+------------+-----------+-----+--------------+-------------+
# |  1|    New York|      10001|Alice|           101|          250|
# |  1|    New York|      10001|Alice|           102|          450|
# +---+------------+-----------+-----+--------------+-------------+

# # 5. Save the Tabular Data:
# # Write the resulting DataFrame to a tabular format such as CSV or Parquet:
# flattened_df.write.csv("output_tabular_data.csv", header=True)
