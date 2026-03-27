# Provide a solution to check the input df before any processing and compare the schema with the schema
# (column name and types) from the json file, check particular columns for null values, etc

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("Countries total amount").master("local").getOrCreate()


"""1. Create a test schema"""

# In case if we have the schema in the JSON file

# with open(schema_path) as json_file:
#         pyth_obj = StructType.fromJson(json.load(json_file))

# To test our solution we will use json string
json_string = """[
  {"name": "id", "type": "long"},
  {"name": "name", "type": "string"},
  {"name": "age", "type": "long"}
]"""

pyth_obj = json.loads(json_string)
json_schema = StructType.fromJson({"fields": pyth_obj})
test_df = spark.createDataFrame([], json_schema)


"""2. Create the input df"""

data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df1 = spark.createDataFrame(data, schema=["name", "age"])

data = [("Alice", 28, 111), ("Bob", 25, 222), ("Charlie", 30, 333)]
df2 = spark.createDataFrame(data, schema=["name", "age", "id"])

def compare_schemas(df, df_expected_schema):
    df_fields = {(f.name, f.dataType) for f in df.schema.fields}
    expected_fields = {(f.name, f.dataType) for f in df_expected_schema.schema.fields}
    if df_fields == expected_fields:
        print("Schema is correct")
    else:
        print("Schema mismatch between input DataFrame and expected schema.")

compare_schemas(df1, test_df)
# Schema mismatch between input DataFrame and expected schema.
compare_schemas(df2, test_df)
# Schema is correct
