from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, monotonically_increasing_id, col, lit


spark = SparkSession.builder.appName("Compare Dataframes").master("local").getOrCreate()

data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df1 = spark.createDataFrame(data, schema=["Name", "Age"])

data = [("Alice", 28), ("Bob", 25), ("Charlie", 30), ("Alice", 22)]
df2 = spark.createDataFrame(data, schema=["Name", "Age"])

data = [("Alice", 28, 2000), ("Bob", 25, 3000), ("Charlie", 30, 4000)]
df3 = spark.createDataFrame(data, schema=["Name", "Age", "Salary"])

data = [("Alice", 28), ("Bobb", 25), ("Charlie", 30)]
df4 = spark.createDataFrame(data, schema=["Name", "Age"])

# 1. Compare Schemas
if df1.schema == df3.schema:
    print("Schemas are identical")
else:
    print("Schemas differ")
    print("DF1 Schema:", df1.schema)
    # DF1 Schema: StructType([StructField('Name', StringType(), True), StructField('Age', LongType(), True)])
    print("DF3 Schema:", df3.schema)
    # DF3 Schema: StructType([StructField('Name', StringType(), True), StructField('Age', LongType(), True), StructField('Salary', LongType(), True)])

    print("DF1 Columns:", df1.columns)
    # DF1 Columns: ['Name', 'Age']
    print("DF3 Columns:", df3.columns)
    # DF3 Columns: ['Name', 'Age', 'Salary']
