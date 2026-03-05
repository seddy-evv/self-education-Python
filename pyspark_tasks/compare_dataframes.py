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

# 2. Compare Data
# 2.1 Row Count
if df1.count() == df2.count():
    print("Row counts are identical")
else:
    print("Row counts differ")

# 2.2 Data Differences
# Rows in df1 but not in df2
diff1 = df1.exceptAll(df2)
# Rows in df2 but not in df1
diff2 = df2.exceptAll(df1)

if diff1.count() == 0 and diff2.count() == 0:
    print("DataFrames are identical")
else:
    print("Differences found")
    diff1.show()
    # +------+-----+
    # | Name | Age |
    # +------+-----+
    # +------+-----+
    diff2.show()
    # +------+-----+
    # | Name | Age |
    # +------+-----+
    # | Alice | 22 |
    # +-------+----+

# 2.3 For Large DataFrames
# For very large DataFrames, avoid .collect() or .show() with large numbers. Instead, you can check counts or sample
# a few differences:
print("Rows in df1 not in df2:", diff1.count())
# Rows in df1 not in df2: 0
print("Rows in df2 not in df1:", diff2.count())
# Rows in df2 not in df1: 1

# 3. Optional: Hash-Based Comparison
# For a quick check, you can compare hashes:

def df_hash(df):
    return df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))

df1_hash = df_hash(df1)
df2_hash = df_hash(df2)

hash_diff1 = df1_hash.select("row_hash").exceptAll(df2_hash.select("row_hash"))
hash_diff2 = df2_hash.select("row_hash").exceptAll(df1_hash.select("row_hash"))

if hash_diff1.count() == 0 and hash_diff2.count() == 0:
    print("DataFrames are identical by hash")
else:
    print("DataFrames differ by hash")
