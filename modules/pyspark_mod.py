# Apache PySpark is a powerful library for distributed computing using Python, built on top of Apache Spark.
# It allows handling large-scale data across multiple nodes and provides a rich set of functions for data processing.
# Below is a description and examples of some of the main PySpark functions:

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, sum, max, concat, lit, expr, create_map, to_date, to_timestamp, \
    concat_ws, coalesce, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, avg, min, udf, \
    current_date, floor, rand, count, array, explode, count_distinct, broadcast, desc, date_format, substring_index, \
    regexp_replace, upper, length, substring, trim, instr, split, array_contains, arrays_overlap, arrays_zip, element_at, \
    transform, posexplode, array_union, collect_list, struct, round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType, LongType
from pyspark.sql.window import Window
import pandas as pd
import pyspark.pandas as ps
import time

# Create SparkSession
spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()

def get_pyspark_df():
    data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
    df = spark.createDataFrame(data, schema=["Name", "Age"])

    return df

def get_pyspark_df1():
    data = [("Alice", 28, "Amazon"), ("Bob", 25, "Google"), ("Charlie", 30, "Oracle")]
    df1 = spark.createDataFrame(data, schema=["Name", "Age", "Company"])

    return df1

def get_pyspark_df2():
    data = [("Alex", 38, "Microsoft"), ("John", 35, "Netflix")]
    df2 = spark.createDataFrame(data, schema=["Name", "Age", "Company"])

    return df2

def get_pyspark_df3():
    data = [("Alice", 2000, 2.4), ("Bob", 3000, 2.5), ("Charlie", 4000, 3.5), ("Bob", 4000, 4.5)]
    df3 = spark.createDataFrame(data, schema=["Name", "Salary", "Percent"])

    return df3

def get_pyspark_df_cities():
    data = [("Alice", "New York"), ("Bob", "San Francisco")]
    df_cities = spark.createDataFrame(data, schema=["Name", "City"])

    return df_cities

def get_pyspark_df_null():
    data = [(None, None), ("1", None), (None, 2), ("3", 3)]
    df_null = spark.createDataFrame(data, ("a", "b"))

    return df_null

def get_pyspark_df_join1():
    data1 = [("Alice", 28), ("Bob", 25)]
    df_join1 = spark.createDataFrame(data1, ["Name", "Age"])

    return df_join1

def get_pyspark_df_join2():
    data2 = [("Alice", "F"), ("Bob", "M"), ("Charlie", "M")]
    df_join2 = spark.createDataFrame(data2, ["Name", "Gender"])

    return df_join2

def get_pyspark_df_join3():
    data3 = [("Mike", "M"), ("Bob", "M")]
    df_join3 = spark.createDataFrame(data3, ["Name", "Gender"])

    return df_join3

def get_pyspark_df_date():
    data = [("Alice", 28, '1997-02-28 10:30:00'), ("Bob", 25, '2000-02-28 10:30:00'),
            ("Charlie", 30, '2005-02-28 10:30:00')]
    df_date = spark.createDataFrame(data, schema=["Name", "Age", "Date"])

    return df_date

def get_pyspark_df_window():
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Maria", "Finance", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3300),
            ("Saif", "Sales", 4100)
            ]
    schema = ["employee_name", "department", "salary"]
    df_window = spark.createDataFrame(data=data, schema=schema)

    return df_window

def get_array_df():
    data = [
        (1, [1, 2, 3, 4], [3, 4, 5], ["a", "b", "c"]),
        (2, [5, 6, 7], [7, 8], ["x", "y"]),
        (3, [], [2, 3], [])
    ]
    schema = ["id", "numbers", "numbers1", "letters"]
    df_array = spark.createDataFrame(data, schema)
    return df_array

def get_array_split():
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Maria", "Finance", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3300),
            ("Saif", "Sales", 4100),
            ("Alex", "Sales", 4000),
            ("Bob", "Finance", 3900),
            ("Michael", "Sales", 4900),
            ("Mike", "HR", 1900)
            ]
    schema = ["employee_name", "department", "salary"]
    df_split = spark.createDataFrame(data=data, schema=schema)

    return df_split

# PySpark RDD Operations

# Create RDD
# Create SparkContext
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("PySpark Examples").setMaster("local")
sc = SparkContext(conf=conf)
# from a text file
rdd = sc.textFile("/path/textfile.txt")
# from a CSV file
rdd = sc.textFile("/path/csvfile.csv")
# from a JSON file
import json
rddFromJson = sc.textFile("/path/to/your/jsonfile.json").map(json.loads)
# from an HDFS file
rddFromHdfs = sc.textFile("hdfs://localhost:9000/path/to/your/file")
# from a Sequence file
rddFromSequenceFile = sc.sequenceFile("/path/to/your/sequencefile")

# Create an RDD from a Python list
# SparkContext.parallelize(Iterable, numSlices)
# numSlices - (optional) the number of partitions of the new RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# NARROW TRANSFORMATIONS
# Map Function
mapped_rdd = rdd.map(lambda x: x * 2)
print(mapped_rdd.collect())  # [2, 4, 6, 8, 10]
# flatMap Function
rdd1 = spark.sparkContext.parallelize([2, 3, 4])
flat_mapped_rdd = rdd1.flatMap(lambda x: range(x, 6))
print(flat_mapped_rdd.collect())  # [2, 3, 4, 5, 3, 4, 5, 4, 5]
# Filter Function
filtered_rdd = mapped_rdd.filter(lambda x: x > 6)
print(filtered_rdd.collect())  # [8, 10]
# Reduce Function
sum_result = rdd.reduce(lambda x, y: x + y)
print(sum_result)  # 15
# Union Function
rdd2 = spark.sparkContext.parallelize([1, 2, 3])
rdd3 = spark.sparkContext.parallelize([4, 5, 6])
union_rdd = rdd2.union(rdd3)
print(union_rdd.collect())  # [1, 2, 3, 4, 5, 6]
# Distinct Function
rdd4 = spark.sparkContext.parallelize([1, 1, 2, 2, 3, 3])
distinct_rdd = rdd4.distinct()
print(distinct_rdd.collect())  # [1, 2, 3]
# mapPartitions Function
def process_partition(iterator):
    yield sum(iterator)
rdd5 = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)
result_rdd = rdd5.mapPartitions(process_partition)
print(result_rdd.collect())  # [3, 12]

# WIDE TRANSFORMATIONS
# groupByKey Function
rdd6 = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 1)])
grouped_rdd = rdd6.groupByKey()
print(grouped_rdd.collect())
# reduceBy Function
rdd7 = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 1)])
reduced_rdd = rdd7.reduceByKey(lambda a, b: a + b)
print(reduced_rdd.collect())  # [("a", 2), ("b", 1)]
# aggregateByKey Function
seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd8 = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 2)], 2)
agg_rdd = rdd8.aggregateByKey((0, 0), seqOp, combOp)
print(agg_rdd.collect())  # [("a", (3, 2)), ("b", (1, 1))]
# sortBy Function
rdd9 = spark.sparkContext.parallelize([("a", 3), ("b", 1), ("a", 2)])
sorted_rdd = rdd9.sortBy(lambda x: x[1])
print(sorted_rdd.collect())  # [("b", 1), ("a", 2), ("a", 3)]
# join Function
rdd10 = spark.sparkContext.parallelize([("a", 1), ("b", 4)])
rdd11 = spark.sparkContext.parallelize([("a", 2), ("a", 3)])
join_rdd = rdd10.join(rdd11)
print(join_rdd.collect())  # [("a", (1, 2)), ("a", (1, 3))]


# Create DataFrame

# Create DataFrame from Python List of Tuples
data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df = spark.createDataFrame(data, schema=["Name", "Age"])
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# Create DataFrame from Python List of dicts
data = [
  {"Name": "Alice", "Age": 28},
  {"Name": "Bob", "Age": 25},
  {"Name": "Charlie", "Age": 30}
  ]
df = spark.createDataFrame(data)
df.show()
# +---+-------+
# |Age|   Name|
# +---+-------+
# | 28|  Alice|
# | 25|    Bob|
# | 30|Charlie|
# +---+-------+

# Create DataFrame from schema with types
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])
data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df = spark.createDataFrame(data, schema=schema)
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# Additional methods to create DataFrame
# from a Pandas DataFrame
pandas_df = pd.DataFrame({"Name": ["Alice", "Bob", "Charlie"], "Age": [28, 25, 30]})
df = spark.createDataFrame(pandas_df)
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# from an RDD
rdd = spark.sparkContext.parallelize([("Alice", 28), ("Bob", 25), ("Charlie", 30)])
df = rdd.toDF(["Name", "Age"])
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# from a list of Row objects
data = [Row(Name="Alice", Age=28), Row(Name="Bob", Age=25), Row(Name="Charlie", Age=30)]
df = spark.createDataFrame(data)
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# from a range
df = spark.range(3).toDF("num")
df.show()
# +---+
# |num|
# +---+
# |  0|
# |  1|
# |  2|
# +---+


# Basic PySpark DataFrame Operations
# Narrow Transformations: filter(), map(), flatMap(), withColumn(), select(), drop(), limit(), coalesce()
# Wide Transformations: groupBy(), groupByKey(), join(), distinct(), union(), repartition(),
# reduceByKey() (RDD, shuffle less data unlike groupByKey()), orderBy() / sort(), aggregateByKey()

df = get_pyspark_df()
# Show: Display the DataFrame
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# Collect: Retrieve all data
data = df.collect()
print(data)
# [Row(Name='Alice', Age=28), Row(Name='Bob', Age=25), Row(Name='Charlie', Age=30)]

# Count: Count the number of rows
count_res = df.count()
print(count_res)
# 3

# describe() - Computes basic statistics for numeric and string columns.
df.describe().show()
# +-------+-------+------------------+
# |summary|   Name|               Age|
# +-------+-------+------------------+
# |  count|      3|                 3|
# |   mean|   null|27.666666666666668|
# | stddev|   null|2.5166114784235836|
# |    min|  Alice|                25|
# |    max|Charlie|                30|
# +-------+-------+------------------+

# summary() - Computes specified statistics for numeric and string columns.
df.summary().show()
# +-------+-------+------------------+
# |summary|   Name|               Age|
# +-------+-------+------------------+
# |  count|      3|                 3|
# |   mean|   null|27.666666666666668|
# | stddev|   null|2.5166114784235836|
# |    min|  Alice|                25|
# |    25%|   null|                25|
# |    50%|   null|                28|
# |    75%|   null|                30|
# |    max|Charlie|                30|
# +-------+-------+------------------+

print(df.columns)
# ['Name', 'Age']

# we can use df.schema to create a new df from existing one with the same schema
print(df.schema)
# StructType([StructField('Name', StringType(), True), StructField('Age', LongType(), True)])

# select numerical columns
numeric_columns = [col for col in df.columns if df.schema[col].dataType in [IntegerType(), DoubleType(), FloatType(),
                                                                            LongType()]]

# select all columns except some
df1 = get_pyspark_df1()
columns_to_select = [col for col in df1.columns if col not in ["Company"]]
df_selected = df.select(columns_to_select)
df_selected.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

print(numeric_columns)
# ['Age']

df.printSchema()
# root
#  |-- Name: string (nullable = true)
#  |-- Age: long (nullable = true)

# if we need to get a schema above as a string
schema_string = df._jdf.schema().treeString()
print(schema_string)
# root
#  |-- Name: string (nullable = true)
#  |-- Age: long (nullable = true)

# isEmpty() - checks if the DataFrame is empty and returns a boolean value.
df.isEmpty()
# False

# Select Columns
# We can specify the colum name in two ways (strings are case-insensitive e.g. namE)
df.select("Name").show()
# or
df.select(df.Name).show()
# or
df.select(df["Name"]).show()
# or
df.select(col("Name")).show()
# +-------+
# |   Name|
# +-------+
# |  Alice|
# |    Bob|
# |Charlie|
# +-------+

# selectExpr - This is a variant of select() that accepts SQL expressions, but we don't need to create temp view.
df_expr = df.selectExpr("Age * 2")
df_expr.show()
# +---------+
# |(Age * 2)|
# +---------+
# |       56|
# |       50|
# |       60|
# +---------+

# select distinct
df.select("Name").distinct().show()
# +-------+
# |   Name|
# +-------+
# |  Alice|
# |    Bob|
# |Charlie|
# +-------+

# Filter Rows, & - and, | - or, ~ - not, don't forget brackets!
# where() is an alias for filter()
# also we can use a SQL expression string: df.filter("Age > 26")
df.filter((df.Age > 26) & (df.Age != 30)).show()
# +-----+---+
# | Name|Age|
# +-----+---+
# |Alice| 28|
# +-----+---+

# due to lazy evaluation this also will work
df.select(df.Name).filter(df.Age > 26).show()
# +-------+
# |   Name|
# +-------+
# |  Alice|
# |Charlie|
# +-------+

# filter column data using a list of values
df.filter(col("Age").isin(25, 26)).show()
# this code provides the same result:
# df.filter(df["Age"].isin(25, 26)).show()
# +----+---+
# |Name|Age|
# +----+---+
# | Bob| 25|
# +----+---+

# Order rows
df.orderBy(df["Age"].desc()).show()
# or
df.orderBy(df["Age"], ascending=False).show()
# we also can use [col("col_name1"), col("col_name2")] instead of just col("col_name")
df.orderBy(col("Age"), ascending=False).show()
# or
df.orderBy(desc("Age")).show()
# or
df.sort("Age", ascending=False).show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |Charlie| 30|
# |  Alice| 28|
# |    Bob| 25|
# +-------+---+

# If we need to get a column value from the one row, we can use .Column
Name = df.sort("Age", ascending=False).first().Name
print(Name)
# Charlie

# Add New Column
df.withColumn("AgePlusOne", col("Age") + lit(1)).show()
# similar to:
df.select("*", (col("Age") + lit(1)).alias("AgePlusOne")).show()
# +-------+---+----------+
# |   Name|Age|AgePlusOne|
# +-------+---+----------+
# |  Alice| 28|        29|
# |    Bob| 25|        26|
# |Charlie| 30|        31|
# +-------+---+----------+

# Replace Column Name
df.withColumnRenamed("Name", "New Name").show()
# +--------+---+
# |New Name|Age|
# +--------+---+
# |   Alice| 28|
# |     Bob| 25|
# | Charlie| 30|
# +--------+---+

# GroupBy and Aggregate
df.groupBy("Age").count().show()
# +---+-----+
# |Age|count|
# +---+-----+
# | 30|    1|
# | 25|    1|
# | 28|    1|
# +---+-----+

df3 = get_pyspark_df3()
df3 = df3.withColumn("Percent", round(col("Percent")))
df3.show()
# +-------+------+-------+
# |   Name|Salary|Percent|
# +-------+------+-------+
# |  Alice|  2000|    2.0|
# |    Bob|  3000|    3.0|
# |Charlie|  4000|    4.0|
# |    Bob|  4000|    5.0|
# +-------+------+-------+
aggregated_df = df3.groupBy("Name").agg(sum("Salary").alias("Salary sum"), max("Salary"), round("Percent"))
aggregated_df.show()
# +-------+----------+-----------+
# |   Name|Salary sum|max(Salary)|
# +-------+----------+-----------+
# |  Alice|      2000|       2000|
# |    Bob|      7000|       4000|
# |Charlie|      4000|       4000|
# +-------+----------+-----------+

df3.agg(max("Salary"), sum("Salary"), avg("Salary")).show()
# +-----------+-----------+-----------+
# |max(Salary)|sum(Salary)|avg(Salary)|
# +-----------+-----------+-----------+
# |       4000|      13000|     3250.0|
# +-----------+-----------+-----------+

df_wndw = get_pyspark_df_window()
df_wndw.show()
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |        James|     Sales|  3000|
# |      Michael|     Sales|  4600|
# |        Maria|   Finance|  3000|
# |        Scott|   Finance|  3300|
# |          Jen|   Finance|  3300|
# |         Saif|     Sales|  4100|
# +-------------+----------+------+

df_wndw.groupBy(df_wndw.department).agg(count_distinct(df_wndw.employee_name)).show()
# +----------+--------------------+
# |department|count(employee_name)|
# +----------+--------------------+
# |     Sales|                   3|
# |   Finance|                   3|
# +----------+--------------------+

df_wndw.groupBy("employee_name").pivot("department").sum("salary").show()
# +-------------+-------+-----+
# |employee_name|Finance|Sales|
# +-------------+-------+-----+
# |        Scott|   3300| null|
# |        James|   null| 3000|
# |          Jen|   3300| null|
# |      Michael|   null| 4600|
# |         Saif|   null| 4100|
# |        Maria|   3000| null|
# +-------------+-------+-----+

# Drop columns
df1 = get_pyspark_df1()
df1.drop("Name", "Age").show()
# +-------+
# |Company|
# +-------+
# | Amazon|
# | Google|
# | Oracle|
# +-------+

# Limit rows
df1.limit(1).show()
# +-----+---+-------+
# | Name|Age|Company|
# +-----+---+-------+
# |Alice| 28| Amazon|
# +-----+---+-------+

# Drop rows with null values
df_null = get_pyspark_df_null()
df_null.show()
# +----+----+
# |   a|   b|
# +----+----+
# |null|null|
# |   1|null|
# |null|   2|
# |   3|   3|
# +----+----+
df_null.dropna().show()  # Drop rows that have at least one null value
# +---+---+
# |  a|  b|
# +---+---+
# |  3|  3|
# +---+---+
df_null.dropna(subset=["a"]).show()  # Drop rows that have null values in specific cols
# +---+----+
# |  a|   b|
# +---+----+
# |  1|null|
# |  3|   3|
# +---+----+
df_null.dropna(how="all").show()  # Drop rows that have null values in all columns
# +----+----+
# |   a|   b|
# +----+----+
# |   1|null|
# |null|   2|
# |   3|   3|
# +----+----+

# Fill null rows with a specified value.
# If the data type of the value does not match the data type of the column, the value will remain null.
df_null.fillna(-1).show()  # Fill all null values with a specified value
# +----+---+
# |   a|  b|
# +----+---+
# |null| -1|
# |   1| -1|
# |null|  2|
# |   3|  3|
# +----+---+
df_null.fillna({"a": "unknown", "b": -1}).show()  # Fill all null values with a specified value in specific columns
# +-------+---+
# |      a|  b|
# +-------+---+
# |unknown| -1|
# |      1| -1|
# |unknown|  2|
# |      3|  3|
# +-------+---+

# select all rows with nulls
df_null.filter(col("b").isNull()).show()
# +----+----+
# |   a|   b|
# +----+----+
# |null|null|
# |   1|null|
# +----+----+

# select all rows without nulls
df_null.filter(col("b").isNotNull()).show()
# +----+---+
# |   a|  b|
# +----+---+
# |null|  2|
# |   3|  3|
# +----+---+

# Replace all occurrences of value with new specified value
df_null.replace("1", "2", subset=["a"]).show()
# +----+----+
# |   a|   b|
# +----+----+
# |null|null|
# |   2|null|
# |null|   2|
# |   3|   3|
# +----+----+

# Repartition and coalesce
# repartition() - Returns a new DataFrame partitioned by the given partitioning expressions (number or/and column).
# The resulting DataFrame is hash partitioned. The size of partitions can be greater or less than the original.
# coalesce() - Returns a new DataFrame that has exactly numPartitions partitions. If a larger number of partitions
# is requested, it will stay at the current number of partitions.
print(df.repartition(3).rdd.getNumPartitions())  # 3
print(df.coalesce(1).rdd.getNumPartitions())  # 1
print(df.repartition('Name').rdd.getNumPartitions())  # 1
print(df.repartition(3, 'Name').rdd.getNumPartitions())  # 3 Repartition the data into 3 partitions by ‘Name’ column

# Union, how to add new rows to existing Dataframe, column positions should be the same
# To perform union by column names use unionByName, if allowMissingColumns=True, missing columns will be filled with null.
df2 = get_pyspark_df2()
df1.union(df2).show()
# +-------+---+---------+
# |   Name|Age|  Company|
# +-------+---+---------+
# |  Alice| 28|   Amazon|
# |    Bob| 25|   Google|
# |Charlie| 30|   Oracle|
# |   Alex| 38|Microsoft|
# |   John| 35|  Netflix|
# +-------+---+---------+

# If we need to union a list with dfs:
# Suppose you have a list of DataFrames: dfs = [df1, df2, df3, ..., dfN]
# from functools import reduce
# result_df = reduce(lambda df1, df2: df1.union(df2), dfs)

# randomSplit(), randomly splits this DataFrame with the provided weights, seed - to control the randomness.
df_split = get_array_split()
df1, df2 = df_split.randomSplit([0.3, 0.7], 42)
df1.show()
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |        Scott|   Finance|  3300|
# |         Saif|     Sales|  4100|
# |         Mike|        HR|  1900|
# +-------------+----------+------+
df2.show()
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |        James|     Sales|  3000|
# |      Michael|     Sales|  4600|
# |        Maria|   Finance|  3000|
# |          Jen|   Finance|  3300|
# |         Alex|     Sales|  4000|
# |          Bob|   Finance|  3900|
# |      Michael|     Sales|  4900|
# +-------------+----------+------+

# cache() and persist()
# Every action (like show() or count()) triggers a computation pipeline that might include re-reading data from
# disk if it's not cached or persisted.
# Caching saves the data in memory or on disk (depending on the persistence level), so subsequent actions on
# the same DataFrame or RDD do not require reading the data again from the disk.
# With cache(), we can use only the default storage level:
# - MEMORY_ONLY for RDD
# - MEMORY_AND_DISK for DataFrame
# With persist(), we can specify which storage level we want for RDD and DataFrame, such as MEMORY_ONLY, DISK_ONLY, etc.
df.cache()
print(df.count())  # Now we can use `df` in more transformations and actions without reading the data from disk again
df.persist(StorageLevel.MEMORY_AND_DISK)  # Equivalent to df.cache()
# Unpersisting: If you no longer need the cached or persisted data, call df.unpersist()


# PySpark SQL
df_my_table = spark.table("my_table")
# is similar to
df_my_table = spark.sql("SELECT * FROM my_table")

# Register Temp View and Execute SQL
df_people = get_pyspark_df()
df_people.createOrReplaceTempView("people")

df_cities = get_pyspark_df_cities()
df_cities.createOrReplaceTempView("cities")

spark.sql("SELECT Name, Age FROM people WHERE Age > 26").show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |Charlie| 30|
# +-------+---+

# Aggreagation
spark.sql("SELECT Max(Age), AVG(Age) AS Max_Age FROM people").show()
# +--------+------------------+
# |max(Age)|           Max_Age|
# +--------+------------------+
# |      30|27.666666666666668|
# +--------+------------------+

# Join
spark.sql("SELECT p.Name, c.City FROM people p INNER JOIN cities c ON p.Name = c.Name").show()
# +-----+-------------+
# | Name|         City|
# +-----+-------------+
# |Alice|     New York|
# |  Bob|San Francisco|
# +-----+-------------+

# Subquery
spark.sql("SELECT * FROM people WHERE Age > (SELECT AVG(AGE) FROM people)").show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |Charlie| 30|
# +-------+---+

# Read a delta table from path
spark.sql("SELECT * FROM delta.`/path/to/delta_table`").show()


# PySpark DataFrame Joins

df1 = get_pyspark_df_join1()
df1.show()
# +-----+---+
# | Name|Age|
# +-----+---+
# |Alice| 28|
# |  Bob| 25|
# +-----+---+
df2 = get_pyspark_df_join2()
df2.show()
# +-------+------+
# |   Name|Gender|
# +-------+------+
# |  Alice|     F|
# |    Bob|     M|
# |Charlie|     M|
# +-------+------+
df3 = get_pyspark_df_join3()
# +----+------+
# |Name|Gender|
# +----+------+
# |Mike|     M|
# | Bob|     M|
# +----+------+

# Inner Join
inner_join = df1.join(df2, on="Name", how="inner")
inner_join.show()
# +-----+---+------+
# | Name|Age|Gender|
# +-----+---+------+
# |Alice| 28|     F|
# |  Bob| 25|     M|
# +-----+---+------+

# Important moment with pyspark join
inner_join = df1.join(df2, df1.Name == df2.Name, how="inner")
inner_join.show()
# +-----+---+-----+------+
# | Name|Age| Name|Gender|
# +-----+---+-----+------+
# |Alice| 28|Alice|     F|
# |  Bob| 25|  Bob|     M|
# +-----+---+-----+------+

# To fix it
inner_join = df1.join(df2, df1.Name == df2.Name, how="inner").select(df1.Name, df1.Age, df2.Gender)
inner_join.show()
# +-----+---+------+
# | Name|Age|Gender|
# +-----+---+------+
# |Alice| 28|     F|
# |  Bob| 25|     M|
# +-----+---+------+

# Left Join
left_join = df2.join(df1, on="Name", how="left")
left_join.show()
# +-------+------+----+
# |   Name|Gender| Age|
# +-------+------+----+
# |  Alice|     F|  28|
# |    Bob|     M|  25|
# |Charlie|     M|null|
# +-------+------+----+

# Broadcast Join
df1.join(broadcast(df2), on="Name", how="inner").explain()
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Project [Name#156, Age#157L, Gender#161]
#    +- BroadcastHashJoin [Name#156], [Name#160], Inner, BuildRight, false, true
#       :- Filter isnotnull(Name#156)
#       :  +- Scan ExistingRDD[Name#156,Age#157L]
#       +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=396]
#          +- Filter isnotnull(Name#160)
#             +- Scan ExistingRDD[Name#160,Gender#161]

broadcast_join = df1.join(broadcast(df2), on="Name", how="inner")
broadcast_join.show()
# +-----+---+------+
# | Name|Age|Gender|
# +-----+---+------+
# |Alice| 28|     F|
# |  Bob| 25|     M|
# +-----+---+------+

# Get a new df containing rows in the df2 but not in df3 while preserving duplicates:
df2.exceptAll(df3).show()
# +-------+------+
# |   Name|Gender|
# +-------+------+
# |  Alice|     F|
# |Charlie|     M|
# +-------+------+


# PySpark File I/O
# A typical write operation:
# 1. In Databricks we can omit the format option, since delta format is default.
# 2. Mode:
# - append: Append contents of this DataFrame to existing data.
# - overwrite: Overwrite existing data. The schema of the df does not need to be the same as that of the existing table.
# - error or errorifexists(default): Throw an exception if data already exists.
# - ignore: Silently ignore this operation if data already exists.
# 3. partitionBy and bucketBy - the separate section below.
# 4. saveAsTable() - creates a table in the Hive metastore within default metastore location.
# 5. save() - saves the df to the specified path.
# Example:
output_path = "/tmp/partitioned_data"
df.write \
  .partitionBy("department") \
  .format("parquet") \
  .mode("overwrite") \
  .save(output_path)
# OR
partitioned_table = "partitioned_table"
df.write \
  .mode("append") \
  .format("parquet") \
  .saveAsTable(partitioned_table)

# When table is created we can use df.write.insertInto("table_name", overwrite=False) to append data and overwrite
# the data in the table with overwrite=True

current_time = time.time()
df = get_pyspark_df()

# Write DataFrame as CSV
df.write.csv(f"/output-{current_time}.csv", header=True)

# Read CSV File with the schema
df = spark.read.csv(f"/output-{current_time}.csv", header=True, inferSchema=True)
# or
df = spark.read.format("csv").options(header=True, inferSchema=True).load(f"/output-{current_time}.csv")
df.show()

# Read CSV file without the schema, schema is StructType([StructField...])
# df = spark.read.csv(f"/output-{current_time}.csv", header=False, schema=schema)

# Read and Write Parquet
df.write.parquet(f"/output-{current_time}.parquet")
# or
df.write.format("parquet").save(f"/output-{current_time}.parquet")

df_parquet = spark.read.parquet(f"/output-{current_time}.parquet")
# or
# path might be directly to the storage account/container/folder
# abfss://curated-container@testaccount123.dfs.core.windows.net/data/
df_parquet = spark.read.format("parquet").load(f"/output-{current_time}.parquet")
df_parquet.show()

# Read and write JSON (set multiline option to true to read JSON records on file from multiple lines)
df.write.format("json").save(f"/output-{current_time}.json")
df_json = spark.read.format("json").load(f"/output-{current_time}.json")
df_json.show()

# Read and write AVRO
df.write.format("avro").save(f"/output-{current_time}.avro")
df_avro = spark.read.format("avro").load(f"/output-{current_time}.avro")
df_avro.show()

# Read and write XML (need to ensure spark-xml package is available)
df.write.format("com.databricks.spark.xml").options(rowTag='book').save(f"/output-{current_time}.xml")
df_xml = spark.read.format("com.databricks.spark.xml").options(rowTag='book').load(f"/output-{current_time}.xml")
df_xml.show()

# Read and write jdbc
jdbc_url = "jdbc:mysql://hostname:3306/database"
jdbc_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

rdbms_data = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "source_table") \
    .options(**jdbc_properties) \
    .load()
rdbms_data.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "target_table") \
    .options(**jdbc_properties) \
    .mode("overwrite") \
    .save()


# PySpark Functions Module
# We can use these functions within select and withColumn methods.
df = get_pyspark_df()
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# concat() - concatenates multiple input columns together into a single column
# lit() - creates a Column of literal value. Often used with withColumn() expression,
# because it requires a Column type parameter.
# col() - returns a Column(type) based on the given column name
df.withColumn("FullMessage", concat(col("Name"), lit(" is "), col("Age"), lit(" years old."))).show()
# +-------+---+--------------------+
# |   Name|Age|        FullMessage|
# +-------+---+--------------------+
# |  Alice| 28|Alice is 28 years...|
# |    Bob| 25|  Bob is 25 years...|
# |Charlie| 30|Charlie is 30 yea...|
# +-------+---+--------------------+

# when() - Evaluates a list of conditions and returns one of multiple possible result expressions.
# If pyspark.sql.Column.otherwise() is not invoked, None is returned for unmatched conditions.
df.withColumn("Name", when(df.Name == "Alice", "Alicia").otherwise(df.Name)).show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# | Alicia| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

# expr() - Parses the expression string into the column that it represents. Expression defined in string.
# to_date() - Converts a Column into pyspark.sql.types.DateType using the optionally specified format.
# Equivalent to col.cast("date").
# col(column_name).cast(dataType) or df.column_name.cast(dataType) - Casts the column into type dataType.
# to_timestamp() - Converts a Column into pyspark.sql.types.TimestampType using the optionally specified format.
# current_date() - get current date
# date_format() - Converts a date/timestamp/string to a value of string in the format specified by the date format.
df_date = get_pyspark_df_date()
df_date_format = df_date.select("Name", expr("length(name)").alias("lenght of the name"), to_date("Date"), to_timestamp("Date"),
                                col("Date").cast("date"), date_format(col("Date"), "yyyy-MM").alias("yyyy-mm"), current_date())
df_date_format.show()
# +-------+------------------+-------------+-------------------+----------+-------+--------------+
# |   Name|lenght of the name|to_date(Date)| to_timestamp(Date)|      Date|yyyy-mm|current_date()|
# +-------+------------------+-------------+-------------------+----------+-------+--------------+
# |  Alice|                 5|   1997-02-28|1997-02-28 10:30:00|1997-02-28|1997-02|    2025-06-12|
# |    Bob|                 3|   2000-02-28|2000-02-28 10:30:00|2000-02-28|2000-02|    2025-06-12|
# |Charlie|                 7|   2005-02-28|2005-02-28 10:30:00|2005-02-28|2005-02|    2025-06-12|
# +-------+------------------+-------------+-------------------+----------+-------+--------------+
df_date_format.printSchema()
# root
#  |-- Name: string (nullable = true)
#  |-- lenght of the name: integer (nullable = true)
#  |-- to_date(Date): date (nullable = true)
#  |-- to_timestamp(Date): timestamp (nullable = true)
#  |-- Date: date (nullable = true)
#  |-- yyyy-mm: string (nullable = true)
#  |-- current_date(): date (nullable = false)

# create_map() - The create_map() function in Apache Spark is popularly used to convert the selected or all the
# DataFrame columns to the MapType, similar to the Python Dictionary (Dict) object.
# concat_ws() - Concatenates multiple input string columns together into a single string column, using the given separator.
df.select(create_map('Name', 'Age').alias("map"), concat_ws(' is ', 'Name', 'Age').alias('concat string')).show()
# +---------------+-------------+
# |            map|concat string|
# +---------------+-------------+
# |  {Alice -> 28}|  Alice is 28|
# |    {Bob -> 25}|    Bob is 25|
# |{Charlie -> 30}|Charlie is 30|
# +---------------+-------------+

# String operations:
# substring_index() - Returns the substring from string str before count occurrences of the delimiter delim.
# regexp_replace() - Replace all substrings of the specified string value that match regexp with replacement.
# upper() - Converts a string expression to upper case.
# length() - Computes the character length of string data or number of bytes of binary data.
# substring() - Substring starts at pos and is of length len.
# trim() - Trim the spaces from both ends for the specified string column.
# instr() - Locate the position of the first occurrence of substr. The position is 1 based index.
# split() - Splits str around matches of the given pattern.
df.select(substring_index(col("Name"), "_", 1).alias("name_substring"),
          regexp_replace(col("Name"), r"\d", "").alias("name_without_digits"),
          upper(col("Name")).alias("name_upper"), length(col("Name")).alias("name_length"),
          substring(col("Name"), 1, 3).alias("name_3"),
          trim(col("Name")).alias("name_trim"),
          instr(col("Name"), "_").alias("underscore_position"),
          split(col("Name"), "_").alias("split_array"),
          split(col("Name"), "_")[0].alias("split_name")
          ).show()
# +-------------+-------------------+----------+-----------+------+---------+-------------------+-----------+----------+
# |name_substing|name_without_digits|name_upper|name_length|name_3|name_trim|underscore_position|split_array|split_name|
# +-------------+-------------------+----------+-----------+------+---------+-------------------+-----------+----------+
# |        Alice|             Alice_|   ALICE_1|          7|   Ali|  Alice_1|                  6| [Alice, 1]|     Alice|
# |          Bob|               Bob_|     BOB_2|          5|   Bob|    Bob_2|                  4|   [Bob, 2]|       Bob|
# |    Charlie3 |           Charlie | CHARLIE3 |          9|   Cha| Charlie3|                  0|[Charlie3 ]| Charlie3 |
# +-------------+-------------------+----------+-----------+------+---------+-------------------+-----------+----------+

# struct() - Creates a new struct column.
struct_df = df.select(struct("Name", "Age").alias("person_info"))
struct_df.show(truncate=False)
# +-------------+
# |person_info  |
# +-------------+
# |{Alice, 28}  |
# |{Bob, 25}    |
# |{Charlie, 30}|
# +-------------+
struct_df.select("person_info.Age").show()
# +---+
# |Age|
# +---+
# | 28|
# | 25|
# | 30|
# +---+

# Array operations:
df = get_array_df()
df.show()
# +---+------------+---------+---------+
# | id|     numbers| numbers1|  letters|
# +---+------------+---------+---------+
# |  1|[1, 2, 3, 4]|[3, 4, 5]|[a, b, c]|
# |  2|   [5, 6, 7]|   [7, 8]|   [x, y]|
# |  3|          []|   [2, 3]|       []|
# +---+------------+---------+---------+

# explode() - returns a new row for each element in the given array or map.
df.select("id", "numbers1", "letters", explode("numbers").alias("number")).show()
# +---+---------+---------+------+
# | id| numbers1|  letters|number|
# +---+---------+---------+------+
# |  1|[3, 4, 5]|[a, b, c]|     1|
# |  1|[3, 4, 5]|[a, b, c]|     2|
# |  1|[3, 4, 5]|[a, b, c]|     3|
# |  1|[3, 4, 5]|[a, b, c]|     4|
# |  2|   [7, 8]|   [x, y]|     5|
# |  2|   [7, 8]|   [x, y]|     6|
# |  2|   [7, 8]|   [x, y]|     7|
# +---+---------+---------+------+

# posexplode() - returns a new row for each element with position in the given array or map.
# posexplode_outer() - Returns a new row for each element with position in the given array or map.
# Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced.
df.select("id", "numbers1", "letters", posexplode("numbers").alias("pos", "number")).show()
# +---+---------+---------+---+------+
# | id| numbers1|  letters|pos|number|
# +---+---------+---------+---+------+
# |  1|[3, 4, 5]|[a, b, c]|  0|     1|
# |  1|[3, 4, 5]|[a, b, c]|  1|     2|
# |  1|[3, 4, 5]|[a, b, c]|  2|     3|
# |  1|[3, 4, 5]|[a, b, c]|  3|     4|
# |  2|   [7, 8]|   [x, y]|  0|     5|
# |  2|   [7, 8]|   [x, y]|  1|     6|
# |  2|   [7, 8]|   [x, y]|  2|     7|
# +---+---------+---------+---+------+

# array_contains() - This function returns a boolean indicating whether the array contains the given value.
df.select("id", "numbers", array_contains("numbers", 3).alias("contains_3")).show()
# +---+------------+----------+
# | id|     numbers|contains_3|
# +---+------------+----------+
# |  1|[1, 2, 3, 4]|      true|
# |  2|   [5, 6, 7]|     false|
# |  3|          []|     false|
# +---+------------+----------+

# arrays_overlap() - This function returns a boolean column indicating if the input arrays have common non-null elements.
df.select("id", "numbers", "numbers1", arrays_overlap("numbers", "numbers1").alias("has_overlap")).show()
# +---+------------+---------+-----------+
# | id|     numbers| numbers1|has_overlap|
# +---+------------+---------+-----------+
# |  1|[1, 2, 3, 4]|[3, 4, 5]|       true|
# |  2|   [5, 6, 7]|   [7, 8]|       true|
# |  3|          []|   [2, 3]|      false|
# +---+------------+---------+-----------+

# arrays_zip () - Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays
df.select("id", "numbers", "letters", arrays_zip("numbers", "letters").alias("zipped_array")).show(truncate=False)
# +---+------------+---------+-----------------------------------+
# |id |numbers     |letters  |zipped_array                       |
# +---+------------+---------+-----------------------------------+
# |1  |[1, 2, 3, 4]|[a, b, c]|[{1, a}, {2, b}, {3, c}, {4, null}]|
# |2  |[5, 6, 7]   |[x, y]   |[{5, x}, {6, y}, {7, null}]        |
# |3  |[]          |[]       |[]                                 |
# +---+------------+---------+-----------------------------------+

# element_at() -  Returns element of array at given (1-based) index.
df.select("id", "numbers", element_at("numbers", 2).alias("second_element_numbers")).show()
# +---+------------+----------------------+
# | id|     numbers|second_element_numbers|
# +---+------------+----------------------+
# |  1|[1, 2, 3, 4]|                     2|
# |  2|   [5, 6, 7]|                     6|
# |  3|          []|                  null|
# +---+------------+----------------------+

# transform() - Returns an array of elements after applying a transformation to each element in the input array.
df.select("id", "numbers", transform("numbers", lambda x: x * 10).alias("transformed_array_numbers")).show()
# +---+------------+-------------------------+
# | id|     numbers|transformed_array_numbers|
# +---+------------+-------------------------+
# |  1|[1, 2, 3, 4]|         [10, 20, 30, 40]|
# |  2|   [5, 6, 7]|             [50, 60, 70]|
# |  3|          []|                       []|
# +---+------------+-------------------------+

# array_union(): Returns a new array containing the union of elements in col1 and col2, without duplicates.
df = df.withColumn("union_array", array_union(col("numbers"), col("numbers1")))
df.show()
# +---+------------+---------+---------+---------------+
# | id|     numbers| numbers1|  letters|    union_array|
# +---+------------+---------+---------+---------------+
# |  1|[1, 2, 3, 4]|[3, 4, 5]|[a, b, c]|[1, 2, 3, 4, 5]|
# |  2|   [5, 6, 7]|   [7, 8]|   [x, y]|   [5, 6, 7, 8]|
# |  3|          []|   [2, 3]|       []|         [2, 3]|
# +---+------------+---------+---------+---------------+

# collect_list(): Collects the values from a column into a list, with duplicates, and returns this list of objects.
df_collect = get_pyspark_df3()
df_collect.show()
# +-------+------+
# |   Name|Salary|
# +-------+------+
# |  Alice|  2000|
# |    Bob|  3000|
# |Charlie|  4000|
# |    Bob|  4000|
# +-------+------+

df_collect = df_collect.groupBy("Name").agg(collect_list("Salary").alias("salary_list"))
df_collect.show()
# +-------+------------+
# |   Name| salary_list|
# +-------+------------+
# |  Alice|      [2000]|
# |    Bob|[3000, 4000]|
# |Charlie|      [4000]|
# +-------+------------+

# coalesce() - Returns the first column that is not null or the default value
# from Spark version 3.5.0
# ifnull() - Returns col2 if col1 is null, or col1 otherwise.
# nullif() - Returns null if col1 equals to col2, or col1 otherwise.
df_null = get_pyspark_df_null()
df_null.show()
# +----+----+
# |   a|   b|
# +----+----+
# |null|null|
# |   1|null|
# |null|   2|
# |   3|   3|
# +----+----+
df_null.select(coalesce(df_null["a"], df_null["b"])).show()
# +--------------+
# |coalesce(a, b)|
# +--------------+
# |          null|
# |             1|
# |             2|
# |             3|
# +--------------+

df_null.select('*', coalesce(df_null["a"], lit(0.0))).show()
# +----+----+----------------+
# |   a|   b|coalesce(a, 0.0)|
# +----+----+----------------+
# |null|null|             0.0|
# |   1|null|               1|
# |null|   2|             0.0|
# |   3|   3|               3|
# +----+----+----------------+

# udf() - create and use an udf, user memory is in use, IntegerType() - return type
def increment_age(age):
    return age + 1

increment_age_udf = udf(increment_age, IntegerType())

df.withColumn("IncrementAge", increment_age_udf(df["Age"])).show()
# +-------+---+------------+
# |   Name|Age|IncrementAge|
# +-------+---+------------+
# |  Alice| 28|          29|
# |    Bob| 25|          26|
# |Charlie| 30|          31|
# +-------+---+------------+


# Window Functions
df_window = get_pyspark_df_window()
df_window.show()
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |        James|     Sales|  3000|
# |      Michael|     Sales|  4600|
# |        Maria|   Finance|  3000|
# |        Scott|   Finance|  3300|
# |          Jen|   Finance|  3300|
# |         Saif|     Sales|  4100|
# +-------------+----------+------+

window_spec = Window.partitionBy("department").orderBy(df_window["salary"].desc())
# for some cases we can use previous rows from df
# window_spec = Window.orderBy(df["day"].asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
# or consider rows with values are within some range
# window_spec = Window.partitionBy("partition_column").orderBy("order_column").rangeBetween(-2, 2)
# or even omit partitionBy() if we need to get a rank between all departments
# window_spec = Window.orderBy(df_window["salary"].desc())

# row_number, rank, dense_rank, percent_rank, ntile
df_window.withColumn("row_number", row_number().over(window_spec)).show()
# +-------------+----------+------+----------+
# |employee_name|department|salary|row_number|
# +-------------+----------+------+----------+
# |        Scott|   Finance|  3300|         1|
# |          Jen|   Finance|  3300|         2|
# |        Maria|   Finance|  3000|         3|
# |      Michael|     Sales|  4600|         1|
# |         Saif|     Sales|  4100|         2|
# |        James|     Sales|  3000|         3|
# +-------------+----------+------+----------+

df_window.select('*', row_number().over(window_spec).alias("row_number")).filter(col("row_number") == 1).show()
# +-------------+----------+------+----------+
# |employee_name|department|salary|row_number|
# +-------------+----------+------+----------+
# |        Scott|   Finance|  3300|         1|
# |      Michael|     Sales|  4600|         1|
# +-------------+----------+------+----------+

df_window.withColumn("rank", rank().over(window_spec)).show()
# +-------------+----------+------+----+
# |employee_name|department|salary|rank|
# +-------------+----------+------+----+
# |        Scott|   Finance|  3300|   1|
# |          Jen|   Finance|  3300|   1|
# |        Maria|   Finance|  3000|   3|
# |      Michael|     Sales|  4600|   1|
# |         Saif|     Sales|  4100|   2|
# |        James|     Sales|  3000|   3|
# +-------------+----------+------+----+

df_window.withColumn("dense_rank", dense_rank().over(window_spec)).show()
# +-------------+----------+------+----------+
# |employee_name|department|salary|dense_rank|
# +-------------+----------+------+----------+
# |        Scott|   Finance|  3300|         1|
# |          Jen|   Finance|  3300|         1|
# |        Maria|   Finance|  3000|         2|
# |      Michael|     Sales|  4600|         1|
# |         Saif|     Sales|  4100|         2|
# |        James|     Sales|  3000|         3|
# +-------------+----------+------+----------+

df_window.withColumn("percent_rank", percent_rank().over(window_spec)).show()
# +-------------+----------+------+------------+
# |employee_name|department|salary|percent_rank|
# +-------------+----------+------+------------+
# |        Scott|   Finance|  3300|         0.0|
# |          Jen|   Finance|  3300|         0.0|
# |        Maria|   Finance|  3000|         1.0|
# |      Michael|     Sales|  4600|         0.0|
# |         Saif|     Sales|  4100|         0.5|
# |        James|     Sales|  3000|         1.0|
# +-------------+----------+------+------------+

df_window.withColumn("ntile", ntile(2).over(window_spec)).show()
# +-------------+----------+------+-----+
# |employee_name|department|salary|ntile|
# +-------------+----------+------+-----+
# |        Scott|   Finance|  3300|    1|
# |          Jen|   Finance|  3300|    1|
# |        Maria|   Finance|  3000|    2|
# |      Michael|     Sales|  4600|    1|
# |         Saif|     Sales|  4100|    1|
# |        James|     Sales|  3000|    2|
# +-------------+----------+------+-----+

# cume_dist, lag, lead
df_window.withColumn("cume_dist", cume_dist().over(window_spec)).show()
# +-------------+----------+------+------------------+
# |employee_name|department|salary|         cume_dist|
# +-------------+----------+------+------------------+
# |        Scott|   Finance|  3300|0.6666666666666666|
# |          Jen|   Finance|  3300|0.6666666666666666|
# |        Maria|   Finance|  3000|               1.0|
# |      Michael|     Sales|  4600|0.3333333333333333|
# |         Saif|     Sales|  4100|0.6666666666666666|
# |        James|     Sales|  3000|               1.0|
# +-------------+----------+------+------------------+

df_window.withColumn("lag", lag("salary", 2).over(window_spec)).show()
# +-------------+----------+------+----+
# |employee_name|department|salary| lag|
# +-------------+----------+------+----+
# |        Scott|   Finance|  3300|null|
# |          Jen|   Finance|  3300|null|
# |        Maria|   Finance|  3000|3300|
# |      Michael|     Sales|  4600|null|
# |         Saif|     Sales|  4100|null|
# |        James|     Sales|  3000|4600|
# +-------------+----------+------+----+

df_window.withColumn("lead", lead("salary", 2).over(window_spec)).show()
# +-------------+----------+------+----+
# |employee_name|department|salary|lead|
# +-------------+----------+------+----+
# |        Scott|   Finance|  3300|3000|
# |          Jen|   Finance|  3300|null|
# |        Maria|   Finance|  3000|null|
# |      Michael|     Sales|  4600|3000|
# |         Saif|     Sales|  4100|null|
# |        James|     Sales|  3000|null|
# +-------------+----------+------+----+

# avg, sum, min, max, round
window_spec_agg = Window.partitionBy("department")

df.withColumn("avg", avg(col("salary")).over(window_spec_agg)) \
  .withColumn("sum", sum(col("salary")).over(window_spec_agg)) \
  .withColumn("min", min(col("salary")).over(window_spec_agg)) \
  .withColumn("max", max(col("salary")).over(window_spec_agg)) \
  .select("department", "avg", "sum", "min", "max") \
  .show()
# +----------+------+-----+----+----+
# |department|   avg|  sum| min| max|
# +----------+------+-----+----+----+
# |   Finance|3200.0| 9600|3000|3300|
# |   Finance|3200.0| 9600|3000|3300|
# |   Finance|3200.0| 9600|3000|3300|
# |     Sales|3900.0|11700|3000|4600|
# |     Sales|3900.0|11700|3000|4600|
# |     Sales|3900.0|11700|3000|4600|
# +----------+------+-----+----+----+


# Salt implementation
# Salt in PySpark is commonly used for handling skewed data during joins/agg or to distribute keys more
# uniformly across partitions. When you have a dataset that heavily skews toward certain key values
# (e.g., in a join operation), the computation may become unbalanced as some partitions have a much larger workload
# than others. Adding a salt value to these keys helps distribute the data across partitions more evenly.
# After salting, always recombine results to get back to the original keys by removing the salt
# (e.g., splitting the salted key)!!!

# AGGREGATION SALT
# When we can use Salt: sum(), count(), max(), min()
# avg() - we can't use as method, but we can combine results using total sum / total count, check example below
# When we can't use Salt: first() and last() (sorted), countDistinct(), rank(), denseRank()
# Avoid salting for holistic, order-sensitive, or distinct operations.

# Example with different keys:
# Sample skewed dataset
data = [
    ("A", 100),  # "A" is heavily skewed
    ("A", 200),
    ("A", 300),
    ("A", 400),
    ("B", 50),   # "B" and "C" are not skewed
    ("B", 60),
    ("C", 30),
    ("C", 40),
    ("C", 50)
]

# Create DataFrame
df = spark.createDataFrame(data, ["key", "value"])

# Show the raw data
print("Original Dataset:")
df.show()
# +---+-----+
# |key|value|
# +---+-----+
# |  A|  100|
# |  A|  200|
# |  A|  300|
# |  A|  400|
# |  B|   50|
# |  B|   60|
# |  C|   30|
# |  C|   40|
# |  C|   50|
# +---+-----+

# Step 1: Add a salt column
# For the skewed keys, add a random salt value to redistribute the data
salt_range = 3  # Adjust based on the skewed nature of the data
df_salted = df.withColumn("salt", floor(rand() * salt_range)) \
              .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Show the salted data
print("Salted Dataset:")
df_salted.show()
# +---+-----+----+----------+
# |key|value|salt|salted_key|
# +---+-----+----+----------+
# |  A|  100|   1|       A_1|
# |  A|  200|   1|       A_1|
# |  A|  300|   0|       A_0|
# |  A|  400|   1|       A_1|
# |  B|   50|   2|       B_2|
# |  B|   60|   0|       B_0|
# |  C|   30|   1|       C_1|
# |  C|   40|   0|       C_0|
# |  C|   50|   2|       C_2|
# +---+-----+----+----------+

# Step 2: Perform the aggregation on the salted data
# Example: Calculate the sum of values for each salted key
aggregated_salted = df_salted.groupBy("salted_key").agg(sum("value").alias("sum_value"),
                                                        max("value").alias("max_value"),
                                                        count("*").alias("count"))

# Show the aggregated result for salted keys
print("Aggregated Dataset with Salted Keys:")
aggregated_salted.show()
# +----------+---------+---------+-----+
# |salted_key|sum_value|max_value|count|
# +----------+---------+---------+-----+
# |       A_1|      700|      400|    3|
# |       A_0|      300|      300|    1|
# |       B_2|       50|       50|    1|
# |       B_0|       60|       60|    1|
# |       C_1|       30|       30|    1|
# |       C_2|       50|       50|    1|
# |       C_0|       40|       40|    1|
# +----------+---------+---------+-----+

# Step 3: Recombine the results by removing the salt
# Extract the original key by splitting the salted key
final_result = aggregated_salted.withColumn("original_key", substring_index(col("salted_key"), "_", 1))\
                                .groupBy("original_key").agg(sum("sum_value").alias("total_sum"),
                                                             max("max_value").alias("max_value"),
                                                             (sum("sum_value") / sum("count")).alias("avg"))

# Show the final aggregated result
print("Final Aggregated Dataset (De-salted):")
final_result.show()
# +------------+---------+---------+-----+
# |original_key|total_sum|max_value|  avg|
# +------------+---------+---------+-----+
# |           B|      110|       60| 55.0|
# |           C|      120|       50| 40.0|
# |           A|     1000|      400|250.0|
# +------------+---------+---------+-----+

# Example with the single key:
# Sample skewed dataset
data = [
    ("A", 100),  # "A" is heavily skewed
    ("A", 200),
    ("A", 300),
    ("A", 400),
    ("A", 50),
    ("A", 60),
    ("A", 30),
    ("A", 40),
    ("A", 50)
]

# Create DataFrame
df = spark.createDataFrame(data, ["key", "value"])

# Show the raw data
print("Original Dataset:")
df.show()
# +---+-----+
# |key|value|
# +---+-----+
# |  A|  100|
# |  A|  200|
# |  A|  300|
# |  A|  400|
# |  A|   50|
# |  A|   60|
# |  A|   30|
# |  A|   40|
# |  A|   50|
# +---+-----+

# Step 1: Add a salt column
# For the skewed keys, add a random salt value to redistribute the data
salt_range = 3  # Adjust based on the skewed nature of the data
df_salted = df.withColumn("salted_key", floor(rand() * salt_range))

# Show the salted data
print("Salted Dataset:")
df_salted.show()
# +---+-----+----------+
# |key|value|salted_key|
# +---+-----+----------+
# |  A|  100|         0|
# |  A|  200|         0|
# |  A|  300|         0|
# |  A|  400|         0|
# |  A|   50|         0|
# |  A|   60|         1|
# |  A|   30|         2|
# |  A|   40|         2|
# |  A|   50|         2|
# +---+-----+----------+

# Step 2: Perform the aggregation on the salted data
# Example: Calculate the sum of values for each salted key
aggregated_salted = df_salted.groupBy("salted_key", "key").agg(sum("value").alias("sum_value"))

# Show the aggregated result for salted keys
print("Aggregated Dataset with Salted Keys:")
aggregated_salted.show()
# +---+----------+---------+
# |key|salted_key|sum_value|
# +---+----------+---------+
# |  A|         0|     1050|
# |  A|         1|       60|
# |  A|         2|      120|
# +---+----------+---------+

# Step 3: Recombine the results by removing the salt
# Extract the original key by splitting the salted key
final_result = aggregated_salted.groupBy("key").agg(sum("sum_value").alias("total_sum"))

# Show the final aggregated result
print("Final Aggregated Dataset (De-salted):")
final_result.show()
# +---+---------+
# |key|total_sum|
# +---+---------+
# |  A|     1230|
# +---+---------+

# JOIN SALT
large_dataset = spark.createDataFrame([
    (1, "value1"),
    (1, "value1"),
    (1, "value1"),
    (2, "value2"),
    (3, "value3")
], ["join_key", "data"])
print("Large Dataset:")
large_dataset.show()
# +--------+------+
# |join_key|  data|
# +--------+------+
# |       1|value1|
# |       1|value1|
# |       1|value1|
# |       2|value2|
# |       3|value3|
# +--------+------+

small_dataset = spark.createDataFrame([
    (1, "small_value1"),
    (2, "small_value2"),
    (3, "small_value3")
], ["join_key", "small_data"])
print("Small Dataset:")
small_dataset.show()
# +--------+------------+
# |join_key|  small_data|
# +--------+------------+
# |       1|small_value1|
# |       2|small_value2|
# |       3|small_value3|
# +--------+------------+

# Add salt to the large dataset
# Choose a salt range based on the degree of skew; here, we use [0, 2] (3 buckets)
# floor() - returns the nearest integer that is less than or equal to given value.
# rand() - Generates a random column with samples uniformly distributed in [0.0, 1.0)
salt_range = 3
large_salted = large_dataset.withColumn("salt", floor(rand() * salt_range)) \
                            .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt")))
print("Large Salted Dataset:")
large_salted.show()
# +--------+------+----+----------+
# |join_key|  data|salt|salted_key|
# +--------+------+----+----------+
# |       1|value1|   0|       1_0|
# |       1|value1|   0|       1_0|
# |       1|value1|   2|       1_2|
# |       2|value2|   0|       2_0|
# |       3|value3|   1|       3_1|
# +--------+------+----+----------+

# Replicate the small dataset for each salt value
# Generate keys for all possible salt values and explode them

small_salted = small_dataset.withColumn("salt_array", array([lit(i) for i in range(salt_range)])) \
                            .withColumn("salt", explode(col("salt_array"))) \
                            .withColumn("salted_key", concat(col("join_key"), lit("_"), col("salt"))) \
                            .drop("salt_array")
print("Small Salted Dataset:")
small_salted.show()
# +--------+------------+----+----------+
# |join_key|  small_data|salt|salted_key|
# +--------+------------+----+----------+
# |       1|small_value1|   0|       1_0|
# |       1|small_value1|   1|       1_1|
# |       1|small_value1|   2|       1_2|
# |       2|small_value2|   0|       2_0|
# |       2|small_value2|   1|       2_1|
# |       2|small_value2|   2|       2_2|
# |       3|small_value3|   0|       3_0|
# |       3|small_value3|   1|       3_1|
# |       3|small_value3|   2|       3_2|
# +--------+------------+----+----------+

# Perform the join on the salted key
result = large_salted.join(small_salted, on=["salted_key", "join_key"], how="inner") \
                     .select("join_key", "data", "small_data")

# Show the result
print("Final Joined Dataset (De-salted):")
result.show()
# +--------+------+------------+
# |join_key|  data|  small_data|
# +--------+------+------------+
# |       1|value1|small_value1|
# |       1|value1|small_value1|
# |       1|value1|small_value1|
# |       2|value2|small_value2|
# |       3|value3|small_value3|
# +--------+------+------------+


# bucketBy and partitionBy

# Bucketing (Bucketing is not supported for Delta tables!!!)
# When you bucket a DataFrame, Spark uses a hash function on the specified column(s) to distribute the rows across a
# fixed number of buckets. Each bucket is stored as a file, and the data in each bucket is sorted based on
# the bucket column(s). Bucketing is particularly useful for operations like joins between large tables. When two
# tables are bucketed on the join column and have the same number of buckets, Spark can perform the join operation
# more efficiently without needing to shuffle data between nodes.
# Sample data for demonstration
data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Charlie", 26),
    (4, "David", 33),
    (5, "Eve", 29),
    (6, "Frank", 33)
]

columns = ["id", "name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Save the DataFrame as a bucketed table
bucketed_table = "bucketed_table"
# Bucketing by the column 'age' into 4 buckets
# Sorting within the buckets by the column 'name'
df.write \
  .bucketBy(4, "age") \
  .sortBy("name") \
  .format("parquet") \
  .saveAsTable(bucketed_table)

# Reading data from the bucketed table
bucketed_df = spark.sql(f"SELECT * FROM {bucketed_table}")
bucketed_df.show()

# Partitioning
# Partitioning in PySpark involves dividing data into separate directories (on HDFS, S3, etc.) based on the values in
# one or more columns. This is useful when working with large datasets, as it helps in reducing the amount of data
# read during queries, optimizing performance for operations like joins and aggregations.
#
# When you read a partitioned table, Spark automatically recognizes the partitions and loads them efficiently based on the query.
# If you filter on the partition column (e.g., department = 'Sales'), Spark will only load the required partitions
# instead of reading the full dataset, reducing I/O overhead.
data = [
    (1, "Alice", "Sales", 3000),
    (2, "Bob", "HR", 4000),
    (3, "Charlie", "Sales", 2500),
    (4, "David", "HR", 3000),
    (5, "Eve", "IT", 5000),
    (6, "Frank", "IT", 4500)
]

columns = ["id", "name", "department", "salary"]

# Creating a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the original DataFrame
print("Original DataFrame:")
df.show()

# Write the DataFrame to disk with partitioning by a column ('department')
output_path = "/tmp/partitioned_data"
# Partitioning by the 'department' column
df.write \
    .partitionBy("department") \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

# The data is saved with a directory structure like:
# /tmp/partitioned_data/department=Sales/...
# /tmp/partitioned_data/department=HR/...
# /tmp/partitioned_data/department=IT/...

print("Data has been written to disk, partitioned by the 'department' column.")

# Reading the partitioned data back
partitioned_df = spark.read.format("parquet").load(output_path)

print("Reading partitioned DataFrame:")
partitioned_df.show()


# DataFrameWriterV2
# from Spark version 3.5.0

data = [("John", 30), ("Jane", 25), ("Sam", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame to a table using DataFrameWriterV2 API
# create() - Create a new table from the contents of the data frame, we will get an error if the table already exists
# createOrReplace() - Create a new table or replace an existing table with the contents of the data frame.\
# using("delta") - We can omit for Databricks
df.writeTo("my_table") \
    .using("delta") \
    .createOrReplace()
# +----+---+
# |Name|Age|
# +----+---+
# |Jane| 25|
# |John| 30|
# | Sam| 35|
# +----+---+

data1 = [("Alex", 31)]
df1 = spark.createDataFrame(data1, columns)

# append() - Append the contents of the data frame to the output table
df1.writeTo("my_table") \
    .append()
# +----+---+
# |Name|Age|
# +----+---+
# |Jane| 25|
# |Alex| 31|
# |John| 30|
# | Sam| 35|
# +----+---+

# overwritePartitions() - Overwrite all partition for which the data frame contains at least one row with the contents of the data frame in the output table.
# If the table doesn't have any partitions all data will be overwriten with new data.
df1.writeTo("my_table") \
    .overwritePartitions()
# +----+---+
# |Name|Age|
# +----+---+
# |Alex| 31|
# +----+---+

data3 = [("John", "NY"), ("Jane", "LA"), ("Sam", "Boston")]
columns3 = ["Name", "City"]
df3 = spark.createDataFrame(data3, columns3)

# replace() - Replace an existing table with the contents of the data frame.
df3.writeTo("my_table") \
    .replace()
# +----+------+
# |Name|  City|
# +----+------+
# | Sam|Boston|
# |John|    NY|
# |Jane|    LA|
# +----+------+

data4 = [("John", "LA")]
df4 = spark.createDataFrame(data4, columns3)

# overwrite() - Overwrite rows matching the given filter condition with the contents of the data frame in the output table.
df4.writeTo("my_table") \
    .overwrite(col("Name") == "John")
# +----+------+
# |Name|  City|
# +----+------+
# | Sam|Boston|
# |Jane|    LA|
# |John|    LA|
# +----+------+

data5 = [("Anna", "Sales", 25), ("John", "IT", 30), ("Kate", "Sales", 27)]
columns5 = ["Name", "Department", "Age"]
df5 = spark.createDataFrame(data5, columns5)

# Partition the table by Department while writing
# partitionedBy() - Partition the output table created by create, createOrReplace, or replace using the given columns or transforms.
df5.writeTo("my_table") \
    .partitionedBy("Department") \
    .tableProperty("delta.logRetentionDuration", "interval 30 days") \
    .tableProperty("delta.deletedFileRetentionDuration", "interval 7 days") \
    .createOrReplace()
# +----+----------+---+
# |Name|Department|Age|
# +----+----------+---+
# |Anna|     Sales| 25|
# |Kate|     Sales| 27|
# |John|        IT| 30|
# +----+----------+---+

spark.sql(f"SHOW TBLPROPERTIES my_table").show()
# +--------------------+----------------+
# |                 key|           value|
# +--------------------+----------------+
# |delta.deletedFile...| interval 7 days|
# |delta.logRetentio...|interval 30 days|
# |delta.minReaderVe...|               1|
# |delta.minWriterVe...|               2|
# +--------------------+----------------+


# Pandas API and Pandas DataFrames with pyspark

# 1 .toPandas() - Returns the contents of this DataFrame as Pandas pandas.DataFrame. This method should only be used
# if the resulting Pandas pandas.DataFrame is expected to be small, as all the data is loaded into the driver’s memory.

df = get_pyspark_df()
pandas_df = df.toPandas()
print(pandas_df)
#       Name  Age
# 0    Alice   28
# 1      Bob   25
# 2  Charlie   30

# 2. Pandas API on Spark
# The pandas API on Spark allows users to run code written with the familiar pandas syntax in a distributed and
# scalable manner on an Apache Spark cluster. It was developed to address the limitation of standard pandas, which
# can only process data that fits into the memory of a single machine.
# API Coverage: While coverage is high, not all pandas APIs are supported, and some features, like specific pandas data
# types (e.g., Timedelta), are not yet fully implemented.
psdf = ps.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                    'b': [100, 200, 300, 400, 500, 600]})

# Use familiar pandas syntax
print(psdf.head())
#    a    b
# 0  1  100
# 1  2  200
# 2  3  300
# 3  4  400
# 4  5  500
print(psdf.describe())
#               a           b
# count  6.000000    6.000000
# mean   3.500000  350.000000
# std    1.870829  187.082869
# min    1.000000  100.000000
# 25%    2.000000  200.000000
# 50%    3.000000  300.000000
# 75%    5.000000  500.000000
# max    6.000000  600.000000

# 3.mapInPandas() - Maps an iterator of batches in the current DataFrame using a Python native function that is
# performed on pandas DataFrames both as input and output, and returns the result as a DataFrame. This can be useful
# if we need to pass a pandas dataframe to a specific library. Processing will be performed for each partition.

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.Age == 28]

df.mapInPandas(filter_func, df.schema).show()
# +-----+---+
# | Name|Age|
# +-----+---+
# |Alice| 28|
# +-----+---+

# 4. @pandas_udf - Pandas UDFs are user defined functions that are executed by Spark using Arrow to transfer data and
# Pandas to work with the data, which allows pandas operations.

# With @pandas_udf the model only needs to be loaded once per executor rather than once per batch during the inference
# process
# @pandas_udf("double")
# def predict(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
#     model_path = f"runs:/{run.info.run_id}/model"
#     model = mlflow.sklearn.load_model(model_path)
#     for features in iterator
#         pdf = pd.concat(features, axis=1)
#         yield pd.Series(model.predict(pdf))
#
# df.select(predict("features")).show()
