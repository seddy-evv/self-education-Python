# Apache PySpark is a powerful library for distributed computing using Python, built on top of Apache Spark.
# It allows handling large-scale data across multiple nodes and provides a rich set of functions for data processing.
# Below is a description and examples of some of the main PySpark functions:

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, sum, max, concat, lit, expr, create_map, to_date, to_timestamp, \
    concat_ws, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd

# Create SparkSession
spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()


# PySpark RDD Operations

# Create RDD
# # Create SparkContext
# from pyspark import SparkConf, SparkContext
# conf = SparkConf().setAppName("PySpark Examples").setMaster("local")
# sc = SparkContext(conf=conf)
# # from a text file
# rdd = sc.textFile("/path/textfile.txt")
# # from a CSV file
# rdd = sc.textFile("/path/csvfile.csv")
# # from a JSON file
# import json
# rddFromJson = sc.textFile("/path/to/your/jsonfile.json").map(json.loads)
# # from an HDFS file
# rddFromHdfs = sc.textFile("hdfs://localhost:9000/path/to/your/file")
# # from a Sequence file
# rddFromSequenceFile = sc.sequenceFile("/path/to/your/sequencefile")

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


# Basic PySpark DataFrame Operations

data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df = spark.createDataFrame(data, schema=["Name", "Age"])

# Collect: Retrieve all data
data = df.collect()
print(data)
# [Row(Name='Alice', Age=28), Row(Name='Bob', Age=25), Row(Name='Charlie', Age=30)]

# Count: Count the number of rows
count = df.count()
print(count)
# 3

# Show: Display the DataFrame
df.show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |    Bob| 25|
# |Charlie| 30|
# +-------+---+

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

# Select Columns
# We can specify the colum name in two ways
df.select("Name").show()
# or
df.select(df.Name).show()
# +-------+
# |   Name|
# +-------+
# |  Alice|
# |    Bob|
# |Charlie|
# +-------+

# Filter Rows
df.filter(df.Age > 26).show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |  Alice| 28|
# |Charlie| 30|
# +-------+---+

# Order rows
df.orderBy(df["Age"].desc()).show()
# +-------+---+
# |   Name|Age|
# +-------+---+
# |Charlie| 30|
# |  Alice| 28|
# |    Bob| 25|
# +-------+---+

# Add New Column
df.withColumn("AgePlusOne", col("Age") + lit(1)).show()
# +-------+---+----------+
# |   Name|Age|AgePlusOne|
# +-------+---+----------+
# |  Alice| 28|        29|
# |    Bob| 25|        26|
# |Charlie| 30|        31|
# +-------+---+----------+

# Replace Column Values
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

data = [("Alice", 2000), ("Bob", 3000), ("Charlie", 4000), ("Bob", 4000)]
df3 = spark.createDataFrame(data, schema=["Name", "Salary"])
aggregated_df = df3.groupBy("Name").agg(sum("Salary").alias("Salary sum"), max("Salary"))
aggregated_df.show()
# +-------+----------+-----------+
# |   Name|Salary sum|max(Salary)|
# +-------+----------+-----------+
# |  Alice|      2000|       2000|
# |    Bob|      7000|       4000|
# |Charlie|      4000|       4000|
# +-------+----------+-----------+

# Drop columns
data = [("Alice", 28, "Amazon"), ("Bob", 25, "Google"), ("Charlie", 30, "Oracle")]
df1 = spark.createDataFrame(data, schema=["Name", "Age", "Company"])
df1.drop("Name", "Age").show()
# +-------+
# |Company|
# +-------+
# | Amazon|
# | Google|
# | Oracle|
# +-------+

# Drop rows with null values
df_null = spark.createDataFrame([(None, None), ("1", None), (None, 2), ("3", 3)], ("a", "b"))
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

# Limit rows
df1.limit(1).show()
# +-----+---+-------+
# | Name|Age|Company|
# +-----+---+-------+
# |Alice| 28| Amazon|
# +-----+---+-------+

# Repartition and coalesce
# repartition() - Returns a new DataFrame partitioned by the given partitioning expressions.
# The resulting DataFrame is hash partitioned. The size of partitions can be greater or less than the original.
# coalesce() - Returns a new DataFrame that has exactly numPartitions partitions. If a larger number of partitions
# is requested, it will stay at the current number of partitions.
print(df.repartition(3).rdd.getNumPartitions())  # 3
print(df.coalesce(1).rdd.getNumPartitions())  # 1

# Union
data = [("Alex", 38, "Microsoft"), ("John", 35, "Netflix")]
df2 = spark.createDataFrame(data, schema=["Name", "Age", "Company"])
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


# PySpark SQL

# Register Temp View and Execute SQL
data = [("Alice", 28), ("Bob", 25), ("Charlie", 30)]
df_sql = spark.createDataFrame(data, schema=["Name", "Age"])
df_sql.createOrReplaceTempView("people")

data = [("Alice", "New York"), ("Bob", "San Francisco"), ]
df_sql = spark.createDataFrame(data, schema=["Name", "City"])
df_sql.createOrReplaceTempView("cities")

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

# PySpark DataFrame Joins

data1 = [("Alice", 28), ("Bob", 25)]
data2 = [("Alice", "F"), ("Bob", "M"), ("Charlie", "M")]
df1 = spark.createDataFrame(data1, ["Name", "Age"])
df2 = spark.createDataFrame(data2, ["Name", "Gender"])

# Inner Join
inner_join = df1.join(df2, on="Name", how="inner")
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


# PySpark File I/O

# Write DataFrame as CSV
df.write.csv("output.csv", header=True)

# Read CSV File
df = spark.read.csv("output.csv", header=True, inferSchema=True)
df.show()

# Read and Write Parquet
df.write.parquet("output.parquet")
df_parquet = spark.read.parquet("output.parquet")
df_parquet.show()


# PySpark Functions Module

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
# to_timestamp() - Converts a Column into pyspark.sql.types.TimestampType using the optionally specified format.
data = [("Alice", 28, '1997-02-28 10:30:00'), ("Bob", 25, '2000-02-28 10:30:00'), ("Charlie", 30, '2005-02-28 10:30:00')]
df_data = spark.createDataFrame(data, schema=["Name", "Age", "Date"])
df_data.select("Name", expr("length(name)").alias("lenght of the name"), to_date("Date"), to_timestamp("Date"),
               col("Date").cast("date")).show()
# +-------+------------------+-------------+-------------------+----------+
# |   Name|lenght of the name|to_date(Date)| to_timestamp(Date)|      Date|
# +-------+------------------+-------------+-------------------+----------+
# |  Alice|                 5|   1997-02-28|1997-02-28 10:30:00|1997-02-28|
# |    Bob|                 3|   2000-02-28|2000-02-28 10:30:00|2000-02-28|
# |Charlie|                 7|   2005-02-28|2005-02-28 10:30:00|2005-02-28|
# +-------+------------------+-------------+-------------------+----------+

# create_map() - The create_map() function in Apache Spark is popularly used to convert the selected or all the
# concat_ws() - Concatenates multiple input string columns together into a single string column, using the given separator.
# DataFrame columns to the MapType, similar to the Python Dictionary (Dict) object.
df.select(create_map('Name', 'Age').alias("map"), concat_ws(' is ', 'Name', 'Age').alias('concat string')).show()
# +---------------+-------------+
# |            map|concat string|
# +---------------+-------------+
# |  {Alice -> 28}|  Alice is 28|
# |    {Bob -> 25}|    Bob is 25|
# |{Charlie -> 30}|Charlie is 30|
# +---------------+-------------+

# coalesce() - Returns the first column that is not null or the default value
df_null = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
df_null.select(coalesce(df_null["a"], df_null["b"])).show()
# +--------------+
# |coalesce(a, b)|
# +--------------+
# |          null|
# |             1|
# |             2|
# +--------------+

df_null.select('*', coalesce(df_null["a"], lit(0.0))).show()
# +----+----+----------------+
# |   a|   b|coalesce(a, 0.0)|
# +----+----+----------------+
# |null|null|             0.0|
# |   1|null|             1.0|
# |null|   2|             0.0|
# +----+----+----------------+

# Window Functions
data = (("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Maria", "Finance", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3300),
        ("Saif", "Sales", 4100)
        )
schema = ["employee_name", "department", "salary"]
df_window = spark.createDataFrame(data=data, schema=schema)
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

# row_number, rank, dense_rank, percent_rank
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
