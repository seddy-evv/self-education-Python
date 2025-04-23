# Apache PySpark is a powerful library for distributed computing using Python, built on top of Apache Spark.
# It allows handling large-scale data across multiple nodes and provides a rich set of functions for data processing.
# Below is a description and examples of some of the main PySpark functions:

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, sum, max, concat, lit, expr, create_map, to_date, to_timestamp, \
    concat_ws, coalesce, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, avg, min, udf, \
    current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
import pandas as pd
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
    data = [("Alice", 2000), ("Bob", 3000), ("Charlie", 4000), ("Bob", 4000)]
    df3 = spark.createDataFrame(data, schema=["Name", "Salary"])

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

def get_pyspark_df_date():
    data = [("Alice", 28, '1997-02-28 10:30:00'), ("Bob", 25, '2000-02-28 10:30:00'),
            ("Charlie", 30, '2005-02-28 10:30:00')]
    df_date = spark.createDataFrame(data, schema=["Name", "Age", "Date"])

    return df_date

def get_pyspark_df_window():
    data = (("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Maria", "Finance", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3300),
            ("Saif", "Sales", 4100)
            )
    schema = ["employee_name", "department", "salary"]
    df_window = spark.createDataFrame(data=data, schema=schema)

    return df_window

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

df = get_pyspark_df()

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

# select distinct
df.select("Name").distinct()
# +-------+
# |   Name|
# +-------+
# |  Alice|
# |    Bob|
# |Charlie|
# +-------+

# Filter Rows, & - and, | - or, ~ - not, don't forget brackets!
df.filter((df.Age > 26) & (df.Age != 30)).show()
# +-----+---+
# | Name|Age|
# +-----+---+
# |Alice| 28|
# +-----+---+

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

df3 = get_pyspark_df3()
aggregated_df = df3.groupBy("Name").agg(sum("Salary").alias("Salary sum"), max("Salary"))
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
# repartition() - Returns a new DataFrame partitioned by the given partitioning expressions.
# The resulting DataFrame is hash partitioned. The size of partitions can be greater or less than the original.
# coalesce() - Returns a new DataFrame that has exactly numPartitions partitions. If a larger number of partitions
# is requested, it will stay at the current number of partitions.
print(df.repartition(3).rdd.getNumPartitions())  # 3
print(df.coalesce(1).rdd.getNumPartitions())  # 1

# Union, how to add new rows to existing Dataframe
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


# PySpark SQL

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

# PySpark DataFrame Joins

df1 = get_pyspark_df_join1()
df2 = get_pyspark_df_join2()

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
current_time = time.time()
df = get_pyspark_df()

# Write DataFrame as CSV
df.write.csv(f"/output-{current_time}.csv", header=True)

# Read CSV File with the schema
df = spark.read.csv(f"/output-{current_time}.csv", header=True, inferSchema=True)
df.show()

# Read and Write Parquet
df.write.parquet(f"/output-{current_time}.parquet")
df_parquet = spark.read.parquet(f"/output-{current_time}.parquet")
df_parquet.show()

# Read and write JSON
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


# PySpark Functions Module
df = get_pyspark_df()

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
# current_date() - get current date
df_date = get_pyspark_df_date()
df_date.select("Name", expr("length(name)").alias("lenght of the name"), to_date("Date"), to_timestamp("Date"),
               col("Date").cast("date"), current_date()).show()
# +-------+------------------+-------------+-------------------+----------+--------------+
# |   Name|lenght of the name|to_date(Date)| to_timestamp(Date)|      Date|current_date()|
# +-------+------------------+-------------+-------------------+----------+--------------+
# |  Alice|                 5|   1997-02-28|1997-02-28 10:30:00|1997-02-28|    2025-04-22|
# |    Bob|                 3|   2000-02-28|2000-02-28 10:30:00|2000-02-28|    2025-04-22|
# |Charlie|                 7|   2005-02-28|2005-02-28 10:30:00|2005-02-28|    2025-04-22|
# +-------+------------------+-------------+-------------------+----------+--------------+

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
df_null = get_pyspark_df_null()
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

# udf() - create and use an udf, user memory is in use
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

df.withColumn("ntile", ntile(2).over(window_spec)).show()
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
df.withColumn("cume_dist", cume_dist().over(window_spec)).show()
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

df.withColumn("lag", lag("salary", 2).over(window_spec)).show()
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

df.withColumn("lead", lead("salary", 2).over(window_spec)).show()
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

# avg, sum, min, max
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
