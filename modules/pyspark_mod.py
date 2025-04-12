# Apache PySpark is a powerful library for distributed computing using Python, built on top of Apache Spark.
# It allows handling large-scale data across multiple nodes and provides a rich set of functions for data processing.
# Below is a description and examples of some of the main PySpark functions:

from pyspark.sql import SparkSession

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
