from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, split, col, lower, trim, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize a SparkSession
spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()

# Read plain text file into a DataFrame (lines are in the 'value' column)
# df = spark.read.text("example.txt")
# Or read plain text into RDD
# rdd = spark.sparkContext.textFile("example.txt")

# or create test data
data = [
    ("""Hello World
        Spark is amazing
        Big Data is here
        PySpark is fun
        Spark and Big Data""", 1)
    ]
schema = StructType([
    StructField("value", StringType(), True),
    StructField("id", IntegerType(), True)
])
df = spark.createDataFrame(data, schema=schema)

# An additional method to create test data
string_data = """Hello World
                 Spark is amazing
                 Big Data is here
                 PySpark is fun
                 Spark and Big Data"""
data_list = string_data.split(",")
schema = StructType([StructField("value", StringType(), True)])
rdd = spark.sparkContext.parallelize(data_list).map(lambda x: Row(x))
df = spark.createDataFrame(rdd, schema)

# Show the input DataFrame
print("=== Input DataFrame ===")
df.show(truncate=False)

# Step 1: Split lines into words
# - Use `split()` to split each line into an array of words.
# - Use `explode()` to flatten the array into individual rows (one word per row).
words_df = df.select(
    explode(
        split(col("value"), r"\s+")  # Split on one or more spaces
    ).alias("word")
)

# Show the DataFrame with words
print("=== Words DataFrame ===")
words_df.show(truncate=False)

# Step 2: Normalize the words
# - Trim whitespace and convert to lowercase for consistency.
normalized_words_df = words_df.select(
    lower(trim(col("word"))).alias("word")
)

# Show the normalized words DataFrame
print("=== Normalized Words DataFrame ===")
normalized_words_df.show(truncate=False)

# Step 3: Count the occurrences of each word
# - Group by the "word" column and apply the `count()` aggregation.
word_count_df = normalized_words_df.groupBy("word").agg(
    count("word").alias("count")
)

# Sort the results in descending order of the word count
sorted_word_count_df = word_count_df.orderBy(col("count").desc())

# Show the word count DataFrame
print("=== Word Count DataFrame ===")
sorted_word_count_df.show(truncate=False)

# Optionally, save the word count to a file
# sorted_word_count_df.write.csv("word_count_output", header=True)
