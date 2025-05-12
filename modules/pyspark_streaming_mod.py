# PySpark Structured Streaming is a scalable and fault-tolerant stream-processing engine built on the Spark SQL engine.
# Unlike traditional streaming, it treats streaming data as a continuous flow of “append-only” table rows.
# This abstraction simplifies stream processing while leveraging rich SQL-like transformations and operations.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, session_window, window, explode, split, expr

# Create SparkSession
spark = SparkSession.builder.appName("StructuredStreamingExample").master("local").getOrCreate()


# Input Sources (DataStream Reader)
# PySpark Structured Streaming supports reading data from multiple streaming sources.
# The readStream method is used to define a DataStream reader.

# File Source
streaming_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema("Name STRING, Age INT") \
    .load("path_to_directory")

# Kafka Source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .load()

# Socket Source
socket_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Rate source (for testing purposes)
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
# +--------------------+-----+
# |           timestamp|value|
# +--------------------+-----+
# |2025-05-05 21:04:...|  230|
# |2025-05-05 21:04:...|  238|
# |2025-05-05 21:04:...|  231|
# |2025-05-05 21:04:...|  239|
# |2025-05-05 21:04:...|  232|
# |2025-05-05 21:04:...|  233|
# |2025-05-05 21:04:...|  234|
# |2025-05-05 21:04:...|  235|
# |2025-05-05 21:04:...|  236|
# |2025-05-05 21:04:...|  237|
# +--------------------+-----+


# Transformations
# Structured Streaming allows DataFrame-like transformations on streaming DataFrames.

# Select Columns
select_df = rate_df.select("value")

# Filter Rows
filtered_df = rate_df.filter(streaming_df.value > 230)

# Stateful Operations (aggregation, streaming dropDuplicates, stream-stream joins, and custom stateful applications)
# GroupBy and Aggregation
agg_df = rate_df.groupBy("value").agg(count("value").alias("Count"))

# Deduplication
# Without watermark using guid column
rate_df.dropDuplicates("timestamp")

# With watermark using the timestamp column
rate_df \
  .withWatermark("timestamp", "10 seconds") \
  .dropDuplicates("timestamp")

# JOINS
# Since the introduction in Spark 2.0, Structured Streaming has supported joins (inner join and some type of outer joins)
# between a streaming and a static DataFrame/Dataset.
staticDf = spark.createDataFrame([("Alice", 28), ("Bob", 25), ("Charlie", 30)], schema=["Name", "value"])
streamingDf = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
inner_join_df = streamingDf.join(staticDf, staticDf.value == streamingDf.value)  # inner join
left_join_df = streamingDf.join(staticDf, staticDf.value == streamingDf.value, "left_outer")  # left outer join

# In Spark 2.3, added support for stream-stream joins, that is, you can join two streaming Datasets/DataFrames.
# Both streams should have event-time columns.
# Both streams must have watermarks defined to bound state that accumulates due to events arriving late.
streaming_df_first = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
streaming_df_second = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# Add Watermarking to Both Streams (Handle Late Events)
# Allows Spark to bound state for stream-to-stream joins.
streaming_df_first = streaming_df_first.withWatermark("timestamp", "10 minutes")
streaming_df_second = streaming_df_second.withWatermark("timestamp", "10 minutes")

# Perform the Join
joined_stream = streaming_df_first.join(streaming_df_second, streaming_df_first.value == streaming_df_second.value)

# Output the Joined Stream
query = joined_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
# +--------------------+-----+--------------------+-----+
# |           timestamp|value|           timestamp|value|
# +--------------------+-----+--------------------+-----+
# |2025-05-11 15:07:...|  964|2025-05-11 15:07:...|  964|
# |2025-05-11 15:07:...| 1010|2025-05-11 15:07:...| 1010|
# |2025-05-11 15:07:...|  938|2025-05-11 15:07:...|  938|
# |2025-05-11 15:07:...|  926|2025-05-11 15:07:...|  926|
# |2025-05-11 15:07:...|  965|2025-05-11 15:07:...|  965|
# |2025-05-11 15:07:...| 1077|2025-05-11 15:07:...| 1077|
# |2025-05-11 15:07:...| 1055|2025-05-11 15:07:...| 1055|
# |2025-05-11 15:07:...| 1042|2025-05-11 15:07:...| 1042|
# +--------------------+-----+--------------------+-----+

# You can also register a streaming DataFrame/Dataset as a temporary view and then apply SQL commands on it.
rate_df.createOrReplaceTempView("updates")
df = spark.sql("select count(*) from updates")  # returns another streaming DF

# Output Sink (DataStream Writer)
# Structured Streaming requires you to specify an output sink to write the results of the stream processing,
# using the writeStream method.
#
# Supported Output Modes:
# - Append Mode: Only newly added rows are written to the sink.
# - Complete Mode: The entire output, including all rows, is updated in the sink.
# - Update Mode: Only rows updated since the last batch are written to the sink.
#
# The trigger settings of a streaming query define the timing of streaming data processing (micro-batch size)
# Supported Trigger Modes:
# - processingTime - The query will be executed with micro-batches mode, where micro-batches will be kicked off at
# the user-specified intervals. If the previous micro-batch takes longer than the interval to complete,
# then the next micro-batch will start as soon as the previous one completes
# Example: processingTime='1 second'
# - once=True (deprecated) - The query will execute only one micro-batch to process all the available data and then stop
# - availableNow - Similar to queries one-time micro-batch trigger, the query will process all the available data
# and then stop on its own. It will process the data in (possibly) multiple micro-batches based on the source options
# (e.g. maxFilesPerTrigger for file source)
# Example: availableNow=True
# - continuous (experimental) - The query will be executed in the new low-latency, continuous processing mode.
# Example: continuous='1 second'
#
# Checkpointing is essential for recovery and fault tolerance for streaming applications. PySpark uses a checkpoint
# directory to persist intermediate results. The system ensures end-to-end exactly-once fault-tolerance guarantees
# through checkpointing and Write-Ahead Logs.


# Console Sink (for testing purposes)
# trigger 1 sec
query = rate_df.writeStream \
    .trigger(processingTime="1 second") \
    .format("console") \
    .outputMode("append") \
    .start()
# +--------------------+-----+
# |           timestamp|value|
# +--------------------+-----+
# |2025-05-08 14:59:...|  180|
# |2025-05-08 14:59:...|  188|
# |2025-05-08 14:59:...|  181|
# |2025-05-08 14:59:...|  189|
# |2025-05-08 14:59:...|  182|
# |2025-05-08 14:59:...|  183|
# |2025-05-08 14:59:...|  184|
# |2025-05-08 14:59:...|  185|
# |2025-05-08 14:59:...|  186|
# |2025-05-08 14:59:...|  187|
# +--------------------+-----+

# trigger 2 sec
query = rate_df.writeStream \
    .trigger(processingTime="2 second") \
    .format("console") \
    .outputMode("append") \
    .start()
# +--------------------+-----+
# |           timestamp|value|
# +--------------------+-----+
# |2025-05-08 14:59:...|   90|
# |2025-05-08 14:59:...|   98|
# |2025-05-08 14:59:...|  106|
# |2025-05-08 14:59:...|   91|
# |2025-05-08 14:59:...|   99|
# |2025-05-08 14:59:...|  107|
# |2025-05-08 14:59:...|   92|
# |2025-05-08 14:59:...|  100|
# |2025-05-08 14:59:...|  108|
# |2025-05-08 14:59:...|   93|
# |2025-05-08 14:59:...|  101|
# |2025-05-08 14:59:...|  109|
# |2025-05-08 14:59:...|   94|
# |2025-05-08 14:59:...|  102|
# |2025-05-08 14:59:...|   95|
# |2025-05-08 14:59:...|  103|
# |2025-05-08 14:59:...|   96|
# |2025-05-08 14:59:...|  104|
# |2025-05-08 14:59:...|   97|
# |2025-05-08 14:59:...|  105|
# +--------------------+-----+

# File Sink
query = rate_df.writeStream \
    .format("csv") \
    .option("path", "output_directory") \
    .option("checkpointLocation", "checkpoint_directory") \
    .outputMode("append") \
    .start()

# Kafka Sink
query = rate_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .option("checkpointLocation", "checkpoint_directory") \
    .outputMode("append") \
    .start()


# Window-Based Operations
# Structured Streaming allows performing windowed aggregations to process events that arrive within
# specific time intervals.

# Tumbling Window
# Tumbling windows are a series of fixed-sized, non-overlapping and contiguous time intervals.
# An input can only be bound to a single window.

tumbling_windowed_df = rate_df.groupBy(
    window(rate_df.timestamp, "10 minutes")
).agg(count("value"))

# Sliding Window
# Sliding windows are similar to the tumbling windows from the point of being “fixed-sized”, but windows can overlap
# if the duration of slide is smaller than the duration of window, and in this case an input can be bound
# to the multiple windows.

sliding_windowed_df = rate_df.groupBy(
    window(rate_df.timestamp, "10 minutes", "5 minutes")
).count()

# Session Window
# Session window has a dynamic size of the window length, depending on the inputs. A session window starts with an input,
# and expands itself if following input has been received within gap duration.
# The session window is defined based on a timeout, e.g., 10 seconds

session_windowed_df = rate_df.groupBy(
    session_window(rate_df.timestamp, "10 seconds")
).count()


# Watermarking
# Watermarking is used for late data handling by specifying maximum allowed lateness in event time.
watermarked_df = rate_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes")) \
    .count()


# Streaming Table APIs
# Since Spark 3.1, we can also use DataStreamReader.table() to read tables as streaming DataFrames and use
# DataStreamWriter.toTable() to write streaming DataFrames as tables:

# Example:
# Create a streaming DataFrame
df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

# Write the streaming DataFrame to a table
df.writeStream \
    .option("checkpointLocation", "path/to/checkpoint/dir") \
    .toTable("myTable")

# Check the table result
spark.read.table("myTable").show()
# +--------------------+-----+
# |           timestamp|value|
# +--------------------+-----+
# |2025-05-11 13:48:...| 3130|
# |2025-05-11 13:48:...| 3138|
# |2025-05-11 13:48:...| 3146|
# |2025-05-11 13:48:...| 3154|
# |2025-05-11 13:48:...| 3162|
# |2025-05-11 13:48:...| 3170|
# +--------------------+-----+

# Transform the source dataset and write to a new table
# Checkpoint locations for streams should be different!!!
spark.readStream \
    .table("myTable") \
    .select("value") \
    .writeStream \
    .option("checkpointLocation", "path/to/checkpoint/dir1") \
    .format("parquet") \
    .toTable("newTable")

# Check the new table result
spark.read.table("newTable").show()
# +-----+
# |value|
# +-----+
# |    3|
# |   11|
# |   19|
# |   27|
# |   35|
# |   43|
# +-----+

# Streaming Query Management
active_queries = spark.streams.active
print([query.name for query in active_queries])

# Query manipulation
query = rate_df.writeStream \
    .trigger(processingTime="1 second") \
    .format("console") \
    .outputMode("append") \
    .start()

query.explain()            # to print detailed explanations of the query
query.stop()               # to stop the query
query.awaitTermination()   # to block until query is terminated, with stop() or with error,
                           # without this command the code after will continue to execute
query.exception()          # to throw the exception if the query has been terminated with error
rate_df.isStreaming()      # to check the whether a df has streaming data or not


# Example: End-to-End Word Count
# Word Count from rate
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

words = rate_df.select(
    explode(split(rate_df.timestamp, " ")).alias("timestamp")
)

# Count occurrences of each word
word_counts = words.groupBy("timestamp").count()

# Output to the console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
# +------------+-----+
# |   timestamp|count|
# +------------+-----+
# |11:03:06.253|    1|
# |11:03:05.853|    1|
# |11:03:06.053|    1|
# |11:03:05.753|    1|
# |11:03:05.453|    1|
# |11:03:05.553|    1|
# |11:03:05.953|    1|
# |11:03:06.353|    1|
# |11:03:05.653|    1|
# |11:03:06.153|    1|
# |  2025-05-11|   10|
# +------------+-----+
