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
