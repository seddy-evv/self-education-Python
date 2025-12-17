from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Databricks Examples").master("local").getOrCreate()

# READ AND WRITE TO DELTA TABLE (for Databricks we can omit delta format option):
# Create df
data = [
  ("Alice", 28, 1990),
  ("Bob", 25, 1991),
  ("Charlie", 30, 1991)
  ]

schema = ["Name", "Age", "Year"]
df = spark.createDataFrame(data, schema=schema)

updates_data = [
  ("Alice", 28, 1990),
  ("Bob", 25, 1991),
  ("Charlie", 30, 1991)
  ]
updates_df = spark.createDataFrame(updates_data, schema=schema)

replace_data = [
  ("Alex", 30, 1990)
  ]

replace_df = spark.createDataFrame(replace_data, schema=schema)

# Create an empty delta table in the Hive metastore
spark.sql("""
    CREATE TABLE my_table
    (
        Name STRING NOT NULL,
        Age INT,
        Year INT
    )
    USING DELTA
""")

# Create a table using CTAS and the existing table
# CTAS do not support manual schema declaration, instead TABLE might be VIEW, TEMP VIEW, GLOBAL TEMP VIEW
# The my_table table was MANAGED, the my_table_new is EXTERNAL due to LOCATION declaration
spark.sql("""
    CREATE TABLE my_table_new
    COMMENT 'Contains names and years'
    PARTITIONED BY (Year)
    LOCATION 'some/path'
    AS SELECT Name, Year FROM my_table
""")
# CTAS from file:
spark.sql("""
    CREATE TABLE my_table_file
    AS SELECT * FROM json.`/path/file_name.json`
""")

# We also can just select from file
spark.sql("""
    SELECT * FROM json.`/path/file_name.json`
""")

# Create an external table from files (the table will not be Delta)
spark.sql("""
    CREATE TABLE table_name
    (
        Name STRING NOT NULL,
        Age INT,
        Year INT
    )
    USING CSV
    OPTIONS (header = 'true', delimiter = ';')
    LOCATION = 'some/path'
""")
# In case if we need to create a Delta table with specific schema using data from the external files
spark.sql("""
    CREATE TEMP VIEW view_name
    (
        Name STRING NOT NULL,
        Age INT,
        Year INT
    )
    USING CSV
    OPTIONS (header = 'true', delimiter = ';', path = 'some/path')
    
    CRATE TABLE table_name
    AS SELECT * FROM view_name
""")

# Write
# .option("mergeSchema", "true") - allows to enable schema evolution, and we can add additional columns with append mode
# .option("overwriteSchema", "true")  - allows add partitions and replace the schema, we should use it with overwrite mode
# .mode() - append, overwrite, error(default), ignore.
# .clusterBy() - Databricks feature from Runtime 15.2 and above, can be used with .option(“clusterByAuto”, “true”) or
# just Auto. Clustering is not compatible with partitioning or ZORDER.
# replaceWhere() - option atomically replaces all records that match a given predicate.
# create delta managed table in the Hive metastore catalog
df.write.format("delta") \
    .mode("append") \
    .clusterBy("Year") \
    .option("mergeSchema", "true").saveAsTable("my_table")
# create delta external table in the Hive metastore catalog, path might be cloud storage url
df.write.format("delta") \
    .mode("append") \
    .partitionBy("Year") \
    .option("path", "path/to/delta_table") \
    .option("mergeSchema", "true").saveAsTable("my_table")
# or just save delta table without creation any tables in the Hive metastore catalog
df.write.format("delta") \
    .mode("append") \
    .partitionBy("Year") \
    .option("mergeSchema", "true").save("path/to/delta_table")
# if we need to create external table in the Hive metastore based on the table above:
spark.sql("CREATE TABLE p USING DELTA LOCATION 'path/to/delta_table'")


# replace records in the my_table, all records in the replace_df should be with Year = 1990 otherwise
# this operation fails with an error, also it's better that the column Year be partition column
(replace_df.write
 .mode("overwrite")
 .option("replaceWhere", "Year = 1990")
 .saveAsTable("my_table")
 )

# Read
df = spark.read.format("delta").load("path/to/delta_table")
# or
df = spark.table("my_table")

# Delta table as a streaming source
df = spark.readStream \
      .format("delta") \
      .schema(schema) \
      .table("source_table")
# or
df = spark.readStream \
      .format("delta") \
      .schema(schema) \
      .load("/delta/events")

# Delta table as a streaming sink
# outputMode and trigger check pyspark_mod.py
df.writeStream.format("delta") \
 .outputMode("append") \
 .option("checkpopintLocation", "path/to/checkpoints") \
 .trigger(once=True).table("my_table")


# CONVERT PARQUET TO DELTA LAKE
# Convert Parquet table to Delta Lake format in place
delta_table = DeltaTable.convertToDelta(spark, "parquet.`/path/to/parquet_table`")
partitioned_delta_table = DeltaTable.convertToDelta(spark, "parquet.`/path/to/parquet_table`", "part int")
# or
spark.sql("CONVERT TO DELTA parquet.`/path/to/table` [PARTITIONED BY (col_name1 col_type1, col_name2 col_type2)]")


# WORKING WITH DELTA TABLES
# A DeltaTable is the entry point for interacting with tables programmatically in Python
delta_table = DeltaTable.forName(spark, "my_table")
# or
delta_table = DeltaTable.forPath(spark, "delta.`path/to/table`")
# or empty Delta table
DeltaTable.create() \
 .tableName("my_table") \
 .addColumn("Name", dataType="STRING") \
 .addColumn("Age", dataType="INT") \
 .addColumn("Year", dataType="INT") \
 .execute()

# DELTA LAKE DDL/DML: UPDATES, DELETES, INSERTS, MERGES
# Delete rows:
delta_table.delete("date < '2017-01-01'")
# or
delta_table.delete(col("date") < "2017-01-01")
# or
spark.sql("DELETE FROM tableName WHERE date < '2017-01-01'")

# Update rows:
delta_table.update(condition="eventType = 'clk'", set={"eventType": "'click'"})
# or
delta_table.update(condition=col("eventType") == "clk", set={"eventType": lit("click")})
# or
spark.sql("UPDATE my_table SET event = 'click' WHERE event = 'clk'")

# Insert values directly into table
spark.sql("""
          INSERT INTO TABLE my_table VALUES 
              (8003, "Kim", "2023-01-01", 3),
              (8004, "Tim", "2023-01-01", 4)
          """)

# Insert using SELECT statement
spark.sql("INSERT INTO my_table SELECT * FROM sourceTable")

# Replace all data in the table with new values
spark.sql("""INSERT OVERWRITE my_table VALUES
                (8003, "Kim", "2023-01-01", 3),
                (8004, "Tim", "2023-01-01", 4)
          """)

# Upsert (update + insert) using MERGE INTO
# Available options:
# .whenMatchedUpdate(), .whenMatchedUpdateAll(), .whenNotMatchedInsert(), .whenMatchedDelete()
(delta_table.alias("target").merge(
    source=updates_df.alias("updates"),
    condition="target.eventId = updates.eventId")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsert(
    values={
        "date": "updates.date",
        "eventId": "updates.eventId",
        "data": "updates.data",
        "count": 1
    }
).execute()
 )
# or
spark.sql("""
          MERGE INTO target
          USING updates
          ON target.Id = updates.Id
          WHEN MATCHED AND target.delete_flag = "true" THEN
            DELETE
          WHEN MATCHED THEN
            UPDATE SET * -- star notation means all columns
          WHEN NOT MATCHED THEN
            INSERT (date, Id, data) -- or, use INSERT *
            VALUES (date, Id, data)
          """)

# Insert with deduplication using MERGE
(delta_table.alias("logs").merge(
    updates_df.alias("newDedupledLogs"),
    "logs.uniqueId = newDedupledLogs.uniqueId")
 .whenNotMatchedInsertAll()
 .execute()
 )
# or
spark.sql("""
          MERGE INTO logs
          USING newDedupledLogs
          ON logs.eventId = newDedupledLogs.eventId
          WHEN NOT MATCHED 
            THEN INSERT *
          """)

# Incremental Data Ingestion - loading new data files encountered since the last ingestion
# 1. Copy new data into Delta table (with idempotent retries)
spark.sql("""
          COPY INTO [dbName.] targetTable
          FROM (SELECT * FROM "/path/to/table")
          FILEFORMAT = DELTA -- or CSV, Parquet, ORC, JSON
          """)
# 2. Auto Loader - more efficient than COPY INTO, support near real-time ingestion of millions of files per hour
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "<source_format>") \
    .option("cloudFiles.schemaLocation", "<schema_directiry>") \
    .load("/path/to/data") \
    .writeStream \
    .option("checkpointLocation", "<checkpoint_directory>") \
    .option("mergeSchema", "true") \
    .table("my_table")

# CDC: Process of identifying changes made to data in the source and delivering those changes to the target
# (Insert, Delete, Update), useful if streaming source table is not append only.
# APPLY CHANGES INTO - we can use within DLT pipelines to implement CDC, specifying keys, sequence on a streaming
# target table, simplifying complex merges for streaming data.
spark.sql("""
          APPLY CHANGES INTO LIVE.target_table
          FROM STREAM(LIVE.cdc_feed_table)
          KEYS (key_field)
          APPLY AS DELETE WHEN operating_filed = 'DELETE'
          SEQUENCE BY sequence_field
          COLUMNS *
          """)
# AUTO CDC: The AUTO CDC APIs replace the APPLY CHANGES APIs, and have the same syntax.

# When to APPLY CHANGES INTO AND AUTO CDC vs. MERGE INTO
# APPLY CHANGES INTO / AUTO CDC: Recommended for streaming CDC in DLT, handles complexity (duplicates, out-of-order data) automatically.
# MERGE INTO: More flexible for batch, but requires manual logic for streaming CDC, error handling, and deduplication, leading to higher complexity and potential errors.

# Alter table schema - add columns, but sometimes it's not allowed for the table
spark.sql("""
          ALTER TABLE tableName ADD COLUMNS (
              col_name data_type
              [FIRST|AFTER col_a]
          )
          """)

# Alter table schema - drop column, this operation might be not supported for your Delta table
# In this case you need to specify properties below, but che the project documentation first
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
          "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')")
spark.sql("ALTER TABLE my_table DROP COLUMN new_column")

# Alter table - add constraint
# Add "Not NULL" constraint:
spark.sql("ALTER TABLE my_table CHANGE COLUMN col_name SET NOT NULL")
# Add "check" constraint:
spark.sql("ALTER TABLE my_table ADD CONSTRAINT dateWithinRange CHECK (date > '1900-01-01')")
# Drop Constraint:
spark.sql("ALTER TABLE my_table DROP CONSTRAINT dateWithinRange")


# TIME TRAVEL
# View transaction log
full_history_df = delta_table.history()
# or
full_history_df = spark.sql("DESCRIBE HISTORY my_table")

# Get the last timestamp version of the table
timestamp_last = spark.sql(f"DESCRIBE HISTORY my_table1").orderBy("version", ascending=False).collect()[0].timestamp
# Query historical versions of Delta Lake tables:
df = spark.read.format("delta") \
      .option("versionAsOf", 2) \
      .option("timestampAsOf", "2020-12-18") \
      .load("path/to/delta_table")
# or
df = spark.sql("SELECT * FROM my_table TIMESTAMP AS OF '2020-12-18 11:37:00'")
df = spark.sql("SELECT * FROM my_table VERSION AS OF 2")

# Find chenges between 2 versions of table
df1 = spark.read.format("delta").load("path/to/delta_table")
df2 = spark.read.format("delta").option("versionAsOf", 2).load("path/to/delta_table")
df1.exceptAll(df2).show()
# or
spark.sql("SELECT * FROM my_table VERSION AS OF 2 EXCEPT ALL SELECT * FROM my_table VERSION AS OF 1")

# Rollback a table by version or timestamp
delta_table.restoreToVersion(2)
delta_table.resoreToTimestamp("2020-12-18")
# or
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 0")
spark.sql("RESTORE TABLE my_table TO TIMESTAMP AS OF '2020-12-18'")

# Modify data retention settings for Delta table
# logRetentionDuration - how long transaction log history is kept
# deletedFileRetentionDuration - how long ago a file must have been deleted before being a candidate for VACUUM
spark.sql("""ALTER TABLE my_table SET TBLPROPERTIES (
  delta.logRetentionDuration = 'interval 30 days'
  delta.deletedFileRetentionDuration = 'interval 7 days'
)""")
# Check table properties
spark.sql("SHOW TBLPROPERTIES my_table").show()


# CREATE AND QUERY DELTA TABLES
# Create and use managed database
# Managed database is saved in the Hive metastore
# Default database is named "default"
spark.sql("DROP DATABASE IF EXISTS dbName;")
spark.sql("CREATE DATABASE dbname;")
spark.sql("USE dbName;")  # This command avoids having to specify dbNname.tableName every time instead of just tableName
# spark.sql("USE CATALOG catalog_name;") # This command avoids having to specify catalog_name if Unity Catalog is used

# Query Delta table by name or path
df = spark.sql("SELECT * FROM [dbname.]my_table")
df = spark.sql("SELECT * FROM delta.`/path/to/delta_table`")

# Create table, define schema explicitly
spark.sql("""
          CREATE TABLE [dbName.] my_table(
              id INT [NOT NULL],
              name STRING,
              date DATE,
              int_rate FLOAT)
          USING DELTA
          [LOCATION /path/to/delta_table]  -- optional for external tables, path might be cloud storage url
          [PARTITION BY (time, date)] -- optional for tables with partitions
          """)

# CTAS, Create table as SELECT* with no upfront schema definition
# instead of * might be specific columns
spark.sql("""
          CREATE TABLE [dbname.] my_table
          USING DELTA
          AS SELECT * FROM tableName| parquet.`path/to/data`
          [LOCATION `path/to/table`] -- for external tables, path might be cloud storage url
          """)

# UTILITY METHODS
# View table details
spark.sql("DESCRIBE DETAIL my_table")
spark.sql("DESCRIBE FORMATTED my_table")

# Compact old files with Vacuum
delta_table.vacuum()  # vacuum files older than default retention period (7 days)
# or
# DRY RUN - Return a list of up to 1000 files to be deleted.
spark.sql("VACUUM my_table [RETAIN num HOURS] [DRY RUN]")

# The ANALYZE TABLE statement collects estimated statistics about a specific table or all tables in a specified schema.
# These statistics are used by the query optimizer to generate an optimal query plan.
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")

# The REFRESH TABLE invalidates the cached entries for Apache Spark cache, which include data and metadata of the
# given table or view. This is useful for tables created from csv or parquet files.
spark.sql("REFRESH TABLE books_csv;")

# Clone a Delta table
delta_table = DeltaTable.forName(spark, "source_table")
delta_table.clone(target="target_table", isShallow=True, replace=False) # clone the source at latest version
# or
# we may not specify the location, in this case the target_table will be MANAGED
spark.sql("CREATE TABLE target_table [SHALLOW | DEEP] CLONE source_table [VERSION AS OF 0] LOCATION 'path/to/delta_table';")

# Get DataFrame representation of a Delta table
df = delta_table.toDF()


# PERFORMANCE OPTIMIZATIONS
# Compact data files with Optimize and Z-Order
spark.sql("OPTIMIZE my_table [ZORDER BY (colA, colB)]")

# Auto-optimize tables
# For existing tables:
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)")
# To enable auto-optimize for all new Delta tables:
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

# Cache frequently queried data in Delta Cache
spark.sql("CACHE SELECT * FROM my_table")
# or
spark.sql("CACHE SELECT colA, colB FROM my_table WHERE colA > 0")

# Enable deletion vectors for tables
spark.sql("CREATE TABLE my_table TBLPROPERTIES ('delta.enableDeletionVectors' = true);")
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);")


# UNITY CATALOG
# Grant privileges
spark.sql("GRANT privilege_type ON securable_object TO principal")
