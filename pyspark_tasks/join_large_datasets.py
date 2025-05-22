# We need to join two large datasets, but the join operation is causing out-of-memory errors.
# One of the methods to solve this problem:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("test").master("local").getOrCreate()

# Create test datasets
data1 = [
    (1, 28),
    (2, 27),
    (3, 33),
    (4, 44),
    (5, 35),
    (6, 46),
    (7, 76),
    (8, 39),
    (9, 20),
    (10, 33)
    ]
df_1 = spark.createDataFrame(data1, ["id", "Age"])

data2 = [
    (10, "M"),
    (9, "M"),
    (8, "F"),
    (7, "M"),
    (6, "M"),
    (5, "F"),
    (4, "M"),
    (3, "F"),
    (2, "F"),
    (1, "M")
    ]
df_2 = spark.createDataFrame(data2, ["id", "Gender"])

# Repartition the data and persist using partitioned tables, format might be orc or delta, for Databricks we can
# omit format option
df_1 = df_1.withColumn("par_id", col('id') % 5).repartition(5, 'par_id')
df_1.show()
# +---+---+------+
# | id|Age|par_id|
# +---+---+------+
# |  4| 44|     4|
# |  5| 35|     0|
# |  9| 20|     4|
# | 10| 33|     0|
# |  3| 33|     3|
# |  8| 39|     3|
# |  2| 27|     2|
# |  7| 76|     2|
# |  1| 28|     1|
# |  6| 46|     1|
# +---+---+------+
df_1.write.mode("overwrite").format('parquet').partitionBy("par_id").saveAsTable("temp_1")

df_2 = df_2.withColumn("par_id", col('id') % 5).repartition(5, 'par_id')
df_2.show()
# +---+------+------+
# | id|Gender|par_id|
# +---+------+------+
# | 10|     M|     0|
# |  9|     M|     4|
# |  5|     F|     0|
# |  4|     M|     4|
# |  8|     F|     3|
# |  3|     F|     3|
# |  7|     M|     2|
# |  2|     F|     2|
# |  6|     M|     1|
# |  1|     M|     1|
# +---+------+------+

df_2.write.mode("overwrite").format('parquet').partitionBy("par_id").saveAsTable("temp_2")

# Then read and loop through each sub partition data and join both the dataframes and persist them together.
counter = 0
parition_count = 4
while counter <= parition_count:
    query1 = "SELECT * FROM temp_1 where par_id={}".format(counter)
    query2 = "SELECT * FROM temp_2 where par_id={}".format(counter)
    df1 = spark.sql(query1)
    df2 = spark.sql(query2)
    innerjoin_EMP = df1.join(df2, df1.id == df2.id, how='inner').select(df1.id, df1.Age, df2.Gender)
    innerjoin_EMP.show()
    innerjoin_EMP.write.mode("append").format('parquet').saveAsTable("temp")
    counter = counter + 1

df_result = spark.sql("SELECT * FROM temp")
df_result.show()
# +---+---+------+
# | id|Age|Gender|
# +---+---+------+
# |  6| 46|     M|
# |  1| 28|     M|
# |  8| 39|     F|
# |  3| 33|     F|
# |  9| 20|     M|
# |  4| 44|     M|
# | 10| 33|     M|
# |  5| 35|     F|
# |  7| 76|     M|
# |  2| 27|     F|
# +---+---+------+
