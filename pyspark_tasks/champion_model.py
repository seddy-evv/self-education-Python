# We have a table with metrics for multiple model versions, we need to determine the champion model and reflect
# this in the data frame.

from pyspark.sql.functions import to_timestamp, desc, row_number, when, lit, col
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("champion_model").master("local").getOrCreate()

data = [("1", 0.9, "2025-11-01"), ("2", 0.9, "2025-12-01"), ("3", 0.8, "2026-01-01")]
df_metrics = spark.createDataFrame(data, schema=["version", "f1", "train_date"])

df_metrics = df_metrics.withColumn("train_date", to_timestamp("train_date"))
w = Window.orderBy(desc("f1"), desc("train_date"))
df_metrics = df_metrics.withColumn("rank", row_number().over(w))
df_metrics = df_metrics.withColumn("champion", when(col("rank") == 1, lit(True)).otherwise(lit(False))).drop("rank")


df_metrics.show()
