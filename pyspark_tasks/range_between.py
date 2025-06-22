from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Range Between").master("local").getOrCreate()

# Example with sample data
data = [("A", 10), ("A", 12), ("A", 15), ("B", 20), ("B", 22)]
df = spark.createDataFrame(data, ["partition_column", "order_column"])

# Window specification with rangeBetween
window_spec = Window.partitionBy("partition_column").orderBy("order_column").rangeBetween(-2, 2)

# Applying the window function (e.g., sum)
# sum will be calculated for all rows with values that between (order_column value - 2) and (order_column value + 2)
result_df = df.withColumn("sum_in_range", f.sum("order_column").over(window_spec))

result_df.show()
# +----------------+------------+------------+
# |partition_column|order_column|sum_in_range|
# +----------------+------------+------------+
# |               A|          10|          22|
# |               A|          12|          22|
# |               A|          15|          15|
# |               B|          20|          42|
# |               B|          22|          42|
# +----------------+------------+------------+
