# pivot and unpivot pyspark functions
from pyspark.sql.functions import when, lit, col, sum, explode, sequence
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()

data = [
    (1, 0, "2025-10-01", "XGB"),
    (1, 1, "2025-10-02", "XGB"),
    (0, 1, "2025-10-03", "XGB"),
    (1, 0, "2025-10-04", "XGB"),
    (1, 1, "2025-10-05", "XGB"),
    (0, 0, "2025-10-06", "XGB"),
    (1, 0, "2025-10-07", "XGB"),
    (0, 1, "2025-10-08", "XGB"),
    (1, 0, "2025-10-09", "XGB"),
    (0, 0, "2025-10-10", "XGB"),
    (0, 1, "2025-10-11", "XGB"),
    (1, 1, "2025-10-12", "XGB"),
    (1, 0, "2025-10-13", "XGB"),
]
df = spark.createDataFrame(data, schema=["prediction", "actual", "date", "model"])

print("df initial")
df.show()
