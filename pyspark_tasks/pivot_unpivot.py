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
# +----------+------+----------+-----+
# |prediction|actual|      data|model|
# +----------+------+----------+-----+
# |         1|     0|2025-10-01|  XGB|
# |         1|     1|2025-10-02|  XGB|
# |         0|     1|2025-10-03|  XGB|
# |         1|     0|2025-10-04|  XGB|
# |         1|     1|2025-10-05|  XGB|
# |         0|     0|2025-10-06|  XGB|
# |         1|     0|2025-10-07|  XGB|
# |         0|     1|2025-10-08|  XGB|
# |         1|     0|2025-10-09|  XGB|
# |         0|     0|2025-10-10|  XGB|
# |         0|     1|2025-10-11|  XGB|
# |         1|     1|2025-10-12|  XGB|
# |         1|     0|2025-10-13|  XGB|
# +----------+------+----------+-----+

# PIVOT
df_confusion_matrix = df.withColumn(
    "confusion_matrix",
    when((col("prediction") == 1) & (col("actual") == 1), lit("TP"))
    .when((col("prediction") == 1) & (col("actual") == 0), lit("FP"))
    .when((col("prediction") == 0) & (col("actual") == 1), lit("FN"))
    .when((col("prediction") == 0) & (col("actual") == 0), lit("TN"))
    .otherwise("null"),
)

print("df confusion matrix")
df_confusion_matrix.show()
# +----------+------+----------+-----+----------------+
# |prediction|actual|      data|model|confusion_matrix|
# +----------+------+----------+-----+----------------+
# |         1|     0|2025-10-01|  XGB|              FP|
# |         1|     1|2025-10-02|  XGB|              TP|
# |         0|     1|2025-10-03|  XGB|              FN|
# |         1|     0|2025-10-04|  XGB|              FP|
# |         1|     1|2025-10-05|  XGB|              TP|
# |         0|     0|2025-10-06|  XGB|              TN|
# |         1|     0|2025-10-07|  XGB|              FP|
# |         0|     1|2025-10-08|  XGB|              FN|
# |         1|     0|2025-10-09|  XGB|              FP|
# |         0|     0|2025-10-10|  XGB|              TN|
# |         0|     1|2025-10-11|  XGB|              FN|
# |         1|     1|2025-10-12|  XGB|              TP|
# |         1|     0|2025-10-13|  XGB|              FP|
# +----------+------+----------+-----+----------------+

df_pivot = df_confusion_matrix.groupBy("model").pivot("confusion_matrix").count().fillna(0)

print("df pivot")
df_pivot.show()
# +-----+---+---+---+---+
# |model| FN| FP| TN| TP|
# +-----+---+---+---+---+
# |  XGB|  3|  5|  2|  3|
# +-----+---+---+---+---+
