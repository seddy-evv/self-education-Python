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

# WITHOUT PIVOT

df_custom = df \
.withColumn("TP", when((col("prediction") == 1) & (col("actual") == 1), 1).otherwise(0)) \
.withColumn("FP", when((col("prediction") == 1) & (col("actual") == 0), 1).otherwise(0)) \
.withColumn("FN", when((col("prediction") == 0) & (col("actual") == 1), 1).otherwise(0)) \
.withColumn("TN", when((col("prediction") == 0) & (col("actual") == 0), 1).otherwise(0)) \

print("df custom")
df_custom.show()
# +----------+------+----------+-----+---+---+---+---+
# |prediction|actual|      data|model| TP| FP| FN| TN|
# +----------+------+----------+-----+---+---+---+---+
# |         1|     0|2025-10-01|  XGB|  0|  1|  0|  0|
# |         1|     1|2025-10-02|  XGB|  1|  0|  0|  0|
# |         0|     1|2025-10-03|  XGB|  0|  0|  1|  0|
# |         1|     0|2025-10-04|  XGB|  0|  1|  0|  0|
# |         1|     1|2025-10-05|  XGB|  1|  0|  0|  0|
# |         0|     0|2025-10-06|  XGB|  0|  0|  0|  1|
# |         1|     0|2025-10-07|  XGB|  0|  1|  0|  0|
# |         0|     1|2025-10-08|  XGB|  0|  0|  1|  0|
# |         1|     0|2025-10-09|  XGB|  0|  1|  0|  0|
# |         0|     0|2025-10-10|  XGB|  0|  0|  0|  1|
# |         0|     1|2025-10-11|  XGB|  0|  0|  1|  0|
# |         1|     1|2025-10-12|  XGB|  1|  0|  0|  0|
# |         1|     0|2025-10-13|  XGB|  0|  1|  0|  0|
# +----------+------+----------+-----+---+---+---+---+

df_custom_pivot = df_custom.groupBy("model").agg(sum("TP").alias("TP"), sum("FP").alias("FP"), sum("FN").alias("FN"), sum("TN").alias("TN"))

print("df custom pivot")
df_custom_pivot.show()
# +-----+---+---+---+---+
# |model| TP| FP| FN| TN|
# +-----+---+---+---+---+
# |  XGB|  3|  5|  3|  2|
# +-----+---+---+---+---+

# UNPIVOT
df_unpivot = df_pivot.unpivot("model", ["TP", "FP", "FN", "TN"], "confusion_matrix", "val")
print("df unpivot")
df_unpivot.show()
# +-----+----------------+---+
# |model|confusion_matrix|val|
# +-----+----------------+---+
# |  XGB|              TP|  3|
# |  XGB|              FP|  5|
# |  XGB|              FN|  3|
# |  XGB|              TN|  2|
# +-----+----------------+---+

# UNPIVOT CUSTOM
df_unpivot_custom = df_pivot.selectExpr("model", "stack(4, 'TP', TP, 'FP', FP, 'FN', FN, 'TN', TN) as (confusion_matrix, val)")
print("df unpivot custom")
df_unpivot_custom.show()
# +-----+----------------+---+
# |model|confusion_matrix|val|
# +-----+----------------+---+
# |  XGB|              TP|  3|
# |  XGB|              FP|  5|
# |  XGB|              FN|  3|
# |  XGB|              TN|  2|
# +-----+----------------+---+

# UNPIVOT TO THE ORIGINAL DF
# For 'TP'
df_tp = df_pivot.select(
    "model",
    explode(sequence(lit(1), col("TP"))).alias("dummy")
).withColumn("type", lit("TP")).drop("dummy")

# For 'FP'
df_fp = df_pivot.select(
    "model",
    explode(sequence(lit(1), col("FP"))).alias("dummy")
).withColumn("type", lit("FP")).drop("dummy")

# For 'FN'
df_fn = df_pivot.select(
    "model",
    explode(sequence(lit(1), col("FN"))).alias("dummy")
).withColumn("type", lit("FN")).drop("dummy")

# For 'TN'
df_tn = df_pivot.select(
    "model",
    explode(sequence(lit(1), col("TN"))).alias("dummy")
).withColumn("type", lit("TN")).drop("dummy")

result = df_tp.union(df_fp).union(df_fn).union(df_tn)

print("unpivot to the original df")
result.show()
# +-----+----+
# |model|type|
# +-----+----+
# |  XGB|  TP|
# |  XGB|  TP|
# |  XGB|  TP|
# |  XGB|  FP|
# |  XGB|  FP|
# |  XGB|  FP|
# |  XGB|  FP|
# |  XGB|  FP|
# |  XGB|  FN|
# |  XGB|  FN|
# |  XGB|  FN|
# |  XGB|  TN|
# |  XGB|  TN|
# +-----+----+
