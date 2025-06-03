# You have a dataset containing user activity logs with missing values and inconsistent data types.
# Cleaning and standardizing a dataset with missing values and inconsistent data types using PySpark involves
# the following key steps:
from pyspark.sql.functions import mean, when, col, upper, trim, to_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local").getOrCreate()

# 1. Load the Dataset:
# Read the data from storage (e.g., CSV, Parquet, or JSON) into a PySpark DataFrame:
# df = spark.read.csv("data.csv", header=True, inferSchema=True)

# or create test data
data = [
    ("Alice", 28, 1000, "HR", "2020-01-01"),
    ("Bob", 25, 1500, "it", "2021-01-01"),
    ("Charlie", 30, 1200, "IT", "2023-01-01"),
    (None, None, None, None, None),
    ("Alex", None, None, "iT", "2022-01-01"),
    ("Mike", -21, 900, "HR", "2021-01-01"),
    ("Alice", 28, 1000, "hr", "2024-01-01"),
    ]
df = spark.createDataFrame(data, schema=["name", "Age", "Salary", "Department", "Date"])

# 2. Inspect and Understand the Data:
# Use df.show(), df.printSchema(), and df.describe() to identify missing values, inconsistent data types,
# and malformed entries.
df.show()
# +-------+----+------+----------+----------+
# |   name| Age|Salary|Department|      Date|
# +-------+----+------+----------+----------+
# |  Alice|  28|  1000|        HR|2020-01-01|
# |    Bob|  25|  1500|        it|2021-01-01|
# |Charlie|  30|  1200|        IT|2023-01-01|
# |   null|null|  null|      null|      null|
# |   Alex|null|  null|        iT|2022-01-01|
# |   Mike| -21|   900|        HR|2021-01-01|
# |  Alice|  28|  1000|        hr|2024-01-01|
# +-------+----+------+----------+----------+

df.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- Age: long (nullable = true)
#  |-- Salary: long (nullable = true)
#  |-- Department: string (nullable = true)
#  |-- Date: string (nullable = true)

df.describe().show()
# +-------+----+-----------------+-----------------+----------+----------+
# |summary|name|              Age|           Salary|Department|      Date|
# +-------+----+-----------------+-----------------+----------+----------+
# |  count|   6|                5|                5|         6|         6|
# |   mean|null|             18.0|           1120.0|      null|      null|
# | stddev|null|21.87464285422736|238.7467277262664|      null|      null|
# |    min|Alex|              -21|              900|        HR|2020-01-01|
# |    max|Mike|               30|             1500|        it|2024-01-01|
# +-------+----+-----------------+-----------------+----------+----------+

# 3. Handle Missing Values:
# Drop rows or columns with too many missing values:

df = df.dropna(how="all")

# Impute missing values for numerical or categorical columns:

mean_value = df.select(mean("Salary")).collect()[0][0]
df = df.fillna({"Salary": mean_value, "Age": 21})

# 4. Fix Inconsistent Data Types:
# Cast columns to the appropriate data types:
df = df.withColumn("Age", df["Age"].cast("Integer"))

# Handle invalid values:
df = df.withColumn("Age", when(col("Age") < 0, 21).otherwise(col("Age")))

# 5. Rename or Standardize Columns:
# Rename columns to follow a consistent naming convention:

df = df.withColumnRenamed("name", "Name")

# 6. Remove or Handle Duplicates:
# Identify and drop duplicate rows:
df = df.dropDuplicates()

# 7. Normalize and Standardize Data Formats:
# Handle text inconsistencies (e.g., case normalization):
df = df.withColumn("Department", upper(trim(col("Department"))))

# Format timestamps:
df = df.withColumn("Date", to_timestamp("Date", "yyyy-MM-dd"))
df.show()
# +-------+---+------+----------+-------------------+
# |   Name|Age|Salary|Department|               Date|
# +-------+---+------+----------+-------------------+
# |  Alice| 28|  1000|        HR|2020-01-01 00:00:00|
# |    Bob| 25|  1500|        IT|2021-01-01 00:00:00|
# |Charlie| 30|  1200|        IT|2023-01-01 00:00:00|
# |   Alex| 21|  1120|        IT|2022-01-01 00:00:00|
# |   Mike| 21|   900|        HR|2021-01-01 00:00:00|
# |  Alice| 28|  1000|        HR|2024-01-01 00:00:00|
# +-------+---+------+----------+-------------------+

# 8. Validate the Cleaned Dataset:
# Re-inspect the DataFrame using .show() or .summary() to ensure that issues are resolved.
