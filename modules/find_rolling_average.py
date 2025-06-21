# Coding Question: python. find the 3-month rolling average of total revenue from purсhases given a table with users,
# their purchase amount, and date purchases (YYYY-MM-DD).
# Output the year-month (YYYY-MM) and 3-month rolling average of revenue, sorted form earliest month to latest month.

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, sum, col, date_format, avg, round
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DoubleType
from pyspark.sql import Window

spark = SparkSession.builder.appName("interview").master("local").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("purchase_date", DateType())
])

data = [
    (1, 100.0, datetime(2021, 1, 10)),
    (2, 150.0, datetime(2021, 1, 20)),
    (3, 250.0, datetime(2021, 1, 30)),
    (1, 200.0, datetime(2021, 2, 15)),
    (2, 300.0, datetime(2021, 2, 22)),
    (1, 150.0, datetime(2021, 3, 1)),
    (3, 180.0, datetime(2021, 3, 18)),
    (1, 120.0, datetime(2021, 4, 5)),
    (2, 220.0, datetime(2021, 4, 25)),
    (3, 160.0, datetime(2021, 5, 16)),
    (1, 140.0, datetime(2021, 5, 29)),
    (2, 280.0, datetime(2021, 6, 10))
]

# OR

# schema = ["user_id", "purchase_amount", "purchase_date"]
# data = [
#     (1, 100.0, '2021-01-10'),
#     (2, 150.0, '2021-01-20'),
#     (3, 250.0, '2021-01-30'),
#     (1, 200.0, '2021-02-15'),
#     (2, 300.0, '2021-02-22'),
#     (1, 150.0, '2021-03-01'),
#     (3, 180.0, '2021-03-18'),
#     (1, 120.0, '2021-04-05'),
#     (2, 220.0, '2021-04-25'),
#     (3, 160.0, '2021-05-16'),
#     (1, 140.0, '2021-05-29'),
#     (2, 280.0, '2021-06-10')
# ]


df = spark.createDataFrame(data, schema)
df.show()
# +-------+---------------+-------------+
# |user_id|purchase_amount|purchase_date|
# +-------+---------------+-------------+
# |      1|          100.0|   2021-01-10|
# |      2|          150.0|   2021-01-20|
# |      3|          250.0|   2021-01-30|
# |      1|          200.0|   2021-02-15|
# |      2|          300.0|   2021-02-22|
# |      1|          150.0|   2021-03-01|
# |      3|          180.0|   2021-03-18|
# |      1|          120.0|   2021-04-05|
# |      2|          220.0|   2021-04-25|
# |      3|          160.0|   2021-05-16|
# |      1|          140.0|   2021-05-29|
# |      2|          280.0|   2021-06-10|
# +-------+---------------+-------------+

df_sum = df.groupBy(date_format(col("purchase_date"), "yyyy-MM").alias("year_month")).agg(
    sum(df["purchase_amount"]).alias("purchase_amount_sum"))
df_sum.show()
# +----------+-------------------+
# |year_month|purchase_amount_sum|
# +----------+-------------------+
# |   2021-01|              500.0|
# |   2021-02|              500.0|
# |   2021-03|              330.0|
# |   2021-04|              340.0|
# |   2021-05|              300.0|
# |   2021-06|              280.0|
# +----------+-------------------+

window_spec = Window.orderBy(df_sum["year_month"].asc()).rowsBetween(-2, Window.currentRow)

df_res = df_sum.select(df_sum["year_month"], round(avg(df_sum["purchase_amount_sum"]).over(window_spec), 2).alias("res"))\
                       .orderBy(col("year_month").asc())
df_res.show()
# +----------+------+
# |year_month|   res|
# +----------+------+
# |   2021-01| 500.0|
# |   2021-02| 500.0|
# |   2021-03|443.33|
# |   2021-04| 390.0|
# |   2021-05|323.33|
# |   2021-06|306.67|
# +----------+------+
