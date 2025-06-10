# We have a log of hotel check-ins and check-outs.
# The log is presented as records with the following fields:
# user_id (VARCHAR) — unique user identifier.
# event_type (VARCHAR) — event type: 'IN' — user checked in. 'OUT' — user checked out.
# ts (TIMESTAMP) — date and time of the event.
#
# We need to display how many people were in the hotel by day.

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_date, count, when, col, sum
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts", TimestampType(), True)
])

data = [
    (1, 'IN', datetime(2022, 1, 1, 10, 0, 0)),
    (2, 'IN', datetime(2022, 1, 1, 10, 0, 0)),
    (1, 'OUT', datetime(2022, 1, 6, 10, 0, 0)),
    (4, 'IN', datetime(2022, 1, 6, 10, 0, 0)),
    (5, 'IN', datetime(2022, 1, 6, 10, 0, 0)),
    (6, 'IN', datetime(2022, 1, 7, 10, 0, 0)),
    (8, 'IN', datetime(2022, 1, 10, 10, 0, 0)),
    (2, 'OUT', datetime(2022, 1, 10, 10, 0, 0)),
    (1, 'IN', datetime(2022, 2, 1, 10, 0, 0))
]

df = spark.createDataFrame(data, schema)

df.show()

df_hotel_agg = df.groupBy(df.event_type, to_date(df.ts).alias("day")).agg(count(to_date(df.ts)).alias("count_ts"))

df_hotel_sum = df_hotel_agg.select(df_hotel_agg.day, when(df_hotel_agg.event_type == "IN", df_hotel_agg.count_ts)
                                   .otherwise(when(df_hotel_agg.event_type == "OUT", - df_hotel_agg.count_ts)
                                   .otherwise(0)).alias("res")).groupBy(col("day")).agg(sum(col("res")).alias("sum_res"))

window_spec = Window.orderBy(df_hotel_sum["day"].asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
# OR if rows between unbounded preceding and the current row we can omit this condition
# window_spec = Window.orderBy(df_hotel_sum["day"].asc())

df_res = df_hotel_sum.select(df_hotel_sum["day"], sum(df_hotel_sum["sum_res"]).over(window_spec).alias("agg_res"))
df_res.show()
# +----------+-------+
# |       day|agg_res|
# +----------+-------+
# |2022-01-01|      2|
# |2022-01-06|      3|
# |2022-01-07|      4|
# |2022-01-10|      4|
# |2022-02-01|      5|
# +----------+-------+
