# purchases_df columns: order_id, user_id, order_date, amount
# users_df columns: user_id, country, signup_date
#
# Enrich each order with the user’s country and signup date.
#
# Consider only users who signed up in the last 1 year.
# From those users, return the top 2 countries by total order amount.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_sub, sum

spark = SparkSession.builder.appName("Countries total amount").master("local").getOrCreate()

orders_data = [
    (1, 11, "2026-01-01", 250),
    (2, 22, "2024-01-01", 350),
    (3, 33, "2026-02-01", 450),
    (4, 44, "2026-03-01", 150),
    (5, 55, "2025-01-01", 600),
    (6, 11, "2026-01-01", 250)
]

orders_df = spark.createDataFrame(orders_data, schema=["order_id", "user_id", "order_date", "amount"])

users_data = [
    (11, "USA", "2026-01-01"),
    (22, "Canada", "2024-01-01"),
    (33, "Germany", "2026-02-01"),
    (44, "UK", "2026-03-01"),
    (55, "Poland", "2025-01-01")
]

users_df = spark.createDataFrame(users_data, schema=["user_id", "country", "signup_date"])

users_filtered = users_df.filter(col("signup_date").cast("date") > date_sub(current_date(), 365))

users_filtered.show()
# +-------+-------+-----------+
# |user_id|country|signup_date|
# +-------+-------+-----------+
# |     11|    USA| 2026-01-01|
# |     33|Germany| 2026-02-01|
# |     44|     UK| 2026-03-01|
# +-------+-------+-----------+

join_df = orders_df.join(users_filtered, on="user_id", how="inner")
result_df = join_df.groupBy("country").agg(sum("amount").alias("total_amount")).orderBy(col("total_amount").desc()).limit(2)

result_df.show()
# +-------+------------+
# |country|total_amount|
# +-------+------------+
# |    USA|         500|
# |Germany|         450|
# +-------+------------+
