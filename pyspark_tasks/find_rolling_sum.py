from pyspark.sql.functions import coalesce, sum, lit
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("find_rolling_sum").master("local").getOrCreate()

orders_data = [
    (1, 1, 100, '2021-01-10'),
    (2, 2, 150, '2021-01-20'),
    (3, 3, 250, '2021-01-30'),
    (4, 1, 200, '2021-02-15'),
    (5, 2, 300, '2021-02-22'),
    (6, 1, 150, '2021-03-01'),
    (7, 3, 180, '2021-03-18'),
    (8, 1, 120, '2021-04-05'),
    (9, 2, 220, '2021-04-25'),
    (10, 3, 160, '2021-05-16'),
    (11, 1, 140, '2021-05-29'),
    (12, 2, 280, '2021-06-10')
]

df_orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "total_amount", "order_date"])
df_orders.show()

payments_data = [
    (1, 1000.0),
    (2, 1110.55),
    (3, 2530.3),
    (4, 2003.0),
    (5, 3100.0),
    (6, 1250.0),
    (7, 1380.44),
    (8, 1420.0),
    (9, 2120.0),
    (10, 1160.66),
    (11, 1340.0),
    (12, 2811.3)
]

df_payments = spark.createDataFrame(payments_data, ["order_id", "payment_amount"])
df_payments.show()

customers_data = [
    (1, 'Alex', 'NY'),
    (2, 'Bob', 'LA'),
    (3, 'John', 'Texas')
]

df_customers = spark.createDataFrame(customers_data, ["customer_id", "customer_name", "region"])
df_customers.show()

df_order_payments = df_orders.join(df_payments, on="order_id", how="left")\
                            .groupBy("order_id", "customer_id", "order_date", "total_amount")\
                            .agg(coalesce(sum("payment_amount"), lit(0)).alias("total_paid"))
df_order_payments.show()

window_spec = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_res = df_order_payments.join(df_customers, on="customer_id", how="inner")\
    .select("customer_id", "customer_name", "region", "order_id", "order_date", "total_amount", "total_paid",
            sum("total_amount").over(window_spec).alias("running_total_by_customer"))\
    .orderBy("customer_id", "order_date")
df_res.show()
# +-----------+-------------+------+--------+----------+------------+----------+-------------------------+
# |customer_id|customer_name|region|order_id|order_date|total_amount|total_paid|running_total_by_customer|
# +-----------+-------------+------+--------+----------+------------+----------+-------------------------+
# |          1|         Alex|    NY|       1|2021-01-10|         100|    1000.0|                      100|
# |          1|         Alex|    NY|       4|2021-02-15|         200|    2003.0|                      300|
# |          1|         Alex|    NY|       6|2021-03-01|         150|    1250.0|                      450|
# |          1|         Alex|    NY|       8|2021-04-05|         120|    1420.0|                      570|
# |          1|         Alex|    NY|      11|2021-05-29|         140|    1340.0|                      710|
# |          2|          Bob|    LA|       2|2021-01-20|         150|   1110.55|                      150|
# |          2|          Bob|    LA|       5|2021-02-22|         300|    3100.0|                      450|
# |          2|          Bob|    LA|       9|2021-04-25|         220|    2120.0|                      670|
# |          2|          Bob|    LA|      12|2021-06-10|         280|    2811.3|                      950|
# |          3|         John| Texas|       3|2021-01-30|         250|    2530.3|                      250|
# |          3|         John| Texas|       7|2021-03-18|         180|   1380.44|                      430|
# |          3|         John| Texas|      10|2021-05-16|         160|   1160.66|                      590|
# +-----------+-------------+------+--------+----------+------------+----------+-------------------------+
