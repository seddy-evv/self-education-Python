# You are provided with two files (sales.csv, customers.parquet) containing sales and customer information.
#
# sale_id, customer_id, sale_amount
# 1,       1001,        250
# 2,       1002,        750
# 3,       1003,        1200
# 4,       1001,        850
# 5,       1004,        500
# 6,       1002,        300
#
# id,   customer_name,  country_code
# 1001, John Doe,       US
# 1002, Jane Smith,     UK
# 1003, Bob Johnson,    US
# 1004, Alice Williams, CA
#
# Your tasks are to process these files (avoid repeatedly reading data from the disk):
# - find 5 customers with the least number of transactions
# - identify the 3 customers who spent the most money (total_sales) within each country
#
# Save the output to parquet table (you should have only one file in the result)
#
# 1001, 1100, John Doe,       US
# 1002, 950,  Jane Smith,     UK
# 1003, 1200, Bob Johnson,    US
# 1004, 500,  Alice Williams, CA

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, sum, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").master("local").getOrCreate()

# Create test data
sales_data = [
  (1, 1001, 250),
  (2, 1002, 750),
  (3, 1003, 1200),
  (4, 1001, 850),
  (5, 1004, 500),
  (6, 1002, 300),
  (7, 1005, 400),
  (8, 1006, 400),
  (9, 1007, 100),
]
sales_df = spark.createDataFrame(sales_data, schema=["sale_id", "customer_id", "sale_amount"])
print("sales_df")
sales_df.show()
# sales_df
# +-------+-----------+-----------+
# |sale_id|customer_id|sale_amount|
# +-------+-----------+-----------+
# |      1|       1001|        250|
# |      2|       1002|        750|
# |      3|       1003|       1200|
# |      4|       1001|        850|
# |      5|       1004|        500|
# |      6|       1002|        300|
# |      7|       1005|        400|
# |      8|       1006|        400|
# |      9|       1007|        100|
# +-------+-----------+-----------+

costumer_data = [
  (1001, "John Doe", "US"),
  (1002, "Jane Smith", "UK"),
  (1003, "Bob Johnson", "US"),
  (1004, "Alice Williams", "CA"),
  (1005, "Alex", "CA"),
  (1006, "Katie", "CA"),
  (1007, "Maks", "CA"),
]
costumer_df = spark.createDataFrame(costumer_data, schema=["id", "customer_name", "country_code"])
print("costumer_df")
costumer_df.show()
# costumer_df
# +----+--------------+------------+
# |  id| customer_name|country_code|
# +----+--------------+------------+
# |1001|      John Doe|          US|
# |1002|    Jane Smith|          UK|
# |1003|   Bob Johnson|          US|
# |1004|Alice Williams|          CA|
# |1005|          Alex|          CA|
# |1006|         Katie|          CA|
# |1007|          Maks|          CA|
# +----+--------------+------------+

# Create test data with schema
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# schema_sales = StructType([
#   StructField("sale_id", IntegerType(), True),
#   StructField("customer_id", IntegerType(), True),
#   StructField("sale_amount", IntegerType(), True)
# ])
# sales_df = spark.createDataFrame(sales_data, schema=schema_sales)
# sales_df.show()

# customer_schema = StructType([
#   StructField("id", IntegerType(), True),
#   StructField("customer_name", StringType(), True),
#   StructField("country_code", StringType(), True)
# ])
# costumer_df = spark.createDataFrame(costumer_data, schema=["id", "customer_name", "country_code"])
# costumer_df.show()

# Read data from csv
# sales_df = spark.read.format("csv").options(inferSchema=True, header=True).load("dbfs:/FileStore/sales.csv").cache()
# customer_df = spark.read.format("parquet").load("dbfs:/FileStore/customers.parquet").cache()

# find 5 customers with the least number of transactions
df_least_tr = sales_df.groupBy(sales_df.customer_id).agg(count(sales_df.sale_id).alias("count_sale")).orderBy(col("count_sale")).limit(5)
print('5 customers with the least number of transactionss:')
df_least_tr.show()
# 5 customers with the least number of transactionss:
# +-----------+----------+
# |customer_id|count_sale|
# +-----------+----------+
# |       1003|         1|
# |       1004|         1|
# |       1005|         1|
# |       1007|         1|
# |       1006|         1|
# +-----------+----------+

# 5 customers with additioonal info
df_least_tr.join(costumer_df, df_least_tr.customer_id == costumer_df.id).show()
# +-----------+----------+----+--------------+------------+
# |customer_id|count_sale|  id| customer_name|country_code|
# +-----------+----------+----+--------------+------------+
# |       1003|         1|1003|   Bob Johnson|          US|
# |       1004|         1|1004|Alice Williams|          CA|
# |       1005|         1|1005|          Alex|          CA|
# |       1006|         1|1006|         Katie|          CA|
# |       1007|         1|1007|          Maks|          CA|
# +-----------+----------+----+--------------+------------+

# identify the 3 customers who spent the most money (total_sales) within each country
df_sales_agg = sales_df.groupBy("customer_id").agg(sum("sale_amount").alias("total_sales"))
df_sales_join = df_sales_agg.join(costumer_df, df_sales_agg.customer_id == costumer_df.id)

window_spec = Window.partitionBy("country_code").orderBy(df_sales_join.total_sales.desc())
df_res = df_sales_join.withColumn("rank_sales", row_number().over(window_spec)).filter(col("rank_sales") < 4).drop("id", "rank_sales")
print('3 customers who spent the most money (total_sales) within each country:')
df_res.show()
# 3 customers who spent the most money (total_sales) within each country:
# +-----------+-----------+--------------+------------+
# |customer_id|total_sales| customer_name|country_code|
# +-----------+-----------+--------------+------------+
# |       1004|        500|Alice Williams|          CA|
# |       1005|        400|          Alex|          CA|
# |       1006|        400|         Katie|          CA|
# |       1002|       1050|    Jane Smith|          UK|
# |       1003|       1200|   Bob Johnson|          US|
# |       1001|       1100|      John Doe|          US|
# +-----------+-----------+--------------+------------+

# df_res.coalesce(1).write.format("parquet").save("output_path.parquet")
