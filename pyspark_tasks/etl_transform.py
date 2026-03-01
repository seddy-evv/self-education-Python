# from Spark version 3.0.0
# pyspark.sql.DataFrame.transform - Returns a new DataFrame. Concise syntax for chaining custom transformations.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col, when


def add_full_name(df: DataFrame) -> DataFrame:
    if "first" in df.columns and "last" in df.columns:
        return df.withColumn("full_name", concat_ws(" ", col("first"), col("last")))
    else:
        return df


def add_status(df: DataFrame) -> DataFrame:
    if "age" in df.columns:
        return df.withColumn("status", when(col("age") > 18, "adult").otherwise("minor"))


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PythonETLApp") \
        .getOrCreate()

    data = [("John", "Doe", 28),
            ("Jane", "Smith", 34),
            ("Alice", "Johnson", 25),
            ("Bob", "Williams", 40),
            ("Eve", "Brown", 22)]

    raw_df = spark.createDataFrame(data, ["first_name", "last_name", "age"])
    res_df = (
        raw_df.transform(add_full_name)
        .transform(add_status)
    )
    res_df.show()
    # +----------+-----------+-----+--------+
    # |first_name| last_name | age | status |
    # +----------+-----------+-----+--------+
    # |     John |       Doe |  28 |  adult |
    # |     Jane |     Smith |  34 |  adult |
    # |    Alice |   Johnson |  25 |  adult |
    # |      Bob |  Williams |  17 |  minor |
    # |      Eve |     Brown |  22 |  adult |
    # +----------+-----------+-----+--------+
