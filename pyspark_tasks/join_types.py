# Different join types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PySpark Examples").master("local").getOrCreate()

data1 = [
    (1, 0, 2),
    (1, 1, 2),
    (1, 2, 2),
    (11, 3, 2)
]
t1 = spark.createDataFrame(data1, ["id1", "num1", "val1"])

data2 = [
    (1, 0, 3),
    (1, 1, 3),
    (22, 2, 3)
]
t2 = spark.createDataFrame(data2, ["id2", "num2", "val2"])

# INNER JOIN (default)
t1.join(t2, t1.id1 == t2.id2, how="inner").show()
# or
t1.join(t2, t1.id1 == t2.id2).show()
# +---+----+----+---+----+----+
# |id1|num1|val1|id2|num2|val2|
# +---+----+----+---+----+----+
# |  1|   0|   2|  1|   0|   3|
# |  1|   0|   2|  1|   1|   3|
# |  1|   1|   2|  1|   0|   3|
# |  1|   1|   2|  1|   1|   3|
# |  1|   2|   2|  1|   0|   3|
# |  1|   2|   2|  1|   1|   3|
# +---+----+----+---+----+----+

# LEFT JOIN (leftouter, left_outer)
t1.join(t2, t1.id1 == t2.id2, how="left").show()
# +---+----+----+----+----+----+
# |id1|num1|val1| id2|num2|val2|
# +---+----+----+----+----+----+
# |  1|   0|   2|   1|   1|   3|
# |  1|   0|   2|   1|   0|   3|
# |  1|   1|   2|   1|   1|   3|
# |  1|   1|   2|   1|   0|   3|
# |  1|   2|   2|   1|   1|   3|
# |  1|   2|   2|   1|   0|   3|
# | 11|   3|   2|null|null|null|
# +---+----+----+----+----+----+

# RIGHT JOIN (rightouter, right_outer)
t1.join(t2, t1.id1 == t2.id2, how="right").show()
# +----+----+----+---+----+----+
# | id1|num1|val1|id2|num2|val2|
# +----+----+----+---+----+----+
# |   1|   2|   2|  1|   0|   3|
# |   1|   1|   2|  1|   0|   3|
# |   1|   0|   2|  1|   0|   3|
# |   1|   2|   2|  1|   1|   3|
# |   1|   1|   2|  1|   1|   3|
# |   1|   0|   2|  1|   1|   3|
# |null|null|null| 22|   2|   3|
# +----+----+----+---+----+----+

# FULL JOIN (outer, fullouter, full_outer)
t1.join(t2, t1.id1 == t2.id2, how="full").show()
# +----+----+----+----+----+----+
# | id1|num1|val1| id2|num2|val2|
# +----+----+----+----+----+----+
# |   1|   0|   2|   1|   0|   3|
# |   1|   0|   2|   1|   1|   3|
# |   1|   1|   2|   1|   0|   3|
# |   1|   1|   2|   1|   1|   3|
# |   1|   2|   2|   1|   0|   3|
# |   1|   2|   2|   1|   1|   3|
# |  11|   3|   2|null|null|null|
# |null|null|null|  22|   2|   3|
# +----+----+----+----+----+----+

# CROSS JOIN (with keys)
t1.join(t2, t1.id1 == t2.id2, how="cross").show()
# +---+----+----+---+----+----+
# |id1|num1|val1|id2|num2|val2|
# +---+----+----+---+----+----+
# |  1|   0|   2|  1|   0|   3|
# |  1|   0|   2|  1|   1|   3|
# |  1|   1|   2|  1|   0|   3|
# |  1|   1|   2|  1|   1|   3|
# |  1|   2|   2|  1|   0|   3|
# |  1|   2|   2|  1|   1|   3|
# +---+----+----+---+----+----+

# CROSS JOIN
t1.join(t2, how="cross").show()
# or inner join without keys
t1.join(t2).show()
# +---+----+----+---+----+----+
# |id1|num1|val1|id2|num2|val2|
# +---+----+----+---+----+----+
# |  1|   0|   2|  1|   0|   3|
# |  1|   0|   2|  1|   1|   3|
# |  1|   0|   2| 22|   2|   3|
# |  1|   1|   2|  1|   0|   3|
# |  1|   1|   2|  1|   1|   3|
# |  1|   1|   2| 22|   2|   3|
# |  1|   2|   2|  1|   0|   3|
# |  1|   2|   2|  1|   1|   3|
# |  1|   2|   2| 22|   2|   3|
# | 11|   3|   2|  1|   0|   3|
# | 11|   3|   2|  1|   1|   3|
# | 11|   3|   2| 22|   2|   3|
# +---+----+----+---+----+----+

# LEFT ANTI JOIN (anti, leftanti)
t1.join(t2, t1.id1 == t2.id2, how="left_anti").show()
# +---+----+----+
# |id1|num1|val1|
# +---+----+----+
# | 11|   3|   2|
# +---+----+----+

# LEFT SEMI JOIN (semi, leftsemi)
# Returns rows and columns only from the first table if the keys matched the second table, like inner join, but without
# the values of the second table and, accordingly, without duplicates, etc.
t1.join(t2, t1.id1 == t2.id2, how="left_semi").show()
# +---+----+----+
# |id1|num1|val1|
# +---+----+----+
# |  1|   0|   2|
# |  1|   1|   2|
# |  1|   2|   2|
# +---+----+----+

# SELF JOIN
# Using Self join, we will output the name of the boss for the employees of the group via id. If the boss's id is
# not in the database, then this employee will not be in the resulting table.
data = [
    (1, "Smith", -1, 2018, 10, "M", 4000),
    (2, "Rose", 1, 2010, 20, "F", 2500),
    (3, "Williams", 1, 2010, 10, "M", 2100),
    (4, "Jones", 2, 2005, 10, "M", 1800),
    (5, "Brown", 2, 2010, 40, "M", 1700)
]
df = spark.createDataFrame(data, ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"])

df.alias("emp1").join(df.alias("emp2"), col("emp1.superior_emp_id") == col("emp2.emp_id"), how="inner")\
    .select(col("emp1.emp_id"),
            col("emp1.name"),
            col("emp2.emp_id").alias("superior_emp_id"),
            col("emp2.name").alias("superior_name")).show()
