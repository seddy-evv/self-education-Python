# 4. Joins to compare two PySpark DataFrames

# 4.1 Identify a Unique Key
# You need a column or combination of columns that uniquely identifies each row (like an ID). If you don’t have one,
# you can create a synthetic key using monotonically_increasing_id() or by concatenating all columns.

def df_id(df):
    return df.withColumn("id", monotonically_increasing_id())

df1_id = df_id(df1)
df1_id.show()
# +-------+---+---+
# |   Name|Age| id|
# +-------+---+---+
# |  Alice| 28|  0|
# |    Bob| 25|  1|
# |Charlie| 30|  2|
# +-------+---+---+
df4_id = df_id(df4)
df4_id.show()
# +-------+---+---+
# |   Name|Age| id|
# +-------+---+---+
# |  Alice| 28|  0|
# |   Bobb| 25|  1|
# |Charlie| 30|  2|
# +-------+---+---+
