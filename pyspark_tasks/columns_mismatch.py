# When you try to write a PySpark DataFrame to a Delta table in Databricks, and the DataFrame has either fewer or
# more columns than the target Delta table, the behavior and error handling depend on the specific scenario and
# the write mode you use.

# What Happens
# 1. DataFrame Has Fewer Columns Than Delta Table
# Default Behavior:
# If you try to write a DataFrame with missing columns (compared to the Delta table schema), you will get an error like:

# org.apache.spark.sql.AnalysisException:
# A schema mismatch detected when writing to the Delta table.

# Reason:
# Delta Lake enforces schema compatibility by default. Missing columns in the DataFrame are not automatically filled with nulls.

# 2. DataFrame Has More Columns Than Delta Table
# Default Behavior:
# If your DataFrame has extra columns not present in the Delta table, you will also get a schema mismatch error.
# 3. DataFrame Columns Have Different Data Types
# Default Behavior:
# If the column names match but the data types differ, you will get a schema mismatch error.

# How to Handle Errors
# 1. Enable Schema Evolution (for Append or Overwrite)
# Schema Evolution:
# If you want to allow the Delta table to accept new columns (i.e., schema evolution), you can use the mergeSchema option:
# python

df.write.format("delta").mode("append").option("mergeSchema", "true").save("/path/to/delta-table")

# This will add new columns from the DataFrame to the Delta table.
# Note: Schema evolution is only supported in append or overwrite modes, not in replace mode.
# 2. Align DataFrame Schema with Delta Table
