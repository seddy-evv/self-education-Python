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
