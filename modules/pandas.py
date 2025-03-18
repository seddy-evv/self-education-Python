# Python pandas is a powerful library for data manipulation, analysis, and visualization. It provides a range
# of functions to work with data efficiently. Here's a description of the main pandas functions:

import pandas as pd

# Data Creation

# 1. pd.DataFrame(data): Creates a DataFrame from a dictionary, list, or another structure. A DataFrame is
# a two-dimensional, tabular data structure with labeled axes (rows and columns).

# Example:
data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'Los Angeles', 'Chicago']}
df = pd.DataFrame(data)
print(df)

# 2. pd.Series(data): Creates a one-dimensional labeled array (like a column in a DataFrame).

# Example:
age_series = pd.Series([25, 30, 35], name='Age')
print(age_series)


# I/O Functions (Data Input/Output)

# 1. pd.read_csv(filepath): Reads data from a CSV file and loads it into a DataFrame.
# 2. pd.to_csv(filepath): Writes the DataFrame to a CSV file.
# 3. pd.read_excel(filepath): Reads data from an Excel file into a DataFrame.
# 4. pd.to_excel(filepath): Writes the DataFrame to an Excel file.
# 5. pd.read_json(filepath): Reads JSON data into a DataFrame.
# 6. pd.read_sql(query, connection): Reads SQL data into a DataFrame from a database.
# 7. pd.read_html(url): Reads HTML tables from a webpage into a DataFrame.


# Indexing and Selection

data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'Los Angeles', 'Chicago']}
df = pd.DataFrame(data)

# 1. .loc[]: Access rows/columns by labels or boolean conditions.

# Example:
print(df.loc[0, 'Age'])  # Age of the first row (25)

# 2. .iloc[]: Access rows/columns by integer positions.

# Example:
print(df.iloc[1])  # Second row

# 3. .at[]: Access a single value by row/column labels.

# Example:
print(df.at[1, "Name"])

# 4. .iat[]: Access a single value by integer positions.

# Example:
print(df.iat[1, 0])  # 1 - row, 0 - column

# 5. df['column_name']: Select a specific column.

# Example:
print(df['Name'])

# 6. df.iloc[start:end]: Slice rows or columns.

# Example:
print(df.iloc[0:2])

