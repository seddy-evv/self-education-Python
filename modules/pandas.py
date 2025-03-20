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


# Data Exploration

data = {'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'Los Angeles', 'Chicago']}
df = pd.DataFrame(data)

# 1. df.head(n): Returns the first n rows of the DataFrame (default is 5 rows).

# Example:
print(df.head(1))

# 2. df.tail(n): Returns the last n rows of the DataFrame (default is 5 rows).

# Example:
print(df.tail(1))

# 3. df.info(): Provides an overview of the DataFrame, including column types and non-null counts.

# Example:
print(df.info())

# 4. df.describe(): Provides summary statistics for numeric columns.

# Example:
print(df.describe())

# 5. df.shape: Returns the dimensions of the DataFrame (rows, columns).

# Example:
print(df.shape)

# 6. df.columns: Lists all column names.

# Example:
print(df.columns)

# 7. df.index: Displays the index (labels) of the DataFrame.

# Example:
print(df.index)


# Data Cleaning

df_with_na = pd.DataFrame({'Name': ['Alice', 'Bob', None], 'Age': [25, None, 35]})

# 1. df.dropna(): Removes rows or columns with missing values.

# Example:
print(df_with_na.dropna())

# 2. df.fillna(value): Fills missing values with a specified value.

# Example:
print(df_with_na.fillna('Unknown'))

# 3. df.isnull(): Returns a DataFrame of boolean values indicating missing data.

# Example:
print(df_with_na.isnull())

# 4. df.notnull(): Returns a DataFrame of boolean values for non-missing data.

# Example:
print(df_with_na.notnull())

# 5. df.drop(columns='column_name'): Drops a specific column.

# Example:
print(df_with_na.drop(columns='Name'))

# 6. df.rename(columns={'old_name': 'new_name'}): Renames one or more columns.

# Example:
renamed_df = df_with_na.rename(columns={'Name': 'Full Name', 'Age': 'Years'})
print(renamed_df)


# 8. df.dtypes: Lists the data types of each column.

# Example:
print(df.dtypes)
