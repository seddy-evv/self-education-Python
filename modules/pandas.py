# Python pandas is a powerful library for data manipulation, analysis, and visualization. It provides a range
# of functions to work with data efficiently. Here's a description of the main pandas functions:

import pandas as pd

# DATA CREATION
print('\n', 'DATA CREATION', '\n')

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
print('\n', 'I/O Functions (Data Input/Output)', '\n')

# 1. pd.read_csv(filepath): Reads data from a CSV file and loads it into a DataFrame.
# 2. pd.to_csv(filepath): Writes the DataFrame to a CSV file.
# 3. pd.read_excel(filepath): Reads data from an Excel file into a DataFrame.
# 4. pd.to_excel(filepath): Writes the DataFrame to an Excel file.
# 5. pd.read_json(filepath): Reads JSON data into a DataFrame.
# 6. pd.read_sql(query, connection): Reads SQL data into a DataFrame from a database.
# 7. pd.read_html(url): Reads HTML tables from a webpage into a DataFrame.


# Indexing and Selection
print('\n', 'Indexing and Selection', '\n')

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
print('\n', 'Data Exploration', '\n')

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

# 8. df.dtypes: Lists the data types of each column.

# Example:
print(df.dtypes)


# Data Cleaning
print('\n', 'Data Cleaning', '\n')

df_with_na = pd.DataFrame({'Name': ['Alice', 'Bob', None], 'Age': [25, None, 35]})

# 1. df.dropna(): Removes rows or columns with missing values (None, NA, NULL).
# df.dropna(inplace=True) - whether to modify the DataFrame than creating a new one (you don't need a new variable)

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


# Data Manipulation
print('\n', 'Data Manipulation', '\n')

data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex'],
        'Age': [45, 25, 33, 36],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York']}
df = pd.DataFrame(data)

# 1. df.sort_values(by='column_name'): Sorts the DataFrame by a specific column.

# Example:
print(df.sort_values(by='Age'))

# 2. df.groupby('column_name'): Groups data by a specific column for aggregation.
# 3. df.agg({'column': 'function'}): Provides aggregate operations (e.g., sum, mean, max).

# Example:
grouped = df.groupby('City').agg({'Age': 'mean'})
print(grouped)

# Example:
# In some cases we may encounter a situation where grouping from a dataframe creates a series,
# in this case command reset_index() will create a dataframe by adding an index.

data = {'Age': [45, 25, 33, 36],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York']}
df_gr = pd.DataFrame(data)

# The result will be the same for these two statements, but the structure will be different.
df_group = df_gr.groupby(['City']).agg({'Age': ['sum']})
series = df_gr.groupby(['City'])['Age'].sum()
df_from_series = series.reset_index()

print(df_group)
print(series)
print(df_from_series)

# 4. df.merge(other_df, on='key'): Merges two DataFrames on a specified key column.

# Example:
df2 = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Salary': [50000, 60000]})
merged = df.merge(df2, on='Name')
print(merged)

# 5. df.join(other_df): Joins two DataFrames on their indexes.

# Example:
df3 = pd.DataFrame({'Salary': [50000, 60000, 70000, 80000]})
joined = df.join(df3)
print(joined)

# 6. pd.concat([df1, df2]): Concatenates two or more DataFrames along rows or columns.

# Example:
df_concat = pd.concat([df, df2], axis=1)
print(df_concat)

# 7. df._append(df2): Append rows of other to the end of caller, returning a new object.
# Columns in other that are not in the caller are added as new columns, for existing rows values will be NULL.

df_append = df._append(df2)
print(df_append)

# 8. df.pivot_table(values, index, columns): Creates a pivot table for data summarization.

# Example:
df4 = pd.DataFrame({'foo': ['one', 'one', 'one', 'two', 'two', 'two'],
                   'bar': ['A', 'B', 'C', 'A', 'B', 'C'],
                   'baz': [1, 2, 3, 4, 5, 6],
                   'zoo': ['x', 'y', 'z', 'q', 'w', 't']})

print(df4.pivot(index='foo', columns='bar', values='baz'))

# Data Transformation
print('\n', 'Data Transformation', '\n')

data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex'],
        'Age': [45, 25, 33, 36],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York']}
df = pd.DataFrame(data)
age_series = pd.Series([25, 30, 35], name='Age')

# 1. df.apply(function): Applies a function to rows or columns.

# Example:
df['Age in Months'] = df['Age'].apply(lambda x: x * 12)
print(df)
# df['Age in Months'] - adds a new column with calculated values

# 2. df.map(function): Applies element-wise functions to a Series.

# Example:
age_series = age_series.map(lambda x: x * 10)
print(age_series)

# 3. df.replace(to_replace, value): Replaces specific values in the DataFrame.

# Example:
df['City'] = df['City'].replace('New York', 'NYC')
print(df)

# 4. df.drop_duplicates(): Removes full duplicate rows.

# Example:
df_with_duplicates = pd.DataFrame({'Name': ['Alice', 'Bob', 'Alice'], 'Age': [25, 30, 25]})
print(df_with_duplicates.drop_duplicates())

# 5. df.astype(dtype): Changes the data type of all columns.
# 6. df.astype({'col1': dtype}): Changes the data type of the col1 column.
# 7. df['col1] = df['col1'].astype(dtype): The additional way to change the data type of the col1 column.

# Example:

print(df_with_duplicates.astype('string').dtypes)
print(df_with_duplicates.astype({'Age': 'string'}).dtypes)
df_with_duplicates['Age'] = df_with_duplicates['Age'].astype('string')
print(df_with_duplicates.dtypes)

# 8. df['new_column'] = 'const' - Creates a new column 'new_column' with the 'const' value for all rows.

# Example:
df['new_column'] = 'const'
print(df)

# 9. df[['id', 'type']] = df['type_id'].str.split(“*”, expand=True) - Splits the column into two new ones.

# Example:
data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex'],
        'type_id': ['A*1', 'B*2', 'C*3', 'D*4']}
df = pd.DataFrame(data)
df[['id', 'type']] = df['type_id'].str.split('*', expand=True)
print(df)

# 10.  df['column_name'].round()

# Example:
data = {'Name': ['Alice', 'Bob'],
        'salary': [100.1, 200.5]}
df = pd.DataFrame(data)

df['salary'] = df['salary'].round()
print(df)

# 11. df[df[column_name < value]] - filters df by values in the column column_name

# Example:
df = df[df['salary'] < 150]
print(df)


# Statistical Functions
print('\n', 'Statistical Functions', '\n')

data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex'],
        'Age': [45, 25, 33, 36],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York']}
df = pd.DataFrame(data)
age_series = pd.Series([25, 30, 35], name='Age')

# 1. df.mean(): Calculates the mean of each column.

# Example:
print(df['Age'].mean())

# 2. df.median(): Calculates the median of each column.

# Example:
print(df['Age'].median())

# 3. df.sum(): Returns the sum of values in each column.

# Example:
print(df['Age'].sum())

# 4. df.min() / df.max(): Calculates the minimum/maximum value for each column.

# Example:
print(df['Age'].min(), df['Age'].max())

# 5. df.corr(): Returns the correlation between columns.

# Example:
df_stats = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
print(df_stats.corr())

# 6. df.count(): Counts non-NA/null values for each column.
# 7. df.shape[0]: Counts the number of df rows.
# 8. series.shape[0]: ounts the number of Series rows.

# Example:
print(df.count())
print(df.shape[0])
print(age_series.shape[0])

# 9. df['column_name'].value_counts() - counts unique values in the column, result is Series so reset_index() for df

# Example:
counts = df['City'].value_counts()
print(type(counts))


# Visualization
print('\n', 'Visualization', '\n')

# 1. df.plot(): Creates basic plots using matplotlib (line, bar, histogram, etc.).
# 2. df.hist(column): Creates a histogram for a specific column.
# 3. df.boxplot(column): Generates a box plot.


# Advanced Functions
print('Advanced Functions')

# 1. df.pivot(index, columns, values): Reshapes data by pivoting.

# Example:
data = {'Name': ['Alice', 'Alice', 'Bob', 'Bob'],
        'Category': ['A', 'B', 'A', 'B'],
        'Score': [90, 85, 80, 95]}
df_advanced = pd.DataFrame(data)
pivot_table = df_advanced.pivot_table(values='Score', index='Name', columns='Category')
print(pivot_table)

# 2. df.melt(): Unpivots a DataFrame from a wide format to a long format.

# Example:
melted = df_advanced.melt(id_vars=['Name'], value_vars=['Score'])
print(melted)

# 3. df.sample(n): Randomly samples n rows from the DataFrame.

# Example:
print(df_advanced.sample(2))

# 4. df['column_name'].sample(n): Randomly samples n values as Series from the DataFrame column column_name.

# Example:
print(df_advanced['Score'].sample(n=2))
# We can use random_state=1 to ensure the reproducibility of the examples
print(df_advanced['Score'].sample(n=2, random_state=1))
