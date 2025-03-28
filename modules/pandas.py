# Python pandas is a powerful library for data manipulation, analysis, and visualization. It provides a range
# of functions to work with data efficiently. Here's a description of the main pandas functions:

import pandas as pd


def get_pd_df():
    data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex'],
            'Age': [45, 25, 33, 36],
            'City': ['New York', 'Los Angeles', 'Chicago', 'New York']}
    df = pd.DataFrame(data)

    return df


def get_age_series():

    return pd.Series([25, 30, 35], name='Age')


# Data Creation
print('\n', 'Data Creation', '\n')

# 1. pd.DataFrame(data): Creates a DataFrame from a dictionary, list, or another structure. A DataFrame is
# a two-dimensional, tabular data structure with labeled axes (rows and columns).

# Example:
df = get_pd_df()
print(df)

# 2. pd.Series(data): Creates a one-dimensional labeled array (like a column in a DataFrame).

# Example:
age_series = get_age_series()
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

df = get_pd_df()

# 1. .loc[]: Access rows/columns by labels or boolean conditions.

# Example:
df_loc = pd.DataFrame([[1, 2,  3], [4, 5, 6], [7, 8, 9]],
                      index=['cobra', 'viper', 'sidewinder'],
                      columns=['max_speed', 'shield', 'wheel'])
print(df)

# returns Series with specified index
df_2 = df_loc.loc['viper']
print(type(df_2))
print(df_2)

# returns DataFrame with specified indexes
df_3 = df_loc.loc[['viper', 'sidewinder'], ['max_speed', 'wheel']]
print(type(df_3))
print(df_3)

# returns Dataframe with specified columns
df_4 = df_loc.loc[:, ['max_speed', 'wheel']]
print(type(df_4))
print(df_4)

# returns value with specified column and index
df_5 = df_loc.loc['cobra', 'shield']
print(type(df_5))
print(df_5)

# returns Series with specified slice with labels for now and single label for column
df_6 = df_loc.loc['cobra': 'viper', 'max_speed']
print(type(df_6))
print(df_6)

# 2. .iloc[]: Access rows/columns by integer positions.

# Example:
print(df.iloc[1])  # Second row
print(df.iloc[0]['Name'])  # Access a single value in first row and Name column.

# 3. .at[]: Access a single value by row/column labels.

# Example:
print(df.at[1, "Name"])

# 4. .iat[]: Access a single value by integer positions.

# Example:
print(df.iat[1, 0])  # 1 - row, 0 - column

# 5. df['column_name']: Selects a specific column as Series.

# Example:
print(df['Name'])

# 6. df[['column_name']]: Selects a specific column as Dataframe

print(df[['Name']])

# 7. df.squeeze('columns'): Squeezes DataFrames with a single column or a single row to a Series

# Example:
print(df[['Name']].squeeze('columns'))

# 8. df.iloc[start:end]: Slice rows or columns.

# Example:
print(df.iloc[0:2])


# Data Exploration
print('\n', 'Data Exploration', '\n')

df = get_pd_df()

# 1. df.head(n): Returns the first n rows of the DataFrame (default is 5 rows).

# Example:
print(df.head(1))

# 2. df.tail(n): Returns the last n rows of the DataFrame (default is 5 rows).

# Example:
print(df.tail(1))

# 3. df.info(): Provides an overview of the DataFrame, including schema, column types and non-null counts.

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

# 9. df.empty: Checks that the dataframe is empty or not, and returns True or False

print(df.empty)

# 10. df.memory_usage(): Returns the memory usage of each column in bytes.

print(df.memory_usage())


# Data Cleaning
print('\n', 'Data Cleaning', '\n')

df_with_na = pd.DataFrame({'Name': ['Alice', 'Bob', None], 'Age': [25, None, 35]})

# 1. df.dropna(): Removes rows or columns with missing values (None, NA, NULL).
#    df.dropna(inplace=True) - whether to modify the DataFrame than creating a new one (you don't need a new variable)

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

# 5. df.drop(columns='column_name'): Drops a specific column, in some cases we have to specify axis

# Example:
print(df_with_na.drop(columns='Name'))
print(df_with_na.drop(['Name'], axis=1))

# 6. df.drop([0, 1]): drops a row by index

# Example:
print(df_with_na.drop([0]))

# 7. df.rename(columns={'old_name': 'new_name'}): Renames one or more columns.

# Example:
renamed_df = df_with_na.rename(columns={'Name': 'Full Name', 'Age': 'Years'})
print(renamed_df)
# Additional method how to change column names:
renamed_df.columns = ['Full Name New', 'Years New']
print(renamed_df)


# Data Manipulation
print('\n', 'Data Manipulation', '\n')

df = get_pd_df()

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

# Example1:
df2 = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Salary': [50000, 60000]})
merged = df.merge(df2, on='Name')
print(merged)

# Example2:
# left merge (in this case, all columns from df2 will be in the result, except for those with matching names
# you need to specify them in the merge condition)
merged_left = df.merge(df2, on='Name', how='left')
print(merged_left)

# Example3:
# if the column names for the merge are different, you can do as below and then drop the unnecessary ones and fill
# in null for the left join
df2 = pd.DataFrame({'name_column': ['Alice', 'Bob'], 'Salary': [50000, 60000]})
merged_left = df.merge(df2, left_on=['Name'], right_on=['name_column'], how='left')
merged_left = merged_left.drop(['name_column'], axis=1)
merged_left['Salary'] = merged_left['Salary'].fillna(0)
print(merged_left)

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

df = get_pd_df()
age_series = get_age_series()

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

# 10. df['column_name'].round()

# Example:
data = {'Name': ['Alice', 'Bob'],
        'salary': [100.1, 200.5]}
df = pd.DataFrame(data)

df['salary'] = df['salary'].round()
print(df)

# 11. df[df[column_name < value]] - filters df by values in the column column_name
#     df[df[column_name].isin(['1', '2', '3'])]  - filters values in a column by inclusion in an array
#     df.query("column_name not in ('', '4', '5')") - filters values with query
#     df_with_na[df_with_na['Name'].isnull()] - selects only rows with null values in the particular column

# Example:
data = {'Name': ['Alice', 'Bob', 'Charlie', 'Alex', 'Jimmy'],
        'Age': [45, 25, 33, 36, None],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'Boston']}
df = pd.DataFrame(data)
df_age = df[df['Age'] < 35]
print(df_age)
df_age = df[(df['Age'] < 40) & (df['Name'] != 'Charlie')]
print(df_age)
df_city = df[df['City'].isin(['New York', 'Los Angeles'])]
print(df_city)
df_query = df.query("Name not in ('', 'Bob')")
print(df_query)
df_nulls = df[df['Age'].isnull()]
print(df_nulls)


# Statistical Functions
print('\n', 'Statistical Functions', '\n')

df = get_pd_df()
age_series = get_age_series()

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
# 8. series.shape[0]: Counts the number of Series rows.

# Example:
print(df.count())
print(df.shape[0])
print(age_series.shape[0])

# 9. df['column_name'].value_counts() - counts unique values in the column, result is Series so reset_index() for df

# Example:
counts = df['City'].value_counts()
print(type(counts))

# 10. df['column_name'].unique() - returns unique values based on the column name

# Example:
unique = df['City'].unique()
print(unique)


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


# Datetime Handling
print('\n', 'Datetime Handling', '\n')

# 1. pd.to_datetime(series): Converts a column or Series to datetime format.

# Example:

df_date = pd.DataFrame({'Date': ['2023-01-01', '2023-02-01', '2023-03-01']})
df_date['Date'] = pd.to_datetime(df_date['Date'])
print(df_date)
print(df_date.dtypes)

# 2. df['date_column'].dt.year: Extracts the year from a datetime column.
# 3. df['date_column'].dt.month: Extracts the month from a datetime column.
# 4. df['date_column'].dt.day: Extracts the day from a datetime column.

# Example:

df_date['Year'] = df_date['Date'].dt.year
df_date['Month'] = df_date['Date'].dt.month
df_date['Day'] = df_date['Date'].dt.day
print(df_date)

# 5. df.resample(rule): Convenience method for frequency conversion and resampling of time series, asfreq() - is needed
# to choose intervals, we can replace asfreq() with agregation function, fillna(0) - is needed to fill NaN

# Example1:
start_time = '2024-01-01'
end_time = '2024-01-08'
future = pd.DataFrame([[start_time, 0], [end_time, 0]], columns=['time', 'value'])
future['time'] = pd.to_datetime(future['time'])
# sets df column as index
future = future.set_index('time')
print('future before resample:')
print(future)
future = future.resample('d').asfreq().fillna(0)
print('future after resample:')
print(future)

# Example2:
# an example to create pandas DatetimeIndex
index = pd.date_range('1/1/2000', periods=9, freq='min')
series = pd.Series(range(9), index=index)
sum_series = series.resample('3min').sum().fillna(0)
print(sum_series)
interval_series = series.resample('3min').asfreq()
print(interval_series)


# Miscellaneous
print('\n', 'Miscellaneous', '\n')

df = get_pd_df()
series = pd.Series([1, 2, 3, 4], name='foo',
                   index=pd.Index(['a', 'b', 'c', 'd'], name='idx'))

# 1. pd.get_dummies(data): Converts categorical data into dummy/indicator variables.

# Example:
df_dummy = pd.get_dummies(df['City'])
print(df_dummy)

# 2. df.set_index('column_name'): Sets one column as the index.

# Example:
df_indexed = df.set_index('Name')
print(df_indexed)

# 3. df.reset_index(): Resets the index to default integer numbering.
#    df.reset_index(drop=True) - We can use the drop parameter to avoid the old index being added as column, default False
#    df.reset_index(inplace=True) - whether to modify the DataFrame than creating a new one
#    series.reset_index(): Resets the index to default integer numbering for Series. Without drop=True returns
#    DataFrame instead of Series.

# Example:
reset_df = df_indexed.reset_index()
print(reset_df)
reset_df_without_Name = df_indexed.reset_index(drop=True)
print(reset_df_without_Name)
reset_df_from_series = series.reset_index()
print(reset_df_from_series)


# Categorical data
print('\n', 'Categorical data', '\n')

# Working with categorical data in pandas is highly efficient due to its Categorical data type. Categorical
# data is a type of variable that represents a fixed number of possible values like labels or groups (e.g.,
# gender, colors, grades). These variables can be either ordered or unordered. Categorical data reduces memory
# usage and improves computation performance compared to using strings.

# Example:
# Categorical variables are faster for grouping. For example:
df = pd.DataFrame({
    'Color': ['red', 'blue', 'green', 'red', 'blue'],
    'Value': [10, 20, 30, 40, 50]
})
df['Color'] = df['Color'].astype('category')

grouped = df.groupby(['Color'], observed=True)['Value'].sum()
print(grouped)

# Categorical data uses less memory compared to object types (strings). You can compare memory usage like this:
df = pd.DataFrame({'Color': ['red', 'blue', 'green', 'red', 'blue']})

print(df.memory_usage(deep=True))  # Memory usage with object type

df['Color'] = df['Color'].astype('category')
print(df.memory_usage(deep=True))  # Memory usage after conversion to category
