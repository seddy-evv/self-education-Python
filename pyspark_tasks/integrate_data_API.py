# If we need to integrate data from an external API into our PySpark pipeline.
# This is how we can achieve this:

# 1. Fetch Data from the API
# Use Python's HTTP libraries (e.g., requests) to fetch data from the external API.
# Parse the API response (e.g., JSON or XML) into a Python data structure (e.g., a list of dictionaries).
import requests
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Fetch data from the API
api_url = "https://api.github.com/repos/seddy-evv/self-education-Python/contributors"
response = requests.get(api_url)
data = response.json()  # Parse JSON response
print(data)
# [{'login': 'seddy-evv', 'id': 79664416, 'node_id': 'MDQ6VXNlcjc5NjY0NDE2',
#   'avatar_url': 'https://avatars.githubusercontent.com/u/79664416?v=4', 'gravatar_id': '',
#   'url': 'https://api.github.com/users/seddy-evv', 'html_url': 'https://github.com/seddy-evv',
#   'followers_url': 'https://api.github.com/users/seddy-evv/followers',
#   'following_url': 'https://api.github.com/users/seddy-evv/following{/other_user}',
#   'gists_url': 'https://api.github.com/users/seddy-evv/gists{/gist_id}',
#   'starred_url': 'https://api.github.com/users/seddy-evv/starred{/owner}{/repo}',
#   'subscriptions_url': 'https://api.github.com/users/seddy-evv/subscriptions',
#   'organizations_url': 'https://api.github.com/users/seddy-evv/orgs',
#   'repos_url': 'https://api.github.com/users/seddy-evv/repos',
#   'events_url': 'https://api.github.com/users/seddy-evv/events{/privacy}',
#   'received_events_url': 'https://api.github.com/users/seddy-evv/received_events',
#   'type': 'User', 'user_view_type': 'public', 'site_admin': False, 'contributions': 73}]
print(len(data))
# 1

# 2. Convert API Data into a Spark DataFrame
# Load the parsed data into a PySpark DataFrame using spark.createDataFrame.

spark = SparkSession.builder \
    .appName("APIIntegration") \
    .getOrCreate()

# Create a DataFrame from the API data
api_df = spark.createDataFrame(data)

# # 3. Transform the API Data
# # Apply standard transformations as needed: filtering, selecting specific fields, or aggregating data.

transformed_api_df = api_df.select(
    col("id").cast("int"),
    col("login"),
    col("user_view_type").alias("view_type")
).filter(col("site_admin") == False)
transformed_api_df.show()
# +--------+---------+---------+
# |      id|    login|view_type|
# +--------+---------+---------+
# |79664416|seddy-evv|   public|
# +--------+---------+---------+

# # 4. Integrate API Data with Other Data Sources
# # Combine the API DataFrame with other DataFrames (e.g., CSV, JSON, or Parquet data) to build a unified dataset
# using operations like union or join.

# other_data = spark.read.format("csv").option("header", "true").load("path/to/other_data.csv")

# integrated_df = transformed_api_df.join(other_data, on="id", how="inner")

# # 5. Load the Final Dataset to a Sink
# # Write the integrated data into a desired output format or location (e.g., file, database, or data warehouse).

# integrated_df.write.format("parquet") \
#     .mode("overwrite") \
#     .save("path/to/output/parquet")

# # 6. Automate and Schedule API Data Fetching
# # Use workflow orchestration tools like Apache Airflow or a periodic scheduler (e.g., cron) to invoke the PySpark
# job periodically and fetch fresh data from the API.
