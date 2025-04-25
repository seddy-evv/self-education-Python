import unittest
from pyspark.sql import SparkSession


# Pyspark methods to test:
def create_dataframe(spark):
    """Create a sample PySpark DataFrame."""
    data = [
        {"name": "Alice", "age": 25, "city": "New York"},
        {"name": "Bob", "age": 30, "city": "San Francisco"},
        {"name": "Charlie", "age": 35, "city": "New York"},
        {"name": "David", "age": 40, "city": "Chicago"}
    ]
    return spark.createDataFrame(data)


def filter_by_city(df, city):
    """Filter DataFrame by city."""
    return df.filter(df.city == city)


def get_average_age(df):
    """Compute the average age from the DataFrame."""
    return df.groupBy().avg("age").collect()[0][0]


def select_columns(df, columns):
    """Select specific columns from the DataFrame."""
    return df.select(*columns)


# Test class
class TestDataFrameOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Initialize the Spark session once for all test cases."""
        cls.spark = SparkSession.builder.master("local[1]").appName("PySparkUnitTest").getOrCreate()
        # for Databricks replace to:
        # cls.spark = spark

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session."""
        # for Databricks comment out
        cls.spark.stop()

    def setUp(self):
        """Create a sample DataFrame for testing."""
        self.df = create_dataframe(self.spark)

    def test_create_dataframe(self):
        """Test the creation of the DataFrame."""
        self.assertEqual(len(self.df.collect()), 4)  # Assert row count
        self.assertEqual(len(self.df.columns), 3)   # Assert column count (name, age, city)

    def test_filter_by_city(self):
        """Test filtering by city."""
        filtered_df = filter_by_city(self.df, "New York")
        results = [row["name"] for row in filtered_df.collect()]
        self.assertEqual(results, ["Alice", "Charlie"])  # Assert rows filtered correctly

    def test_get_average_age(self):
        """Test calculation of average age."""
        avg_age = get_average_age(self.df)
        self.assertEqual(avg_age, 32.5)  # Assert average age is calculated correctly

    def test_select_columns(self):
        """Test selecting specific columns."""
        selected_df = select_columns(self.df, ["name", "age"])
        self.assertEqual(selected_df.columns, ["name", "age"])  # Assert selected columns match


if __name__ == "__main__":
    unittest.main()
# for Databricks replace to
# r = unittest.main(argv=[''], verbosity=2, exit=False)
# assert r.result.wasSuccessful(), 'Test failed; see logs above'
