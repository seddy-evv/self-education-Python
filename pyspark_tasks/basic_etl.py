from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when


class Extractor:
    """
    Class responsible for extracting data into a Spark DataFrame.
    """
    def __init__(self, spark):
        self.spark = spark

    def extract_from_csv(self, file_path):
        """
        Extract data from a CSV file.

        :param file_path: Path to the input CSV file.
        :return: Spark DataFrame with extracted data.
        """
        try:
            df = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
            print("Data extraction completed.")
            return df
        except Exception as e:
            print(f"Error while extracting data: {e}")
            raise


class Transformer:
    """
    Class responsible for transforming the Spark DataFrame.
    """
    @staticmethod
    def clean_data(df):
        """
        Clean data by trimming whitespace and handling null values.

        :param df: Input DataFrame.
        :return: Cleaned DataFrame.
        """
        print("Cleaning data...")
        df_cleaned = df.withColumn("name", trim(col("name"))) \
                       .fillna({"name": "Unknown", "age": None})
        return df_cleaned

    @staticmethod
    def add_age_category(df):
        """
        Add a new column 'age_category' based on the 'age' column.

        :param df: Input DataFrame.
        :return: Transformed DataFrame with 'age_category' column.
        """
        print("Adding age category...")
        df_transformed = df.withColumn(
            "age_category",
            when(col("age") < 18, "Minor")
            .when((col("age") >= 18) & (col("age") < 60), "Adult")
            .when(col("age") >= 60, "Senior")
            .otherwise("Unknown")
        )
        return df_transformed


class Loader:
    """
    Class responsible for loading the transformed Spark DataFrame.
    """
    @staticmethod
    def load_to_parquet(df, output_path):
        """
        Write the transformed DataFrame to a Parquet file.

        :param df: Transformed DataFrame.
        :param output_path: Output path for the Parquet file.
        """
        try:
            df.write.mode("overwrite").parquet(output_path)
            print("Data successfully loaded to Parquet file.")
        except Exception as e:
            print(f"Error while loading data: {e}")
            raise


class ETL:
    """
    Class to orchestrate the Extract, Transform, and Load process.
    """
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self, input_path, output_path):
        """
        Run the ETL process: extract, transform, and load.

        :param input_path: Path to the input file.
        :param output_path: Path to the output file/destination.
        """
        print("Starting ETL pipeline...")

        # Step 1: Extract
        print("Extracting data...")
        df = self.extractor.extract_from_csv(input_path)

        # Step 2: Transform
        print("Transforming data...")
        df_cleaned = self.transformer.clean_data(df)
        df_transformed = self.transformer.add_age_category(df_cleaned)

        # Step 3: Load
        print("Loading data...")
        self.loader.load_to_parquet(df_transformed, output_path)

        print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PythonETLApp") \
        .getOrCreate()

    # Define file paths
    input_file_path = "data.csv"  # Adjust path to your input CSV file
    # name, age
    # Alice, 25
    # Bob, 17
    # Charlie, 65
    # , 30
    output_file_path = "output_data.parquet"  # Path to the output Parquet file

    # Instantiate ETL pipeline components
    extractor = Extractor(spark)
    transformer = Transformer()
    loader = Loader()

    # Create and run the ETL pipeline
    etl_process = ETL(extractor, transformer, loader)
    etl_process.run(input_file_path, output_file_path)

    # Stop Spark session
    spark.stop()
