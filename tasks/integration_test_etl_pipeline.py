# Integration Test Setup
# 1. Prerequisites:
# Source Test Data: A sample CSV file, e.g., sales_data.csv.
# Target Database: A clean, empty table to load the transformed sales data, e.g., sales_summary.
#
# 2. Sample Source Data: ("sales_data.csv")
# SaleID, SaleDate, Product, Quantity, UnitPrice
# 101,  2023-01-01, WidgetA, 10,       5.99
# 102,  2023-01-02, WidgetB,   ,       15.99
# 103,  2023/01/03, WidgetC,  5,       9.99
#
# 3. Expected Final Data in Target Database ("sales_summary")
# SaleID, SaleDate, Product, TotalSaleAmount
# 101,  2023-01-01, WidgetA, 59.90
# 103,  2023-01-03, WidgetC, 49.95
#
# Row with SaleID 102 is excluded due to missing Quantity (invalid entry).
# SaleDate is correctly reformatted from %Y/%m/%d to %Y-%m-%d (uniform date format).
# TotalSaleAmount is calculated as Quantity * UnitPrice.

# Integration Test Steps
# 1. Set Up Test Environment
# Load the sample CSV file (sales_data.csv) into the source data location (e.g., S3 bucket, or staging directory).
# Ensure the target database (sales_summary) is empty.
#
# 2.Run the ETL Pipeline
# Execute the ETL pipeline end-to-end, e.g., by triggering the workflow or script.
#
# 3.Verify Results
# Query the target database (sales_summary) and retrieve the loaded data.
# Compare the retrieved data from the database with the expected data.


import pandas as pd
import sqlite3


def test_etl_pipeline():
    # Connect to the target database
    conn = sqlite3.connect("test_database.db")
    cursor = conn.cursor()

    # Expected data after the ETL pipeline
    expected_data = [
        (101, '2023-01-01', 'WidgetA', 59.90),
        (103, '2023-01-03', 'WidgetC', 49.95)
    ]

    # Query the target table after ETL
    cursor.execute("SELECT * FROM sales_summary")
    loaded_data = cursor.fetchall()

    # Assert that the loaded data matches the expected data
    assert loaded_data == expected_data, f"Loaded data does not match! {loaded_data} != {expected_data}"

    # Close connection
    conn.close()

    print("ETL Pipeline Integration Test Passed!")


# Run the integration test
test_etl_pipeline()
