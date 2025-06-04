# 1. Using pandas
import pandas as pd

# File path
file_path = "path/to/your/large_file.csv"

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your system's memory

# Initialize an empty list if you need to store processed chunks (optional)
# processed_chunks = []

# If the file is compressed (.zip, .gz), Pandas can automatically handle compressed CSV files:
# pd.read_csv("large_file.csv.gz", chunksize=100000)

# Process the large CSV in chunks
try:
    chunk_counter = 0
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Increment chunk counter
        chunk_counter += 1
        print(f"Processing chunk {chunk_counter}...")

        # Example: Perform operations on the chunk
        # Uncomment and modify as needed for your use case
        # chunk = chunk[chunk['some_column'] > some_value]  # Example filter

        # Example: Append processed DataFrame to a list (optional)
        # processed_chunks.append(chunk)

        # Example: Save the chunk to a separate smaller CSV (optional)
        chunk.to_csv(f"output_chunk_{chunk_counter}.csv", index=False)

except Exception as e:
    print(f"An error occurred: {e}")

print("Processing completed.")


# 2. Using csv.reader
import csv

# File path to the large CSV file
file_path = "path/to/your/large_file.csv"

# Initialize a counter to track rows (optional)
row_counter = 0

# Define the chunk size (number of rows to process at a time)
chunk_size = 100000  # Adjust this based on your use case

# Open the CSV file
try:
    with open(file_path, mode='r') as file:
        # Use csv.reader to read the file
        # It is only a pointer it will not load the whole file in the memory!!!
        csv_reader = csv.reader(file)

        # Fetch the header (if your file has one)
        header = next(csv_reader)
        print(f"Header: {header}")

        # Process rows in chunks
        chunk = []  # Temporary storage for a chunk of rows
        # The csv module is excellent for reading and writing CSV files, is memory efficient
        # (providing an iterator that reads only one line from the file into memory at a time)
        for row in csv_reader:
            row_counter += 1
            chunk.append(row)  # Add row to the current chunk

            # If the chunk reaches the defined size, process it
            if len(chunk) == chunk_size:
                print(f"Processing chunk {row_counter // chunk_size}")
                # Perform operations on the chunk (e.g., processing or saving)
                # Example: Print the first row of the chunk
                print(f"First row of this chunk: {chunk[0]}")

                # Clear the chunk for the next set of rows
                chunk = []

        # Process any remaining rows in the last chunk
        if chunk:
            print(f"Processing final chunk (rows {row_counter - len(chunk) + 1} to {row_counter})")
            # Perform operations on the final chunk
            print(f"First row of the final chunk: {chunk[0]}")

    print(f"Finished processing {row_counter} rows.")
except Exception as e:
    print(f"An error occurred: {e}")


# 3. Using csv.DictReader and generator
# csv.DictReader - Create an object that operates like a regular reader but maps the information in each row to a dict
# whose keys are given by the optional fieldnames parameter. If fieldnames is omitted, the values in the first row
# of file f will be used as the fieldnames and will be omitted from the results.
import csv

FUNDING = 'data/funding.csv'
#  The function “returns” a generator, thanks to the yield statement in the function definition. This means, among
#  other things, that the data is evaluated lazily. The file is not opened, read, or parsed until you need it.
#  No more than one row of the file is in memory at any given time. The context manager in the function also
#  ensures that the file handle is closed when you've finished reading the data, even in the face of an
#  exception elsewhere in code.
def read_funding_data(path):
    with open(path, 'rU') as data:
        reader = csv.DictReader(data)
        for row in reader:
            yield row

for idx, row in enumerate(read_funding_data(FUNDING)):
    print(idx, row)
