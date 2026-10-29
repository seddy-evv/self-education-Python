"""Read a large json file."""
# Option 1 using ijson:
import ijson

def process_large_json(file_path):
    """
    Incrementally processes a large JSON file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Parse the JSON file incrementally.
            # You can adjust `ijson.keys`, `ijson.items`, or other functions based on the JSON structure.\
            # The ijson.items() method enables streaming JSON objects one at a time without loading
            # the full JSON file into memory.
            parser = ijson.items(f, 'item')  # Assumes the JSON is an array of objects.

            for count, item in enumerate(parser, 1):
                # Process each item here
                print(f'Processing record #{count}: {item}')

                # You can perform additional transformations or store output here.
                # Add your processing logic based on the structure of the JSON records.

    except Exception as e:
        print(f"An error occurred: {e}")

json_file_path = 'large_file.json'  # Replace with the path to your JSON file
process_large_json(json_file_path)

# Option 2 using pandas
import pandas as pd
chunk_size = 5000
# read_json returns an iterable reader object when chunksize is provided
json_reader = pd.read_json("large_output.json", lines=True, chunksize=chunk_size)

for chunk in json_reader:
    # 'chunk' is a standard pd.DataFrame containing 5000 rows
    print(f"Processing chunk with shape: {chunk.shape}")
    # Perform your data cleaning or mutations here


"""Writing a large json file."""
import numpy as np

# Creating a large dummy DataFrame
large_df = pd.DataFrame({"ID": range(100000), "Value": np.random.randn(100000)})

chunk_size = 10000
with open("large_output.json", "w", encoding="utf-8") as f:
    for i in range(0, len(large_df), chunk_size):
        chunk = large_df.iloc[i: i + chunk_size]
        # lines=True creates the newline-delimited JSON format
        chunk_json = chunk.to_json(orient="records", lines=True)
        f.write(chunk_json + "\n")
