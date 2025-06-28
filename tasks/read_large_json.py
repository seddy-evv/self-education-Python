# How to read large json file:
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


if __name__ == "__main__":
    json_file_path = 'large_file.json'  # Replace with the path to your JSON file
    process_large_json(json_file_path)
