import json


# 1. json.dump(obj, fp, **kwargs)
# Serializes a Python object (obj) to a JSON formatted string and writes it directly to a file-like object (fp).

data = {"name": "Alice", "age": 25, "city": "New York"}

with open("data.json", "w") as file:
    json.dump(data, file, indent=4)  # Save `data` to `data.json` with pretty formatting

# 2. json.dumps(obj, **kwargs)
# Serializes a Python object (obj) to a JSON-formatted string and returns it.
data = {"name": "Alice", "age": 25, "city": "New York"}

json_string = json.dumps(data, indent=4)  # Convert Python dictionary to JSON string
print(json_string)
# {
#     "name": "Alice",
#     "age": 25,
#     "city": "New York"
# }

# 3. json.load(fp, **kwargs)
# Deserializes (parses) a JSON object from a file-like object (fp) and converts it to a Python object.

with open("data.json", "r") as file:
    data = json.load(file)  # Load JSON data from `data.json` as a Python dictionary

print(data)
# {'name': 'Alice', 'age': 25, 'city': 'New York'}

# 4. json.loads(s, **kwargs)
# Deserializes (parses) a JSON-formatted string (s) and converts it to a Python object (e.g., dict, list, etc.).

json_string = '{"name": "Alice", "age": 25, "city": "New York"}'

data = json.loads(json_string)  # Convert JSON string to Python dictionary
print(data)
# {'name': 'Alice', 'age': 25, 'city': 'New York'}

# Summary of Usage:
# json.dump: Write JSON to a file.
# json.dumps: Create JSON string from Python object.
# json.load: Read JSON from a file and convert to Python object.
# json.loads: Convert JSON string to Python object.
