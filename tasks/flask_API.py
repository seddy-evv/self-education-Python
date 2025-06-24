from flask import Flask, jsonify, request

app = Flask(__name__)

# Sample data to simulate a database
items = [
    {"id": 1, "name": "item1", "price": 100},
    {"id": 2, "name": "item2", "price": 200},
]


# Get all items
@app.route('/items', methods=['GET'])
def get_items():
    return jsonify(items)


# Get a specific item by id
@app.route('/items/<int:item_id>', methods=['GET'])
def get_item(item_id):
    item = next((item for item in items if item["id"] == item_id), None)
    if item:
        return jsonify(item)
    else:
        return jsonify({"message": "Item not found"}), 404


# Create a new item
@app.route('/items', methods=['POST'])
def create_item():
    new_item = request.json
    new_item["id"] = len(items) + 1  # Generate a new id
    items.append(new_item)
    return jsonify(new_item), 201


# Update an existing item
@app.route('/items/<int:item_id>', methods=['PUT'])
def update_item(item_id):
    item = next((item for item in items if item["id"] == item_id), None)
    if item:
        updated_data = request.json
        item.update(updated_data)
        return jsonify(item)
    else:
        return jsonify({"message": "Item not found"}), 404


# Delete an item
@app.route('/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    global items
    items = [item for item in items if item["id"] != item_id]
    return jsonify({"message": "Item deleted"})


# Run the server
if __name__ == '__main__':
    app.run(debug=True)


# Run the app(bash):
# python app.py

# The API will start on http://127.0.0.1:5000.

# Endpoints:
# GET /items: Retrieve all items.
# GET /items/<item_id>: Retrieve a single item by ID.
# POST /items: Create a new item (send JSON payload, e.g., {"name": "item3", "price": 300}).
# PUT /items/<item_id>: Update an item by ID (send JSON payload, e.g., {"name": "item1_updated", "price": 150}).
# DELETE /items/<item_id>: Delete an item by ID.


# Example Requests Using curl:

# Get all items(bash):
# curl -X GET http://127.0.0.1:5000/items

# Get an item by ID(bash):
# curl -X GET http://127.0.0.1:5000/items/1

# Create a new item(bash):
# curl -X POST -H "Content-Type: application/json" -d '{"name": "item3", "price": 300}' http://127.0.0.1:5000/items

# Update an existing item(bash):
# curl -X PUT -H "Content-Type: application/json" -d '{"name": "item1_updated", "price": 150}' http://127.0.0.1:5000/items/1

# Delete an item(bash):
# curl -X DELETE http://127.0.0.1:5000/items/1
