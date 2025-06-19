from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Union

app = FastAPI()


# Pydantic model for input validation and structure
class Item(BaseModel):
    name: str
    price: float
    description: str = None


# In-memory database simulation
items = [
    {"id": 1, "name": "item1", "price": 100.0, "description": "First item"},
    {"id": 2, "name": "item2", "price": 200.0, "description": "Second item"},
]


# Get all items
@app.get("/items")
def get_items():
    return items


# Get a specific item by id
@app.get("/items/{item_id}")
def get_item(item_id: int, q: Union[str, None] = None):
    # Open your browser at http://127.0.0.1:8000/items/1?q=somequery
    # in this case q will be "somequery"
    item = next((item for item in items if item["id"] == item_id), None)
    if item:
        return item
    raise HTTPException(status_code=404, detail="Item not found")


# Create a new item
@app.post("/items", status_code=201)
def create_item(item: Item):
    new_item = {"id": len(items) + 1, "name": item.name, "price": item.price, "description": item.description}
    items.append(new_item)
    return new_item


# Update an existing item
@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    index = next((index for index, existing_item in enumerate(items) if existing_item["id"] == item_id), None)
    if index is not None:
        items[index] = {"id": item_id, "name": item.name, "price": item.price, "description": item.description}
        return items[index]
    raise HTTPException(status_code=404, detail="Item not found")


# Delete an item
@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    global items
    items = [item for item in items if item["id"] != item_id]
    return {"message": "Item deleted"}


# Run the application if executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

# Run the app(bash):
# uvicorn main:app --reload

# The API will start on http://127.0.0.1:8000

# Endpoints:
# GET /items: Retrieve all items.
# GET /items/{item_id}: Retrieve a specific item by its ID.
# POST /items: Create a new item (requires a JSON body like {"name": "item3", "price": 300, "description": "Third item"}).
# PUT /items/{item_id}: Update an existing item by ID (requires a JSON body like {"name": "new_name", "price": new_price, "description": "new_description"}).
# DELETE /items/{item_id}: Delete an item by its ID.


# Example Requests Using curl:

# Get all items(bash):
# curl -X GET http://127.0.0.1:8000/items

# Get an item by ID(bash):
# curl -X GET http://127.0.0.1:8000/items/1

# Create a new item(bash):
# curl -X POST -H "Content-Type: application/json" -d '{"name": "item3", "price": 300, "description": "Third item"}' http://127.0.0.1:8000/items

# Update an existing item(bash):
# curl -X PUT -H "Content-Type: application/json" -d '{"name": "updated_name", "price": 400, "description": "Updated description"}' http://127.0.0.1:8000/items/1

# Delete an item(bash):
# curl -X DELETE http://127.0.0.1:8000/items/2
