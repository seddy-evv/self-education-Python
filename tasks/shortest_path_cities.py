# To solve the task of finding the shortest path between two cities in Python, we can use graph-related algorithms
# like Dijkstra's algorithm or the Breadth-First Search (BFS), depending on the nature of the graph.

# Using Dijkstra's Algorithm (for weighted graphs)
import heapq

def find_shortest_path(city_graph, start_city, target_city):
    # Priority queue to store (cost, city)
    priority_queue = [(0, start_city)]
    # Dictionary to store the shortest distance to each city
    shortest_distances = {city: float('inf') for city in city_graph}
    shortest_distances[start_city] = 0
    # Dictionary to track the path
    previous_city = {city: None for city in city_graph}

    while priority_queue:
        # Pop the city with the smallest distance
        current_cost, current_city = heapq.heappop(priority_queue)

        # If we reached the target city, construct the path
        if current_city == target_city:
            path = []
            while current_city:
                path.append(current_city)
                current_city = previous_city[current_city]
            return path[::-1], current_cost

        # Iterate through neighboring cities
        for neighbor_city, travel_cost in city_graph[current_city]:
            new_cost = current_cost + travel_cost
            if new_cost < shortest_distances[neighbor_city]:
                shortest_distances[neighbor_city] = new_cost
                previous_city[neighbor_city] = current_city
                heapq.heappush(priority_queue, (new_cost, neighbor_city))

    return None, float('inf')  # No path found

# Example usage:
city_graph = {
    "A": [("B", 10), ("C", 15)],
    "B": [("D", 12), ("E", 15)],
    "C": [("F", 10)],
    "D": [("E", 2), ("G", 1)],
    "E": [("G", 5)],
    "F": [("G", 5)],
    "G": []
}

start_city = "A"
target_city = "G"

path, cost = find_shortest_path(city_graph, start_city, target_city)
print(f"Shortest path: {path}, Cost: {cost}")


# Using Breadth-First Search (if graph is unweighted)
from collections import deque

def find_shortest_path_unweighted(city_graph, start_city, target_city):
    queue = deque([(start_city, [start_city])])  # (current city, path)
    visited = set()

    while queue:
        current_city, path = queue.popleft()
        if current_city == target_city:
            return path

        visited.add(current_city)

        for neighbor_city in city_graph.get(current_city, []):
            if neighbor_city not in visited:
                queue.append((neighbor_city, path + [neighbor_city]))

    return None  # No path found

# Example usage:
city_graph_unweighted = {
    "A": ["B", "C"],
    "B": ["D", "E"],
    "C": ["F"],
    "D": ["E", "G"],
    "E": ["G"],
    "F": ["G"],
    "G": []
}

start_city = "A"
target_city = "G"

path = find_shortest_path_unweighted(city_graph_unweighted, start_city, target_city)
print(f"Shortest path: {path}")
