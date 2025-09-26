# The Python runtime does not enforce function and variable type annotations. They can be used by third party tools such
# as type checkers, IDEs, linters, etc.
# The "typing" module in Python provides support for type hints, which were introduced in PEP 484 and PEP 526.
# Python is a dynamically typed language, meaning you don't explicitly declare variable types. However, type hints
# allow you to add optional static type annotations to your code, improving readability, maintainability,
# and enabling static analysis tools (like MyPy) to catch potential type-related errors before runtime.

from typing import List, Dict, Tuple, Optional, Union

# Typing for variables
name: str = "Alice"                # A string variable
age: int = 30                      # An integer variable
height: float = 5.6                # A floating-point variable
is_active: bool = True             # A boolean variable

scores: List[int] = [85, 90, 78]   # A list of integers
preferences: Dict[str, Union[str, int]] = {"color": "blue", "font_size": 12}  # A dictionary with mixed types

coordinates: Tuple[float, float] = (34.5, 45.2)  # A tuple of two floats

# Optional types: Might be None
middle_name: Optional[str] = None

# Union types: Variable can hold multiple types
data: Union[str, List[str]] = "Hello, world!"


# Typing for functions
# name: str - means the input name must be a str.
# -> str - means the function will return a str.
def greet(name: str) -> str:
    """
    Returns a greeting message for the given name.
    """
    return f"Hello, {name}!"


def sum_numbers(numbers: List[int]) -> int:
    """
    Sums a list of integers and returns the total.
    """
    return sum(numbers)


# Variables or functions can accept multiple types using Union.
def find_coordinates(data: Dict[str, Union[float, Tuple[float, float]]]) -> Tuple[float, float]:
    """
    Extracts coordinates from a dictionary.
    """
    if isinstance(data["location"], float):  # Single float becomes a tuple
        return (data["location"], data["location"])
    return data["location"]


def is_valid_user(age: int, active_status: bool) -> bool:
    """
    Checks if a user is valid based on age and activity status.
    """
    return age > 18 and active_status

# Optional indicates that the inputs and the return value might be None.
def safe_multiply(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """
    Multiplies two optional floating-point numbers safely. If either value is None, returns None.
    """
    if a is None or b is None:
        return None
    return a * b


# Entry point for demonstration
if __name__ == "__main__":
    # Using typed variables
    print(f"Name: {name}")
    print(f"Age: {age}")
    print(f"Height: {height}")
    print(f"Active: {is_active}")
    print(f"Scores: {scores}")
    print(f"Preferences: {preferences}")
    print(f"Middle Name: {middle_name}")
    print(f"Coordinates: {coordinates}")
    print(f"Data: {data}")

    # Function outputs
    print(greet(name))
    print(f"Total Score: {sum_numbers(scores)}")
    print(f"Valid User: {is_valid_user(age, is_active)}")
    print(f"Safe Multiply: {safe_multiply(4.5, None)}")
    print(f"Coordinates: {find_coordinates({'location': (12.3, 45.6)})}")
