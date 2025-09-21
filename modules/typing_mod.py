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

