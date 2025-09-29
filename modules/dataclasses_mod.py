from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Person:
    """Represents a person."""
    name: str
    age: int
    email: Optional[str] = None  # Optional field with default value None

    def greet(self) -> str:
        """A method for the person to greet."""
        return f"Hello, my name is {self.name} and I am {self.age} years old."
