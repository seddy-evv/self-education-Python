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

@dataclass
class Book:
    """Represents a book."""
    title: str
    author: str
    pages: int
    genres: List[str] = field(default_factory=list)  # Default empty list

    def summary(self) -> str:
        """Provides a short summary of the book."""
        genre_list = ', '.join(self.genres) if self.genres else "No genres listed"
        return f"{self.title} by {self.author}, {self.pages} pages. Genres: {genre_list}"

@dataclass(frozen=True)  # Makes the dataclass immutable
class ImmutablePoint:
    """Represents an immutable point in 2D space."""
    x: float
    y: float

    def distance_to_origin(self) -> float:
        """Calculate the distance of the point from the origin."""
        return (self.x ** 2 + self.y ** 2) ** 0.5


        
