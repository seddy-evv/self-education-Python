# With @dataclass decorator __init__ magic method will be automatically created under the hood, also we can
# specify default values for attributes and we should specify types, and if we pass different data type a TypeError
# will be raised.

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


@dataclass
class Team:
    """Represents a team."""
    name: str
    members: List[Person] = field(default_factory=list)  # Initialize as an empty list

    def add_member(self, person: Person) -> None:
        """Add a person to the team."""
        self.members.append(person)

    def team_info(self) -> str:
        """Get information about the team and its members."""
        member_names = ', '.join([member.name for member in self.members])
        return f"Team: {self.name}\nMembers: {member_names}"


if __name__ == "__main__":
    alice = Person(name="Alice", age=30, email="alice@example.com")
    bob = Person(name="Bob", age=25)
    print(alice.greet())
    print(bob.greet())

    book = Book(title="Python Guide", author="John Doe", pages=300, genres=["Programming", "Education"])
    print(book.summary())

    point = ImmutablePoint(x=3.0, y=4.0)
    print(f"Point distance to origin: {point.distance_to_origin()}")

    team = Team(name="Developers")
    team.add_member(alice)
    team.add_member(bob)
    print(team.team_info())
