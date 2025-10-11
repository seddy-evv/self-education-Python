# Pydantic is a data validation library that leverages Python type hints to validate and serialize data objects.
# This example includes functionality such as required and optional fields, default values, validation logic, nested
# models, and custom validators.
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import List, Optional


# Define a basic model for a person
class Person(BaseModel):
    name: str
    age: int = Field(..., ge=0, description="Age must be a non-negative number.")  # Field constraints
    email: Optional[EmailStr] = None  # Optional email with validation

    @field_validator("name")
    def validate_name(cls, name: str) -> str:
        """Ensure the name is not empty."""
        if not name.strip():
            raise ValueError("Name must not be empty.")
        return name


# Define a model for a book
class Book(BaseModel):
    title: str
    author: str
    pages: int = Field(..., ge=1, description="Pages must be a positive integer.")  # Pages must be >= 1
    genres: List[str] = []  # Default is an empty list

    @field_validator("genres", mode="before")
    def validate_genres(cls, genres: List[str]) -> List[str]:
        """Ensure genres list has no empty strings."""
        return [genre for genre in genres if genre.strip()]


# Define a nested model for a team
class Team(BaseModel):
    name: str
    members: List[Person]  # Nested model (list of Person objects)

    def team_info(self) -> str:
        """Get detailed information about the team."""
        member_names = ', '.join([member.name for member in self.members])
        return f"Team name: {self.name}\nMembers: {member_names}"


# Define a model for coordinates with a custom validator
class Coordinates(BaseModel):
    x: float
    y: float

    @field_validator("x", "y")
    def validate_coordinates(cls, value: float) -> float:
        """Ensure coordinates are finite numbers."""
        if not (-10_000 <= value <= 10_000):
            raise ValueError("Coordinates must be between -10,000 and 10,000.")
        return value

    def distance_to_origin(self) -> float:
        """Calculate the distance to the origin (0, 0)."""
        return (self.x ** 2 + self.y ** 2) ** 0.5


if __name__ == "__main__":
    try:
        alice = Person(name="Alice", age=30, email="alice@example.com")
        bob = Person(name="Bob", age=25)
        print(alice)
        print(bob)

        book = Book(title="Python Guide", author="John Doe", pages=300, genres=["Programming", "Education"])
        print(book)

        team = Team(name="Developers", members=[alice, bob])
        print(team.team_info())

        point = Coordinates(x=3.0, y=4.0)
        print(f"Distance to origin: {point.distance_to_origin()} (from {point})")

        # Validation examples
        invalid_person = Person(name="", age=-5)  # Raises validation errors
    except Exception as e:
        print(f"Validation error: {e}")
