from pydantic import BaseModel, Field, EmailStr, validator
from typing import List, Optional


# Define a basic model for a person
class Person(BaseModel):
    name: str
    age: int = Field(..., ge=0, description="Age must be a non-negative number.")  # Field constraints
    email: Optional[EmailStr] = None  # Optional email with validation

    @validator("name")
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
