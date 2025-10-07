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

