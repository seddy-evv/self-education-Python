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
