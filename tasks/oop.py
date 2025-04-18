# Abstraction: Defining a base class with common properties and methods.
from abc import ABC, abstractmethod


class LibraryItem(ABC):
    def __init__(self, title, creator, year):
        self.title = title
        self.creator = creator
        self.year = year

    @abstractmethod
    def display_info(self):
        """
        Abstract method to be implemented by child classes.
        """
        pass


# Inheritance: Creating specialized child classes that inherit from the base class
class Book(LibraryItem):
    def __init__(self, title, author, year, genre):
        super().__init__(title, author, year)
        self.genre = genre

    # Polymorphism: Overriding the base method
    def display_info(self):
        print(f"Book: {self.title}, Author: {self.creator}, Year: {self.year}, Genre: {self.genre}")


class Movie(LibraryItem):
    def __init__(self, title, director, year, duration):
        super().__init__(title, director, year)
        self.duration = duration

    # Polymorphism: Overriding the base method
    def display_info(self):
        print(f"Movie: {self.title}, Director: {self.creator}, Year: {self.year}, Duration: {self.duration} minutes")


class Album(LibraryItem):
    def __init__(self, title, artist, year, tracks):
        super().__init__(title, artist, year)
        self.tracks = tracks

    # Polymorphism: Overriding the base method
    def display_info(self):
        print(f"Album: {self.title}, Artist: {self.creator}, Year: {self.year}, Tracks: {self.tracks}")


# Encapsulation: Using private attributes (e.g., to store items in the library) and controlled access methods
class Library:
    def __init__(self):
        self.__items = []

    def add_item(self, item):
        if isinstance(item, LibraryItem):
            self.__items.append(item)
        else:
            print("Invalid item. Only LibraryItems can be added.")

    def show_items(self):
        print("\nLibrary Items:")
        for item in self.__items:
            item.display_info()


# Instantiate the library
library = Library()

# Create instances of Books, Movies, and Albums
book1 = Book("1984", "George Orwell", 1949, "Dystopian Fiction")
movie1 = Movie("The Matrix", "Wachowski Sisters", 1999, 136)
album1 = Album("Dark Side of the Moon", "Pink Floyd", 1973, 10)

# Add items to the library
library.add_item(book1)
library.add_item(movie1)
library.add_item(album1)

# Show the items in the library
library.show_items()


# Explanation of OOP Principles:

# Encapsulation:
# The Library class encapsulates the list of items (__items) and provides controlled access through
# the add_item and show_items methods.

# Inheritance:
# The Book, Movie, and Album classes inherit from the base LibraryItem class to reuse common properties and methods.

# Polymorphism:
# Each subclass (Book, Movie, Album) overrides the display_info method to provide a specialized implementation
# depending on the item type.

# Abstraction:
# The base class LibraryItem defines a general concept for library items and forces subclasses to implement
# the display_info method, making the design more modular.
