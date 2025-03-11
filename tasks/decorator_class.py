# EXAMPLE 1: Adding a New Method to a Class Using a Class Decorator

# Class decorator definition
def add_method_to_class(cls):
    # Adding a new method to the class
    def new_method(self):
        return f"Hello, I am a method added by a class decorator!"

    # As you can see the decorator runs before the init class method
    print("decorator")
    # Assign the method to the class
    cls.new_method = new_method
    return cls


# Applying the class decorator
@add_method_to_class
class MyClass:
    def __init__(self, name):
        self.name = name
        print("init class method")

    def greet(self):
        return f"Hi, my name is {self.name}!"


# Instantiate the class
obj = MyClass("Alice")
print(obj.greet())  # Output: Hi, my name is Alice!
print(obj.new_method())  # Output: Hello, I am a method added by a class decorator!


# EXAMPLE 2: Automatically Adding a Default Attribute

# Class decorator definition
def add_default_attribute(cls):
    # Modify the class by adding a default attribute
    cls.default_value = 42
    return cls


# Applying the class decorator
@add_default_attribute
class Example:
    def __init__(self):
        self.value = 10


# Instantiate the class
obj = Example()
print(obj.value)           # Output: 10 (existing attribute)
print(obj.default_value)   # Output: 42 (added via the class decorator)


# EXAMPLE 3: Wrapping a Method in the Decorated Class

# Class decorator definition
def modify_method(cls):
    # Save the original method
    original_method = cls.greet

    # Replace it with a decorated method
    def new_greet(self):
        result = original_method(self)  # Call the original method
        return result + " Have a nice day!"

    cls.greet = new_greet
    return cls


# Applying the class decorator
@modify_method
class GreetingClass:
    def greet(self):
        return "Hello!"


# Instantiate the class
obj = GreetingClass()
print(obj.greet())  # Output: Hello! Have a nice day!


# EXAMPLE 4: Implementing additional logic before __init__ method initialization

# Class decorator definition
def add_method_to_class(cls):
    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.info("A new instance will be created")
    return cls


# Applying the class decorator
@add_method_to_class
class MyClass:
    def __init__(self, name):
        self.name = name
        print("init class method")


# Instantiate the class
obj = MyClass("Alice")
