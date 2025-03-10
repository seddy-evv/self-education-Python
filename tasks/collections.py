# The Python collections module provides specialized container datatypes that extend Python's built-in data structures
# like lists, dictionaries, and tuples. These data types offer more advanced data manipulation capabilities,
# optimized storage, or improved performance compared to the standard containers.


# 1. namedtuple
# Description: Creates immutable, lightweight, and accessible tuple-like objects with named fields.
# Useful for creating simple classes to store related data.
# Syntax: namedtuple(typename, field_names, *, rename=False, defaults=None, module=None)

# Example:
from collections import namedtuple

# Create a named tuple
Point = namedtuple('Point', ['x', 'y'])

# Instantiate the named tuple
p = Point(10, 20)

print(p.x, p.y)  # Output: 10 20
print(p)         # Output: Point(x=10, y=20)

# 2. deque (Double-Ended Queue)
# Description: A list-like container with fast O(1) appends and pops from both ends of the queue.
# Great for efficiently implementing queues and stacks.
# Syntax: collections.deque([iterable], maxlen=None)
# Useful Methods:
# append(x) and appendleft(x): Add elements to the right or left.
# pop() and popleft(): Remove elements from the right or left.
# rotate(n): Rotate the deque by n elements.

# Example:
from collections import deque

d = deque([1, 2, 3])
d.append(4)            # d = deque([1, 2, 3, 4])
d.appendleft(0)        # d = deque([0, 1, 2, 3, 4])
d.pop()                # Removes 4, d = deque([0, 1, 2, 3])
d.popleft()            # Removes 0, d = deque([1, 2, 3])
d.rotate(1)            # d = deque([3, 1, 2]) (rotates right by 1)
print(d)

# 3. Counter
# Description: A dictionary subclass for counting hashable objects. Its elements are stored as dictionary keys
# and their counts as dictionary values.
# Syntax: collections.Counter([iterable-or-mapping])
# Useful Methods:
# counter.most_common(n): Retrieve the n most common elements.
# counter.update(iterable_or_mapping): Add counts from another iterable or mapping.
# subtract(iterable_or_mapping): Subtract counts from another iterable or mapping.

# Example:
from collections import Counter

# Count occurrences in a string
c = Counter("banana")
print(c)  # Output: Counter({'a': 3, 'n': 2, 'b': 1})

# Most common elements
print(c.most_common(2))  # Output: [('a', 3), ('n', 2)]

# Update counts
c.update("apple")
print(c)  # Output: Counter({'a': 4, 'n': 2, 'p': 2, 'b': 1, 'e': 1, 'l': 1})

# 4. OrderedDict (Python 3.7+: Regular dict is already ordered)
# Description: A dictionary subclass that remembers the insertion order of keys. (This is useful for Python
# versions before 3.7, as dictionaries in Python are ordered by default from Python 3.7+.)
# Syntax: collections.OrderedDict(*args, **kwargs)
# Useful Methods:
# move_to_end(key, last=True): Move a key to the end or the beginning of the dictionary.

# Example:
from collections import OrderedDict

od = OrderedDict()
od["a"] = 1
od["b"] = 2
od["c"] = 3

print(od)  # Output: OrderedDict([('a', 1), ('b', 2), ('c', 3)])

od.move_to_end("a")  # Move "a" to the end
print(od)  # Output: OrderedDict([('b', 2), ('c', 3), ('a', 1)])

# 5. defaultdict
# Description: A dictionary subclass that provides a default value for a nonexistent key. When querying a key
# that doesn’t exist, it uses the callable provided at initialization to create a default value.
# Syntax: collections.defaultdict(default_factory)

# Example:
from collections import defaultdict

dd = defaultdict(int)  # Default value is 0
dd["a"] += 1           # Increment "a", automatic default = 0
dd["b"] += 2

print(dd)  # Output: defaultdict(<class 'int'>, {'a': 1, 'b': 2})

# 6. ChainMap
# Description: A dictionary-like class for combining multiple dictionaries or mappings into a single view.
# It searches for keys across all the dictionaries in the order they are provided.
# Syntax: collections.ChainMap(*maps)

# Example:
from collections import ChainMap

dict1 = {"a": 1, "b": 2}
dict2 = {"b": 3, "c": 4}

cm = ChainMap(dict1, dict2)
print(cm["a"])  # Output: 1 (from dict1)
print(cm["b"])  # Output: 2 (from dict1)
print(cm["c"])  # Output: 4 (from dict2)

# 7. UserDict
# Description: A wrapper class around a Python dictionary, allowing developers to subclass and customize
# dictionary behavior.

# Example:
from collections import UserDict

class MyDict(UserDict):
    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise TypeError("Key must be a string")
        super().__setitem__(key, value)

d = MyDict()
d["key"] = "value"  # Works fine
# d[123] = "value"  # Raises TypeError: Key must be a string
print(d)

# 8. UserList
# Description: A wrapper class around a Python list. Allows developers to subclass and modify list behavior.

# Example:
from collections import UserList

class MyList(UserList):
    def append(self, item):
        if not isinstance(item, int):
            raise ValueError("Only integers can be added")
        super().append(item)


l = MyList()
l.append(10)  # Works
# l.append("Hello")  # Raises ValueError: Only integers can be added
print(l)

# 9. UserString
# Description: A wrapper class for customizing string behavior.

# Example:
from collections import UserString

class MyString(UserString):
    def upper(self):
        return "Custom Upper: " + super().upper()

s = MyString("hello")
print(s.upper())  # Output: Custom Upper: HELLO


# Summary of Use Cases
# Function/Class	Use Case
# namedtuple	Create lightweight, immutable, and self-documenting tuple-like objects.
# deque	Fast, efficient queues or stacks with operations on both ends.
# Counter	Quick counting of hashable objects.
# OrderedDict	Maintain insertion order of keys (only needed before Python 3.7).
# defaultdict	Automatically handle missing keys with default values.
# ChainMap	Combine and manage multiple dictionaries.
# UserDict	Customize the behavior of dictionaries.
# UserList	Customize the behavior of lists.
# UserString	Customize the behavior of strings.
