# Create a logic to sort a dict by values (or keys) for Python 3.7+ and for older versions using built-in Python
# functions and using custom logic

initial_dict = {"c": 10, "b": 5, "d": 12, "a": 1}


# Python 3.7+
# Dicts preserve insertion order in Python 3.7+. Same in CPython 3.6
# Sort the dictionary by value in ascending order
sorted_data_asc = dict(sorted(initial_dict.items(), key=lambda item: item[1]))
print(sorted_data_asc)
# {'a': 1, 'b': 5, 'c': 10, 'd': 12}

# for descending order
# sorted_data_asc = dict(sorted(initial_dict.items(), key=lambda item: item[1], reverse=True))

# using operator module
import operator
# The operator module exports a set of efficient functions corresponding to the intrinsic operators of Python.
# # Sort the dictionary by value using itemgetter
sorted_data_asc = dict(sorted(initial_dict.items(), key=operator.itemgetter(1)))

