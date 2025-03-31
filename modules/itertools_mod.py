# Python's itertools module provides a collection of fast, memory-efficient tools for creating and working
# with iterators. These functions are useful for constructing and working with iterators to solve combinatorial
# problems, create pipelines, or perform data manipulations. Here’s an overview of some of the main itertools
# functions and their descriptions:


# Infinite Iterators:

# 1. count(start=0, step=1)
# Creates an infinite iterator that generates evenly spaced values starting from start
# and increasing (or decreasing) by step.

# Example:
from itertools import count

for i in count(10, 2):  # Starts at 10, increments by 2
    print(i)
    if i > 20:
        break  # To prevent infinite loop during display

# 2. cycle(iterable)
# Repeats the elements of the given iterable indefinitely in a cycle.

# Example:
from itertools import cycle

for i, item in enumerate(cycle('AB'), 1):
    print(item)
    if i > 5:
        break  # To prevent infinite loop during display

# 3. repeat(object, times=None)
# Creates an iterator that repeats the given object times. If times is None, it repeats indefinitely.

# Example:
from itertools import repeat

for item in repeat('Hello', 4):
    print(item)  # Outputs "Hello" 4 times


# Iterators that Terminate on Shortest Input:

# 4. accumulate(iterable, func=operator.add, *, initial=None)
# Returns a running total (or accumulated value) of the input iterable. You can specify a custom function
# (func) for accumulation.

# Example:
from itertools import accumulate
import operator

print(list(accumulate([1, 2, 3, 4], operator.mul)))  # Outputs [1, 2, 6, 24] (cumulative product)

# 5. chain(*iterables)
# Combines multiple iterables into a single iterator, treating them sequentially.

# Example:
from itertools import chain
print(list(chain([1, 2, 3], ['a', 'b'])))  # Outputs [1, 2, 3, 'a', 'b']

# 6. chain.from_iterable(iterable)
# Similar to chain(), but takes a single iterable containing multiple iterables.

# Example:
from itertools import chain

iterable = [[1, 2], [3, 4]]
print(list(chain.from_iterable(iterable)))  # Outputs [1, 2, 3, 4]

# 7. compress(data, selectors)
# Filters data by selecting elements corresponding to True values in selectors.

# Example:
from itertools import compress

data = "ABCDEF"
selectors = [1, 0, 1, 0, 1, 0]
print(list(compress(data, selectors)))  # Outputs ['A', 'C', 'E']

# 8. dropwhile(predicate, iterable)
# Drops elements from the iterable as long as the predicate is True. Once the predicate becomes False,
# it returns the rest of the iterable.

# Example:
from itertools import dropwhile

data = [1, 2, 3, 4, 5]
print(list(dropwhile(lambda x: x < 3, data)))  # Outputs [3, 4, 5]

# 9. takewhile(predicate, iterable)
# Returns elements from the iterable as long as the predicate is True; iteration stops as soon as the predicate is False.

# Example:
from itertools import takewhile

data = [1, 2, 3, 4, 5]
print(list(takewhile(lambda x: x < 4, data)))  # Outputs [1, 2, 3]

# 10. filterfalse(predicate, iterable)
# Returns elements of the iterable for which the predicate is False.

# Example:
from itertools import filterfalse

data = [1, 2, 3, 4, 5]
print(list(filterfalse(lambda x: x % 2 == 0, data)))  # Outputs [1, 3, 5]

# 11. groupby(iterable, key=None)
# Groups consecutive elements of the iterable that have the same value or the same result from the key function.

# Example:
from itertools import groupby

data = [1, 1, 2, 2, 3, 3, 3]
for key, group in groupby(data):
    print(key, list(group))  # Outputs (1, [1, 1]), (2, [2, 2]), (3, [3, 3, 3])

# 12. islice(iterable, start, stop, step)
# Returns a slice of the iterable, similar to slicing a list but works on any iterator.

# Example:
from itertools import islice

data = range(10)
print(list(islice(data, 2, 8, 2)))  # Outputs [2, 4, 6]

# 13. starmap(function, iterable)
# Applies a function to arguments unpacked from the elements of the iterable (useful for tuples).

# Example:
from itertools import starmap

data = [(2, 3), (4, 5), (6, 7)]
print(list(starmap(lambda x, y: x * y, data)))  # Outputs [6, 20, 42]


# Combinatoric Iterators
# 14. product(*iterables, repeat=1)
# Computes the Cartesian product of input iterables, optionally repeated.

# Example:
from itertools import product

print(list(product('AB', [1, 2])))  # Outputs [('A', 1), ('A', 2), ('B', 1), ('B', 2)]

# 15. permutations(iterable, r=None)
# Returns all possible permutations of elements in the iterable of length r (defaults to the length of the iterable).

# Example:
from itertools import permutations

print(list(permutations('ABC', 2)))  # Outputs [('A', 'B'), ('A', 'C'), ('B', 'A'), ('B', 'C'), ('C', 'A'), ('C', 'B')]

# 16. combinations(iterable, r)
# Returns all possible combinations (without replacement) of elements in the iterable of length r.

# Example:
from itertools import combinations

print(list(combinations('ABC', 2)))  # Outputs [('A', 'B'), ('A', 'C'), ('B', 'C')]

# 17. combinations_with_replacement(iterable, r)
# Returns combinations with replacement of elements in the iterable of length r.

# Example:
from itertools import combinations_with_replacement

print(list(combinations_with_replacement('ABC', 2)))  # Outputs [('A', 'A'), ('A', 'B'), ('A', 'C'), ('B', 'B'), ('B', 'C'), ('C', 'C')]


# The above functions can be combined creatively for powerful and efficient data processing and analysis.
# Remember that most of these functions create iterators rather than lists, meaning they are designed for
# efficiency in memory usage and lazy evaluation. You often need to convert them to lists (with list()) for
# immediate display or debugging.
