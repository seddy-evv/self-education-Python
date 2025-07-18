"""Create a function that takes an iterable object, with any level of nesting of other iterable objects, and produces a
flat sequence"""
from collections.abc import Iterable


"""Generator and recursion"""
def flatten_generator(items, ignore_types=(str, bytes)):
    """
      str, bytes - are iterable objects, but we want to return them as is
    """
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types):
            yield from flatten_generator(x)
        else:
            yield x


"""Recursion"""
def flatten_recursion(items, ignore_types=(str, bytes), res=[]):
    """
    str, bytes - are iterable objects, but we want to return them as is
    """
    if isinstance(items, Iterable) and not isinstance(items, ignore_types):
        for i in items:
            flatten_recursion(i)
    else:
        res.append(items)
    return res


"""Nested loops"""
def flatten_nested_loops(items, ignore_types=(str, bytes)):
    """
    str, bytes - are iterable objects, but we want to return them as is
    """
    result = []
    while items:
        current = items.pop()
        if isinstance(current, Iterable) and not isinstance(current, ignore_types):
            for item in current:
                items.append(item)
        else:
            result.append(current)

    return result[::-1]


if __name__ == "__main__":
    items = [1, 2, [3, 4, [5, 6], 7], 8, ('A', {'B', 'C'})]
    print(list(flatten_generator(items)))
    print(flatten_recursion(items))
    print(flatten_nested_loops(items))
