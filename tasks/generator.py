# The simplest example of generator

import sys


def my_generator():
    for i in range(1000):
        yield i


gen = my_generator()

lst_ = [_ for _ in range(1000)]

print("Generator size:", sys.getsizeof(gen))
print("List size:", sys.getsizeof(lst_))
