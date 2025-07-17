"""The simplest example of generator"""

import sys

def my_generator():
    for i in range(1000):
        yield i

gen = my_generator()
# OR
gen1 = (i for i in range(1000))

lst_ = [_ for _ in range(1000)]

print("Generator size:", sys.getsizeof(gen))
# Generator size: 104
print("Generator1 size:", sys.getsizeof(gen1))
# Generator size: 104
print("List size:", sys.getsizeof(lst_))
# List size: 8856


"""Fibonacci generator"""
def fibonacci():
    prev, cur = 0, 1
    while True:
        yield prev
        prev, cur = cur, prev + cur

for i in fibonacci():
    print(i)
    if i > 10:
        break

a = fibonacci()
print(next(a))
# 0
print(next(a))
# 1
b = fibonacci()
print(next(b))
# 0
print(next(b))
# 1


"""How yield works"""
def gen_fun():
    print('block 1')
    yield 1
    print('block 2')
    yield 2
    print('end')

# 1. when calling the gen_fun function, a generator object is created
# 2. for calls iter() with this object and gets an iterator of this generator
# 3. in a loop, it calls the next() function with this iterator until a StopIteration exception is received
# 4. with each call to next, execution in the function begins from the place where it was completed the last time
# and continues until the next yield
for i in gen_fun():
    print(i)
    # block 1
    # 1
    # block 2
    # 2
    # end


"""The custom range() implementation"""
def my_range(start, stop, inc):
    x = start
    while x < stop:
        yield x
        x += inc

for n in my_range(1, 3, 0.5):
    print(n)
    # 1
    # 1.5
    # 2.0
    # 2.5


"""How to traverse limited nested structures using generators instead of nested loops"""
def chain(*iterables):
    for it in iterables:
        yield from it

g = chain([1, 2, 3], {'A', 'B', 'C'}, '...')
print(list(g))
# [1, 2, 3, 'A', 'B', 'C', '.', '.', '.']
