"""functions"""
def sum_calc(a_atr, b_atr):
    return a_atr + b_atr


print(sum_calc(1, 2))
# 3

lambda_func = lambda x, y: x + y

print(lambda_func(3, 2))
# 5


"""exceptions"""
try:
    b = 1 / 0
except Exception as exp:
    print("exception", exp)
else:
    pass
finally:
    pass
# exception division by zero


"""str functions"""
def str_sepr(exmp):
    words = exmp.split()
    out = ".".join(words)
    print(out)

str_sepr("My name is Anatol")
# My.name.is.Anatol


"""add 1 to all elements"""
lst = [1, 2, 3]

for i in enumerate(lst):
    lst[i[0]] = lst[i[0]] + 1
print(lst)
# [2, 3, 4]

lst = [i + 1 for i in lst]
print(lst)
# [3, 4, 5]

for i in range(len(lst)):
    lst[i] = lst[i] + 1
print(lst)
# [4, 5, 6]


"""decorator"""
def dec_func(func):
    # some code
    def wrapper(*args, **kwargs):
        print("123")
        result = func(*args, **kwargs)
        print(result)
        return result

    return wrapper

@dec_func
def my_func():
    return "hi"

my_func()
# 123
# hi


"""open files"""
try:
    with open("file", "prop") as fh:
        for line in fh:
            print(line)
except:
    print("no file")
# no file


"""reassign variables"""
a = 1
b = 2

a, b = b, a


"""palindrome"""
def palindrome(origin_number):
    total_number = 0
    num = origin_number
    while num > 0:
        dig = num % 10
        total_number = total_number * 10 + dig
        num = num // 10
    if origin_number == total_number:
        return True
    else:
        return False

print(palindrome(187781))
# True
print(palindrome(187981))
# False


"""move zeros to the end"""
ex = [1, 0, 2, 0, 3]

for i in range(len(ex)):
    if not ex[i]:
        a = ex[i]
        ex.pop(i)
        ex.append(a)
print(ex)
# [1, 2, 3, 0, 0]


"""print duplicates"""
lst_ = [1, 2, 4, 6, 2, 1, 3, 2]
def print_duplicates(lst):
    duplicates = {}
    for n in lst:
        duplicates[n] = duplicates.get(n, 0) + 1
    for key, value in duplicates.items():
        if value > 1:
            print(key)


print_duplicates(lst_)
# 1
# 2


"""create dict {'symbol': ['symbol', 'symbol', 'symbol']}"""
str_ = 'asdfasdfasdfsdf'
counts_dict = {}
for char in str_:
    counts_dict.setdefault(char, []).append(char)
print(counts_dict)
# {'a': ['a', 'a', 'a'], 's': ['s', 's', 's', 's'], 'd': ['d', 'd', 'd', 'd'], 'f': ['f', 'f', 'f', 'f']}


"""Given two integers A and B, print all numbers from A to B inclusive, in ascending order if A < B, or in
# descending order otherwise."""
a = 7
b = 2

if a < b:
    for i in range(a, b + 1):
        print(i)
elif a > b:
    for i in range(a, b - 1, -1):
        print(i)
# 7
# 6
# 5
# 4
# 3
# 2


"""back slice"""
ex = [1, 0, 2, 0, 3]
print(ex[-1:2:-1])
# [3, 0]


"""get file format"""
name = "asamples-workitemstest7.jpg"
paths = name.split(".")
print(paths[-1])
# jpg


"""Remove all values from a list present in another list – three approaches"""

"""First"""
a = [1, 2, 3, 4, 5]
b = [2, 4]

result = list(set(a) - set(b))
print(result)
# [1, 3, 5]

"""Second"""
a = [1, 2, 3, 4, 5]
b = [2, 4]

result = [item for item in a if item not in b]
print(result)
# [1, 3, 5]

"""Third"""
a = [1, 2, 3, 4, 5]
b = [2, 4]
for item in b:
    while item in a:
        a.remove(item)
print(a)
# [1, 3, 5]


"""hash task"""
data = {1, True, 1.0}
print(data)
# {1}


"""Exaplanation:"""
print(hash(1))
print(hash(True))
print(hash(1.0))
# 1
# 1
# 1


"""tuple with list task"""
data = (['bee'], 'pygen')
try:
    data[0] = data[0] + ['geek']
except Exception as exp:
    print(exp)
    # TypeError: 'tuple' object does not support item assignment
# or
# data[0] += ['geek']
# print(data)
# TypeError: 'tuple' object does not support item assignment

"""but it works"""
data = (['bee'], 'pygen')
data[0].append(['geek'])
print(data)
# (['bee', ['geek']], 'pygen')


"""dict keys"""
dct = {None: 0, True: 1, False: 0, (0, 1): 1}
print(dct)
# {None: 0, True: 1, False: 0, (0, 1): 1}

try:
    dct = {(0, [1]): 1}
except Exception as exp:
    print(exp)
    # TypeError: unhashable type: 'list'


"""infinite recursion"""
a = []
a.append(a)
print(a)
# [[...]]


"""random"""
import random

# random(): Returns a random floating-point number between 0.0 (inclusive) and 1.0 (exclusive).
number = random.random()
print(number)
# 0.07761683093797356
print(number * 100)
# 7.761683093797355

# randint() takes two arguments, a and b (includes both the start and end values), representing the start
# and end of the range, respectively.
# It is an alias for randrange(a, b + 1, 1).
number_int = random.randint(0, 100)
print(number_int)
# 41


"""range"""
a = range(10)
print(a[:5])
# range(0, 5)
print(a[5])
# 5


"""crazy i++ version"""
i = 1
i = -~i
print(i)
# 2


"""weird math"""
# since two minuses cancel each other out, and the pluses do nothing
a = 3 +++-- 4
# but in this case - works
b = 3 ++--- 4
print(a)
# 7
print(b)
# -1


"""tuple task"""
t1: tuple[int] = (1, 2)
t1 += 3,
print(t1)
# (1, 2, 3)


"""sort a dict by values """
initial_dict = {"c": 10, "b": 5, "d": 12, "a": 1}

# Dicts preserve insertion order in Python 3.7+. Same in CPython 3.6
# Sort the dictionary by value in ascending order
# sorted_data_asc = dict(sorted(initial_dict.items(), key=lambda item: item[1]))
# print(sorted_data_asc)

# import operator
# # Sort the dictionary by value using itemgetter
# sorted_data_asc = dict(sorted(initial_dict.items(), key=operator.itemgetter(1)))

# Older versions
sorted_data_asc = sorted(initial_dict.items(), key=lambda kv: kv[1])
print(sorted_data_asc)
