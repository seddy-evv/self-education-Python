# functions
def sum_calc(a_atr, b_atr):
    return a_atr + b_atr


print(sum_calc(1, 2))

lambda_func = lambda x, y: x + y

print(lambda_func(3, 2))


# exceptions
try:
    b = 1/0
except Exception as exp:
    print("exception", exp)
else:
    pass
finally:
    pass


# str functions
def str_sepr(exmp):
    words = exmp.split()
    out = ".".join(words)
    print(out)


str_sepr("My name is Anatol")


# add 1 to all elements
lst = [1, 2, 3]

for i in enumerate(lst):
    lst[i[0]] = lst[i[0]] + 1
print(lst)

lst = [i + 1 for i in lst]
print(lst)

for i in range(len(lst)):
    lst[i] = lst[i] + 1
print(lst)


# decorator
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


# open files
try:
    with open("file", "prop") as fh:
        for line in fh:
            print(line)
except:
    print("no file")


# reassign variables
a = 1
b = 2

a, b = b, a


# palindrome
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
print(palindrome(187981))


# move zeros to the end
ex = [1, 0, 2, 0, 3]

for i in range(len(ex)):
    if not ex[i]:
        a = ex[i]
        ex.pop(i)
        ex.append(a)
print(ex)


# back slice
ex = [1, 0, 2, 0, 3]
print(ex[-1:2:-1])


# get file format
name = "asamples-workitemstest7.jpg"
paths = name.split(".")
print(paths[-1])


# Remove all values from a list present in another list – three approaches

# First
a = [1, 2, 3, 4, 5]
b = [2, 4]

result = list(set(a) - set(b))
print(result)

# Second
a = [1, 2, 3, 4, 5]
b = [2, 4]

result = [item for item in a if item not in b]
print(result)

# Third
a = [1, 2, 3, 4, 5]
b = [2, 4]

for item in b:
    while item in a:
        a.remove(item)
print(a)


# hash task
data = {1, True, 1.0}
print(data)
# {1}

# Exaplanation:
print(hash(1))
print(hash(True))
print(hash(1.0))
# 1
# 1
# 1


# tuple with list task
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

# but it works
data = (['bee'], 'pygen')
data[0].append(['geek'])
print(data)
# (['bee', ['geek']], 'pygen')


# dict keys
dct = {None: 0, True: 1, False: 0, (0, 1): 1}
print(dct)
# {None: 0, True: 1, False: 0, (0, 1): 1}

try:
    dct = {(0, [1]): 1}
except:
    print(dct)
    # TypeError: unhashable type: 'list'


# infinite recursion
a = []
a.append(a)
print(a)
# [[...]]


# random
import random

# random(): Returns a random floating-point number between 0.0 (inclusive) and 1.0 (exclusive).
number = random.random()
print(number)
# 0.3848767341685718
print(number * 100)
# randint() takes two arguments, a and b (includes both the start and end values), representing the start
# and end of the range, respectively.
# It is an alias for randrange(a, b + 1, 1).
number_int = random.randint(0, 100)
print(number_int)
# 41
