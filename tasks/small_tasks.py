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
