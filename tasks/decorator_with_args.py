# A decorator with parameters


def decorator_factory(arg):
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(arg)
            result = func(*args, **kwargs)
            return result
        return wrapper
    return decorator


@decorator_factory(3)
def function(num):
    return num + 1


# We can replace the expression above with
# function = decorator_factory(some_arg)(function)
# and then call the function like below
# function(some_arg)
# because we redefined the variable name function to call the decorator dec


if __name__ == '__main__':
    print(function(5))
