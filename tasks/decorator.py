# A decorator has been created that stores the results of function calls
# (for the entire time of calls, and not just the current run of the program).
import datetime
import os.path


def dec(func):
    def wrapper(*args, **kwargs):
        strings = {}
        f_name = func.__name__
        time_now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")
        if os.path.exists('text.txt'):
            with open('text.txt', 'r') as fh:
                for line in fh:
                    key, *value = line.split()
                    strings[key] = " ".join(value)
        func_num = strings.get(f_name, "0").split()[0]
        strings[f_name] = str(int(func_num) + 1) + " " + time_now
        print(f_name + " " + strings[f_name])
        with open('text.txt', 'w') as fh:
            for key, value in strings.items():
                fh.write(key + " " + value + "\n")
        result = func(*args, **kwargs)
        return result

    return wrapper


@dec
def function():
    pass

# We can replace the expression above with
# function = dec(function)
# and then call the function like below
# function()
# because we redefined the variable name function to call the decorator dec


@dec
def function_new():
    pass


# In some cases, we need to create a function without attributes or with one special attribute and pass it as
# an argument. If within this function we also need to pass arguments, then we can use the solution below
# and pass normalize_wrap(some_arg) instead of normalize, because normalize_wrap returns normalize func.

def normalize_wrap(arg):
    def normalize(pdf):
        v = pdf.v - arg
        return pdf.assign(v=(v - v.mean()) / v.std())
    return normalize


if __name__ == '__main__':
    function()
    function_new()
    function_new()
