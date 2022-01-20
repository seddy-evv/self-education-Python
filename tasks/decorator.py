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


@dec
def function_new():
    pass


if __name__ == '__main__':
    function()
    function_new()
    function_new()
