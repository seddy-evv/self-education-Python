# The function prints numbers from 1 to 100, but instead of numbers
# that are multiples of 3,
# it writes Fizz, instead of numbers that are multiples of 5, it writes Buzz,
# and instead of numbers that are simultaneously multiples of both 3
# and 5 print FizzBuzz


def fizz_buzz():
    for n in range(1, 101):
        if not n % 3 and not n % 5:
            print("FizzBuzz")
        elif not n % 3:
            print("Fizz")
        elif not n % 5:
            print("Buzz")
        else:
            print(n)


if __name__ == '__main__':
    fizz_buzz()
