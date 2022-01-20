# The function prints the n-th Fibonacci number.


def fibonacci(n):
    fib_number = 1
    first_number = 0
    for i in range(3, n + 1):
        fib_number, first_number = fib_number + first_number, fib_number
    fib_number = 1 if n == 2 else fib_number
    print(fib_number)


if __name__ == '__main__':
    fibonacci(4)
