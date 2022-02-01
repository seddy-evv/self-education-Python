# The simplest example of calculate factorial


def fact(n):

    factorial = 1
    for i in range(2, n + 1):
        factorial *= i
    return factorial


if __name__ == '__main__':
    print(fact(5))
