# The function finds the maximum divisor of the entered number,
# which is a power of two.


def maximum_divisor(number):
    degree = len(bin(number).replace("0b", "")) - 1
    while degree > 0:
        divisor = 2 ** degree
        if not number % divisor:
            print(divisor)
            break
        degree -= 1
    else:
        print(number)


if __name__ == '__main__':
    maximum_divisor(10)
    maximum_divisor(16)
    maximum_divisor(12)
    maximum_divisor(1)
