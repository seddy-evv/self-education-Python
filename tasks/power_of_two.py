# A function has been implemented that finds the closest
# power of two to the entered number.


def power_of_two(number):
    highest_degree = 2 ** (len(bin(number).replace("0b", "")))
    smallest_degree = 2 ** (len(bin(number).replace("0b", "")) - 1)
    if number - smallest_degree > highest_degree - number:
        print(highest_degree)
    else:
        print(smallest_degree)


if __name__ == '__main__':
    power_of_two(10)
    power_of_two(20)
    power_of_two(11)
    power_of_two(13)
