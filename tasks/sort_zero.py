# The initial data is a list of integers. The function moves all nonzero
# elements
# to the left side of the list without changing their order, and all zeros to
# the right side and prints the resulting list. The order of nonzero
# elements is not changed,
# the additional list is not used, the movement is performed in one pass
# through the list.

def sort_zero(numbers):
    amount_of_elements = len(numbers)
    index = 0
    for n in range(amount_of_elements):
        if numbers[n]:
            numbers[index], numbers[n] = numbers[n], numbers[index]
            index += 1
    return numbers


if __name__ == '__main__':
    print(sort_zero([0, 1, 4, 0, 5, 0, 3, 0, 9, 7, 0]))
