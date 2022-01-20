def insert_sort(numbers):
    """ sorting list A by inserts """
    n = len(numbers)
    for top in range(1, n):
        k = top
        while k > 0 and numbers[k - 1] > numbers[k]:
            numbers[k], numbers[k - 1] = numbers[k - 1], numbers[k]
            k -= 1
    return numbers


def choise_sort(numbers):
    """ sorting list A by choice """
    n = len(numbers)
    for pos in range(0, n - 1):
        for k in range(pos + 1, n):
            if numbers[k] < numbers[pos]:
                numbers[k], numbers[pos] = numbers[pos], numbers[k]
    return numbers


def bubble_sort(numbers):
    """ sorting list A using the bubble method"""
    n = len(numbers)
    for bypass in range(1, n):
        for k in range(0, n-bypass):
            if numbers[k] > numbers[k + 1]:
                numbers[k], numbers[k + 1] = numbers[k + 1], numbers[k]
    return numbers


if __name__ == '__main__':
    example = [1, 2, 3, 1, 11, 33, 14]
    print(insert_sort(example))
    print(choise_sort(example))
    print(bubble_sort(example))
