# The simplest example of binary search


def binary_search(numbers, item):
    low = 0
    high = len(numbers) - 1

    while low <= high:
        mid = (low + high) // 2
        guess = numbers[mid]
        if guess == item:
            return mid
        if guess > item:
            high = mid - 1
        else:
            low = mid + 1
    return None


my_list = [1, 3, 5, 7, 9, 11, 22, 56, 79, 98]


if __name__ == '__main__':
    print(binary_search(my_list, 7))
    print(binary_search(my_list, 10))
