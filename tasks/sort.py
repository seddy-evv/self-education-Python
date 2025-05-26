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


# There are some non-comparison-based sorting algorithms that can achieve O(n) under particular condition.
# Counting Sort.
# It is used when the input consists of a small range of integers. For example, when sorting integers in the range [0, k], where ( k ) is not excessively large compared to ( n ), the number of elements.
def counting_sort(arr):
    # Find the maximum and minimum values in the array
    max_val = max(arr)
    min_val = min(arr)
    
    # Range of numbers in the array
    range_of_numbers = max_val + 1
    
    # Create a count array to store the frequency of each value
    count = [0] * range_of_numbers
    
    # Count each element in the input array
    for num in arr:
        count[num] += 1
    
    # Rebuild the sorted array from the count array
    sorted_arr = []
    for i in range(len(count)):
        sorted_arr.extend([i] * count[i])
    
    return sorted_arr


# Radix Sort
# It works effectively for data with fixed-length integers or strings (e.g., numbers, dates, strings of uniform length). It spreads out the comparison operations over multiple "digit" passes.
def counting_sort_for_radix(arr, exp):
    # Create a count array to store the frequency of digits
    count = [0] * 10  # 10 digits (0-9)
    output = [0] * len(arr)
    
    # Count the occurrences of the digits (based on the current exponent)
    for num in arr:
        index = (num // exp) % 10
        count[index] += 1
    
    # Update count array to store positions of digits in sorted order
    for i in range(1, 10):
        count[i] += count[i - 1]
    
    # Build the output array
    for i in reversed(range(len(arr))):  # Traverse the array in reverse order for stability
        index = (arr[i] // exp) % 10
        output[count[index] - 1] = arr[i]
        count[index] -= 1
    
    # Copy the sorted elements back into the input array
    for i in range(len(arr)):
        arr[i] = output[i]

def radix_sort(arr):
    # Find the maximum number to identify the number of digits
    max_val = max(arr)
    
    # Perform counting sort for each digit (exponent 1, 10, 100, ...)
    exp = 1
    while max_val // exp > 0:
        counting_sort_for_radix(arr, exp)
        exp *= 10


if __name__ == '__main__':
    example = [1, 2, 3, 1, 11, 33, 14]
    print(insert_sort(example))
    print(choise_sort(example))
    print(bubble_sort(example))
    print(counting_sort(example))
    print(radix_sort(example))
