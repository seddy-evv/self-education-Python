# The function counts in the list of numbers the number of pairs of elements
# equal to each other.
# Any two elements equal to each other form one pair to be counted.
# Input data - a string of numbers, separated by spaces.
# Output data - the number of pairs.
# For example: 1 1 1 is 3 pairs, 1 1 1 1 is 6 pairs.


def count_pairs(str_of_numbers):
    clean_str = str_of_numbers.replace(" ", "")
    dict_of_pairs = {}
    sum_of_numb = {}
    for num in clean_str:
        sum_of_numb[num] = sum_of_numb.get(num, 0) + 1
    for key in sum_of_numb:
        dict_of_pairs[key] = (sum_of_numb[key] * (sum_of_numb[key] - 1)) / 2
    number_of_pairs = sum(dict_of_pairs.values())
    return int(number_of_pairs)


if __name__ == '__main__':
    print(count_pairs("1 1 1"))
    print(count_pairs("1 1 1 1 1"))
    print(count_pairs("2 1 1 1 1 2"))
