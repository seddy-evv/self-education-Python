# Write a program that takes a single-line list of numbers as input. The program must output the sum of its two
# neighbors for each element of the list. For list elements that are extreme, one of the neighbors is the element
# at the opposite end of the list. For example, if the input is the list [1, 3, 5, 6, 10] the output is expected to be
# the list [13, 6, 9, 15, 7]. If only one number was received as input, it must be output. The output must contain one
# line with the numbers of the new list, separated by a space.

def sum_of_neighbors(lst):
    result_lst = []

    if len(lst) == 1:
        print(lst[0])
    else:
        for i in range(len(lst)):
            if i == len(lst) - 1:
                result_lst.append(lst[0] + lst[-2])
            else:
                result_lst.append(lst[i - 1] + lst[i + 1])

    return result_lst


if __name__ == "__main__":
    print(sum_of_neighbors([1, 3, 5, 6, 10]))
