# The function takes two lists of numbers and counts how many distinct numbers
# are in the first and second lists in total.
def count_numbers(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    count_of_numbers = len(set1 | set2)
    print(count_of_numbers)


# The function takes two lists of numbers and counts how many distinct numbers
# are included in the first and second lists at the same time.
def count_numbers1(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    count_of_numbers = len(set1 & set2)
    print(count_of_numbers)


# The function takes two lists of numbers and counts how many distinct numbers
# are included in just one of these lists.
def count_numbers2(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    count_of_numbers = len(set1 - set2)
    print(count_of_numbers)


# The function takes two lists of numbers and counts how many distinct numbers
# are in the first and the second list except for those found in both lists.
def count_numbers3(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    count_of_numbers = len(set1 ^ set2)
    print(count_of_numbers)


# The function takes the text and counts the number of
# distinct words separated by spaces and end-of-line characters.
def count_different_words(text):
    print(len({sentence for sentence in text.split()}))


if __name__ == '__main__':
    count_numbers1([1, 3, 4, 1, 5, 7], [7, 8, 3, 22, 1, 18])
    count_numbers1([1, 3, 4, 1, 5, 7], [7, 8, 3, 22, 1, 18])
    count_numbers2([1, 3, 4, 1, 5, 7], [7, 8, 3, 22, 1, 18])
    count_numbers3([1, 3, 4, 1, 5, 7], [7, 8, 3, 22, 1, 18])
    rand_text = """Walking, running, cycling and and
                  playing football are some kinds
                  of sports that     you do every day day"""
    count_different_words(rand_text)
