# The function checks if two strings are Anagram
# An anagram is something where length and character matches but not the order like "Debit card" and "Bad credit",
# both have the same number of characters


def check_anagram(first_str, second_str):
    if len(first_str) != len(second_str):
        return False

    str1_count = {}
    for n in first_str:
        str1_count[n] = str1_count.get(n, 0) + 1

    str2_count = {}
    for n in second_str:
        str2_count[n] = str2_count.get(n, 0) + 1

    return str1_count == str2_count


if __name__ == '__main__':
    str1 = "Debit card".lower()
    str2 = "Bad credit".lower()
    print(check_anagram(str1, str2))
