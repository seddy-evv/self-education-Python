# You've been given a string 'sentence' with a certain number of words N. You need to make it so that the original word order
# in the string is reversed.
# Also, there can be many spaces between words in the original string, but as a result of the script we should get a
# sentence with one space between words, without spaces at the beginning of the sentence and after its end.
# The script execution time should not exceed 1 second.

import time
start = time.time()

sentence = " aa bb   cc   dd ee  ff   "
sentence1 = ""
sentence2 = " "
sentence3 = "aa"


def reverse_words(initial_string):
    if initial_string.strip():
        clean_words = [word.strip() for word in initial_string.split()][:: -1]
        result = " ".join(clean_words)
        return result
    else:
        return ""


if __name__ == '__main__':
    print(reverse_words(sentence))
    print(reverse_words(sentence1))
    print(reverse_words(sentence2))
    print(reverse_words(sentence3))

    print("result:", time.time() - start)
