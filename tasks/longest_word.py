# The function finds the longest word in a sentence.
import re


def return_longest_word(text):
    clean_text = re.sub(r"[^\w\s]", "", text)
    strings = clean_text.split()
    longest_word = ""
    for current_str in strings:
        if len(current_str) > len(longest_word):
            longest_word = current_str
    return longest_word


if __name__ == '__main__':
    sentence = ("Walking, running, cycling and "
                "playing football are some kinds "
                "of sports that you do every day.")
    print(return_longest_word(sentence))
