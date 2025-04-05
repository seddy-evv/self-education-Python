# You've been given a string STR that contains words with spaces.
# You need to replace the spaces between words with “@40”.
# Two approaches:

sentence = "aa bbccdd ee ff"
sentence1 = ""
sentence2 = " "


def reverse_words(initial_string):
    # First

    # if initial_string.strip():
    #     return initial_string.replace(' ', '@40')
    # else:
    #     return ""

    # Second

    if initial_string.strip():
        clean_words = [word.strip() for word in initial_string.split()]
        result = "@40".join(clean_words)
        return result
    else:
        return ""


if __name__ == '__main__':
    print(reverse_words(sentence))
    print(reverse_words(sentence1))
    print(reverse_words(sentence2))
