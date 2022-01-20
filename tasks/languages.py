# According to the initial data, the function prints in the first line the
# number of languages that all students know. Starting from the second line,
# there is a list of such languages.
# Then - the number of languages that at least one student knows,
# on the next lines - a list of such languages.


def languages(initial_text):
    list_of_sentences = initial_text.split("\n")
    number_of_students = int(list_of_sentences[0].strip())
    position = 1
    students_languages = {}
    for i in range(1, number_of_students + 1):
        number_of_languages = int(list_of_sentences[position].strip())
        languages_ = set()
        for language in range(1, number_of_languages + 1):
            languages_.add(list_of_sentences[position + language].strip())

        students_languages[i] = languages_
        position += number_of_languages + 1

    one_students_languages = set()
    for el in students_languages.values():
        one_students_languages |= el

    all_students_languages = one_students_languages.copy()
    for el in students_languages.values():
        all_students_languages &= el

    print(len(all_students_languages))
    print(*all_students_languages, sep="\n")
    print(len(one_students_languages))
    print(*one_students_languages, sep="\n")


if __name__ == '__main__':
    text = """3
              2
              Russian
              English
              3
              Russian
              Belarusian
              English
              3
              Russian
              Italian
              French"""
    languages(text)
