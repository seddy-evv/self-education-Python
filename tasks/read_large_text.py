"""
Write a function that finds how much times a phrase occurs in 1TB the text file.
"""
from functools import partial


# 1
def read_in_chunks(file_path, size_in_bytes, search_string):
    try:
        with open(file_path, 'r') as f:
            prev = ''
            count = 0
            f_read = partial(f.read, size_in_bytes)
            for text in iter(f_read, ''):
                if not text.endswith('\n'):
                    # if file contains a partial line at the end, then don't
                    # use it when counting the substring count.
                    text, rest = text.rsplit('\n', 1)
                    # pre-pend the previous partial line if any.
                    text = prev + text
                    prev = rest
                else:
                    # if the text ends with a '\n' then simple pre-pend the
                    # previous partial line.
                    text = prev + text
                    prev = ''
                count += text.count(search_string)
            count += prev.count(search_string)
            return count
    except Exception as exp:
        print(exp)
        return 0


# 2
def read_in_lines(file_path, search_string):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read and process the file line by line
            count = 0
            for line in file:
                count += line.count(search_string)
            return count
    except Exception as exp:
        print(exp)
        return 0


if __name__ == '__main__':
    print(read_in_chunks('data.txt', 100, "The search phrase"))
    print(read_in_lines('data.txt', "The search phrase"))
