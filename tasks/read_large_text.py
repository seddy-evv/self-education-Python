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

# Some theory regarding iter():
# If you don't use iter(), reading a file in chunks usually requires a clunky while True loop with a manual break condition:

# with open(filepath, 'rb') as f:
#     while True:
#         chunk = f.read(size_in_bytes)
#         if chunk == b"":  # When EOF (End of File) is reached, f.read() returns an empty byte string
#             break

# Python's built-in iter() function has a special, lesser-known form that takes two arguments:
# 1. A Callable: A function or lambda that takes no arguments and returns a value on every call.
# 2. A Sentinel: A specific value that signals the end of the iteration.
# When you pass these two arguments, iter() creates an iterator object. Every time the loop asks for the next item, Python automatically 
# executes the lambda (f.read(65536)).
# The moment the lambda returns the sentinel value (b""), the iterator raises a StopIteration exception behind the scenes, which cleanly 
# and automatically terminates the for loop.

# Modern Pythonic Approach (Using iter):

# for chunk in iter(lambda: f.read(65536), b""):
#     hasher.update(chunk)


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
