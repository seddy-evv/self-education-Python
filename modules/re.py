# The Python re module provides support for working with regular expressions, which are powerful tools for matching,
# searching, manipulating, or extracting text patterns.
# Use online tools like regex101 to test and debug your patterns.
# Here’s a list of the main re functions along with descriptions
# and examples:

import re

# 1. re.match(pattern, string)
# Matches the pattern at the beginning of the string. If the pattern is found, it returns a match object; otherwise,
# it returns None.

# Example:
result = re.match(r'Hello', 'Hello World')
if result:
    print("Matched:", result.group())  # Output: Matched: Hello
else:
    print("No Match")


# 2. re.search(pattern, string)
# Searches for the first occurrence of the pattern anywhere in the string. Returns a match object if found,
# otherwise returns None.

# Example:
result = re.search(r'World', 'Hello World')
if result:
    print("Found:", result.group())  # Output: Found: World
else:
    print("Not Found")


# 3. re.fullmatch(pattern, string)
# Matches the entire string against the given pattern. Returns a match object if the pattern matches the full string;
# otherwise, returns None.

# Example:
result = re.fullmatch(r'\d+', '12345')  # Matches only if the full string is digits
if result:
    print("Full Match:", result.group())  # Output: Full Match: 12345
else:
    print("Not Full Match")


# 4. re.findall(pattern, string)
# Finds all occurrences of the pattern in the string and returns a list of matches.

# Example:
result = re.findall(r'\d+', 'The numbers are 42, 123, and 456.')
print("Found:", result)  # Output: Found: ['42', '123', '456']


# 5. re.finditer(pattern, string)
# Finds all occurrences of the pattern in the string, but returns an iterator of match objects instead of a list.

# Example:
matches = re.finditer(r'\d+', 'The numbers are 42, 123, and 456.')
for match in matches:
    print("Match:", match.group())  # Output: Match: 42, Match: 123, Match: 456


# 6. re.sub(pattern, replacement, string)
# Replaces all occurrences of the pattern in the string with the specified replacement.

# Example:
result = re.sub(r'\d+', 'NUM', 'The numbers are 42, 123, and 456.')
print(result)  # Output: The numbers are NUM, NUM, and NUM.


# 7. re.subn(pattern, replacement, string)
# Similar to re.sub(), but returns a tuple containing the new string and the number of replacements made.

# Example:
result = re.subn(r'\d+', 'NUM', 'The numbers are 42, 123, and 456.')
print(result)  # Output: ('The numbers are NUM, NUM, and NUM.', 3)


# 8. re.split(pattern, string)
# Splits the string at each match of the pattern and returns a list of substrings.

# Example:
result = re.split(r',', 'apple,banana,grape')
print(result)  # Output: ['apple', 'banana', 'grape']


# 9. re.compile(pattern, flags=0)
# Creates a compiled regular expression object, which can be reused for matching, searching, etc. This improves
# performance when the same pattern is used multiple times.

# Example:
pattern = re.compile(r'\d+')
result = pattern.findall('The numbers are 42, 123, and 456.')
print(result)  # Output: ['42', '123', '456']

# 10. re.escape(string)
# Escapes all special characters in the given string so they are treated literally in the regular expression.

# Example:
escaped_string = re.escape('a+b*c')
print(escaped_string)  # Output: a\+b\*c


# Match Object Methods
# When using re.match(), re.search(), or other functions that return a match object, you can access details about the
# match with the following methods:

# 1. group()
# Returns the part of the string that matched the pattern.

# Example:
match = re.search(r'\d+', 'My age is 25.')
print(match.group())  # Output: 25


# 2. start() and end()
# Return the starting and ending positions of the match in the string.

# Example:
match = re.search(r'\d+', 'My age is 25.')
print(match.start())  # Output: 10
print(match.end())    # Output: 12


# 3. span()
# Returns a tuple representing the starting and ending positions of the match.

# Example:
match = re.search(r'\d+', 'My age is 25.')
print(match.span())  # Output: (10, 12)


# Flags in Regular Expressions
# You can use flags to modify the behavior of regular expressions:

# Common Flags:
# 1. re.IGNORECASE or re.I: Makes matching case-insensitive.

# Example:
result = re.search(r'hello', 'Hello World', re.IGNORECASE)
print(result.group())  # Output: Hello


# 2. re.MULTILINE or re.M: Allows ^ and $ to match at the beginning and end of each line (instead of only the entire string).

# Example:
result = re.search(r'^Hello', 'Hello\nWorld', re.MULTILINE)
print(result.group())  # Output: Hello

# 3. re.DOTALL or re.S: Allows . to match newline characters (\n).

# Example:
result = re.search(r'Hello.*World', 'Hello\nWorld', re.DOTALL)
print(result.group())  # Output: Hello\nWorld


# REGULAR EXPRESSIONS SYNTAX (regex)

# 1. Basic Syntax
# Literal Characters
# Literal characters match themselves in a string.
# Example: "cat" matches the string "cat" exactly.

# Example:
pattern = r'cat'
result = re.search(pattern, 'My favorite animal is a cat.')
print(result.group())  # Output: cat


# Special Characters
# Special characters have unique meanings in regex:

# Character	Description
# .	        Matches any single character except a newline.
# ^	        Matches the beginning of a string.
# $	        Matches the end of a string.
# *	        Matches 0 or more repetitions of the preceding character/pattern.
# +	        Matches 1 or more repetitions of the preceding character/pattern.
# ?	        Matches 0 or 1 occurrence of the preceding character/pattern.
# {n}	    Matches exactly n repetitions.
# {n,}	    Matches at least n repetitions.
# {n,m}	    Matches between n and m repetitions.
# \	        Escapes special characters (e.g., \. matches a literal .).

# Example:
pattern = r'ca.'
result = re.search(pattern, 'My cat is cute.')
print(result.group())  # Output: cat

# 2. Character Classes
# Predefined Character Classes:

# [abc]: Matches any one character listed inside the brackets (a, b, or c).
# [a-z]: Matches any one lowercase alphabet from a to z.
# \d:    Matches any digit (0-9).
# \w:    Matches any word-like character (alphanumeric + _).
# \s:    Matches any whitespace (space, tab, newline).
# \D:    Matches any non-digit character.
# \W:    Matches any non-word character.
# \S:    Matches any non-whitespace character.

# Example:
pattern = r'\d'
result = re.search(pattern, 'My age is 25.')
print(result.group())  # Output: 2

# Custom Character Classes
# You can create your own classes using square brackets [ ].
# Example: [aeiou] matches any vowel letter.

# Example:
pattern = r'[aeiou]'
result = re.findall(pattern, 'Hello World')
print(result)  # Output: ['e', 'o', 'o']


# 3. Quantifiers
# Quantifiers specify how many times a character or pattern should occur.

# Quantifier	Description
# *	    Matches 0 or more occurrences.
# +	    Matches 1 or more occurrences.
# ?	    Matches 0 or 1 occurrence.
# {n}	Matches exactly n occurrences.
# {n,}	Matches at least n occurrences.
# {n,m}	Matches between n and m occurrences.

# Example:
pattern = r'\d+'
result = re.search(pattern, 'My phone number is 12345.')
print(result.group())  # Output: 12345


# 4. Groups and Capturing
# Grouping with Parentheses
# Use ( ) to group parts of a pattern for capturing or applying quantifiers.

# Example: (abc)+ matches the sequence "abc" one or more times.

# Example:
pattern = r'(ha)+'
result = re.search(pattern, 'hahaha!')
print(result.group())  # Output: hahaha

# Named Groups
# Use (?P<group_name>pattern) to create named groups.

# Example: (?P<word>\w+) matches word-like characters and names the group "word".

# Example:
pattern = r'(?P<number>\d+)'
result = re.search(pattern, 'There are 123 apples.')
print(result.group('number'))  # Output: 123


# 5. Anchors
# Anchors and Boundaries
# ^:  Matches the start of a string.
# $:  Matches the end of a string.
# \b: Matches a word boundary (e.g., beginning or end of a word).
# \B: Matches anything not at a word boundary.

# Example:
pattern = r'\bcat\b'
result = re.search(pattern, 'My cat is cute.')
print(result.group())  # Output: cat


# 6. Alternation (|)
# The | operator allows you to match one pattern OR another.

# Example:
pattern = r'cat|dog'
result = re.search(pattern, 'I love dogs.')
print(result.group())  # Output: dog


# 7. Escaping Special Characters (\)
# If you want to match characters that have special meanings (e.g., ., *, +), you need to escape them using \.

# Example:
pattern = r'\.com'
result = re.search(pattern, 'Visit example.com')
print(result.group())  # Output: .com


# 8. Flags
# Flags modify the behavior of regex matching. Common flags include:

# re.IGNORECASE or re.I: Case-insensitive matching.
# re.MULTILINE or re.M: ^ and $ match at the start and end of each line.
# re.DOTALL or re.S: Makes . match newline characters.
# re.VERBOSE or re.X: Allows regex to be more readable by ignoring whitespace and comments.

# Example:
pattern = r'hello'
result = re.search(pattern, 'HELLO world', re.IGNORECASE)
print(result.group())  # Output: HELLO


# Putting It All Together
# Here’s an example that combines various regex tools:

text = "Jane's birthday is 2023-05-16, and John's birthday is 2000-12-25."
pattern = r'(\d{4})-(\d{2})-(\d{2})'

matches = re.findall(pattern, text)
print("Dates:", matches)  # Output: [('2023', '05', '16'), ('2000', '12', '25')]
