# Find the first non-repeating character in a string.

# 1. Option using collections
from collections import Counter

def find_first_unique_char(text: str) -> str or None:
    # Count frequencies of all characters
    char_counts = Counter(text)
    
    # Find the first character with a count of 1
    for char in text:
        if char_counts[char] == 1:
            return char
            
    return None  # Return None if all characters repeat

# Examples
print(find_first_unique_char("python"))       # Output: p
print(find_first_unique_char("swiss"))        # Output: w
print(find_first_unique_char("aabbcc"))       # Output: None


# 2. Option using core Python structures (in Python 3.7+ dict preserving the order in which elements are added, but this solution is version independent)
def find_first_unique_char(text: str) -> str or None:
    char_counts = {}
    for char in text:
        char_counts[char] = char_counts.get(char, 0) + 1
    
    for char in text:
        if char_counts[char] == 1:
            return char
            
    return None


print(find_first_unique_char("python"))     
print(find_first_unique_char("swiss"))      
print(find_first_unique_char("ууууух"))
print(find_first_unique_char("aabbcc"))


# Complexity Analysis
# Time Complexity: O(N), where N is the length of the string. The algorithm traverses the string at most twice.
# Space Complexity: O(K), where K is the number of unique characters stored in the dictionary (maximum of 256 for standard ASCII or bounded by the Unicode character set)
