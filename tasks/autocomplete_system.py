# Create an autocomplete system.

class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False


class AutocompleteSystem:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word):
        """Inserts a word into the Trie."""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True

    def search_prefix(self, prefix):
        """Searches for the node that matches the given prefix."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def collect_words(self, node, prefix, words):
        """Recursively collects all words from a given node."""
        if node.is_end_of_word:
            words.append(prefix)
        for char, child in node.children.items():
            self.collect_words(child, prefix + char, words)

    def autocomplete(self, prefix):
        """Returns a list of words that match a given prefix."""
        node = self.search_prefix(prefix)
        if not node:
            return []
        words = []
        self.collect_words(node, prefix, words)
        return words


# Example usage:
autocomplete_system = AutocompleteSystem()

# Insert words into the autocomplete system
words = ["apple", "app", "application", "apply", "banana", "ball", "bat", "battle"]
for word in words:
    autocomplete_system.insert(word)

# Test the autocomplete system
prefix = "app"
suggestions = autocomplete_system.autocomplete(prefix)
print(f"Suggestions for prefix '{prefix}': {suggestions}")

prefix = "ba"
suggestions = autocomplete_system.autocomplete(prefix)
print(f"Suggestions for prefix '{prefix}': {suggestions}")

prefix = "z"
suggestions = autocomplete_system.autocomplete(prefix)
print(f"Suggestions for prefix '{prefix}': {suggestions}")
