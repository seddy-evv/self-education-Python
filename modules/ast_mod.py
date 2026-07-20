# The ast (Abstract Syntax Trees) module in Python is a built-in tool used to process and analyze the source code of
# a program. It breaks down Python code into a structured, hierarchical tree of objects where each node represents
# a specific linguistic element (like a loop, a function definition, or a variable assignment).

# Core Features
#  - Safe Evaluation (ast.literal_eval): This serves as a secure alternative to the built-in eval() function.
#    It evaluates strings containing only Python literals (strings, numbers, tuples, lists, dicts, booleans, and None)
#    without any risk of executing malicious code.
#  - Code Parsing (ast.parse): It converts raw Python code strings into an Abstract Syntax Tree.
#    This allows you to inspect the structure of your code programmatically.
#  - Code Regeneration (ast.unparse): Introduced in Python 3.9, this takes an AST object structure and turns it
#    back into a formatted, readable Python source code string.
#  - Inspecting Trees (ast.NodeVisitor): A helper class that allows you to walk through the nodes of an AST to collect
#    information, such as counting specific patterns or finding errors.
#  - Modifying Trees (ast.NodeTransformer): A subclass designed to modify the code on the fly by replacing, adding, or
#    deleting nodes before compiling it into bytecode.


# Code Examples:
# 1. Safely Parsing Data with literal_eval

import ast

# A string formatted like a dictionary (e.g., from an untrusted text file)
user_input = "{'status': 'success', 'code': 200, 'data': [10, 20, 30]}"

# Safely convert to a true Python dictionary
parsed_data = ast.literal_eval(user_input)
print(parsed_data["data"])
# [10, 20, 30]


# 2. Inspecting Code Structure
import ast

source_code = "x = 10 + 5"

# Parse code into a tree
tree = ast.parse(source_code)

# Dump the tree to see its exact layout
print(ast.dump(tree, indent=4))
# Module(
#     body=[
#         Assign(
#             targets=[
#                 Name(id='x', ctx=Store())],
#             value=BinOp(
#                 left=Constant(value=10),
#                 op=Add(),
#                 right=Constant(value=5)))],
#     type_ignores=[])


# 3. Counting Function Calls (Static Analysis)
import ast

class FuncCallVisitor(ast.NodeVisitor):

    def __init__(self):
        self.count = 0

    # This method automatically triggers whenever a function call node is reached
    def visit_Call(self, node):
        self.count += 1
        self.generic_visit(node)  # Continue checking nested code


code = """
print("Hello")
len([1, 2, 3])
int("42")
"""

tree = ast.parse(code)
visitor = FuncCallVisitor()
visitor.visit(tree)

print(f"Total function calls found: {visitor.count}")
# Total function calls found: 3


# Common Real-World Use Cases:
# - Static Analysis Tools: Linters like flake8 or pylint parse code into an AST to check for style violations, unused
#   imports, or bad formatting without actually executing your file.
# - Security Scanners: Tools like bandit use the AST to search for vulnerabilities, such as hardcoded passwords or c
#   unsafe function calls.
# - Refactoring & Code Generation: Auto-formatters like black or tools that migrate older Python codebases to newer
#   versions use the AST to rewrite code safely.
