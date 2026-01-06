# 1. In Python 3, the round() function uses a method called "round half to even" or banker's rounding. This means that
# when a number is exactly halfway between two integers (e.g., ends in .5), it is rounded to the nearest even integer.
import decimal

print(round(2.5))
# 2
print(round(3.5))
# 4

# 2. Rounds to two decimal places.

print(round(3.14159, 2))
# 3.14

# 3. Rounds to the nearest hundred (left of the decimal).

print(round(12345, -2))
# 12300
