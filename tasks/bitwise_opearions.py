# 1. Bitwise AND (&)
# How it works: Sets each bit to 1 only if both corresponding bits are 1.
a = 5  # 0101
b = 3  # 0011
print(a & b)  # Output: 1  (Binary: 0001)

# 2. Bitwise OR (|)
# How it works: Sets each bit to 1 if at least one of the corresponding bits is 1.
a = 5  # 0101
b = 3  # 0011
print(a | b)  # Output: 7  (Binary: 0111)

# 3. Bitwise XOR (^)
# How it works: Sets each bit to 1 if only one of the corresponding bits is 1 (they must be different).
a = 5  # 0101
b = 3  # 0011
print(a ^ b)  # Output: 6  (Binary: 0110)

# 4. Bitwise NOT (~)
# How it works: Inverts all the bits (flips 1 to 0 and 0 to 1). In Python, this is mathematically equivalent
# to -(x + 1) because integers use Two's Complement representation.
a = 5  # 0101
print(~a)  # Output: -6  (Binary representation flips and adds sign)

# 5. Bitwise Left Shift (<<)How it works: Shifts the bits to the left by the specified number of positions, pushing
# zeros in from the right. This effectively multiplies the number by 2**shift.
a = 5  # 0101
print(a << 1)  # Output: 10 (Binary: 1010) -> Same as 5 * 2
print(a << 2)  # Output: 20 (Binary: 10100) -> Same as 5 * 4

# 6. Bitwise Right Shift (>>)How it works: Shifts the bits to the right by the specified number of positions, discarding
# bits that fall off. This effectively performs floor division of the number by 2**shift.
a = 5  # 0101
print(a >> 1)  # Output: 2  (Binary: 0010) -> Same as 5 // 2


# Quick Cheat Sheet Summary
# Operator    Name           Description                Formula / Behavior
# &           AND            1 if both bits are 1       Intersection
# |           OR             1 if either bit is 1       Union
# ^           XOR            1 if bits are different    Exclusive Difference
# ~           NOT            Inverts all bits           -(x + 1)
# <<          Left Shift     Shifts bits left           x * (2 ** shift)
# >>          Right Shift    Shifts bits right          x // (2 ** shift)
