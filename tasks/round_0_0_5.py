"""1. In Python 3, the round() function uses a method called "round half to even" or banker's rounding. This means that
when a number is exactly halfway between two integers (e.g., ends in .5), it is rounded to the nearest even integer."""
print(round(2.5))
# 2
print(round(3.5))
# 4


""" 2. Rounds to two decimal places."""
print(round(3.14159, 2))
# 3.14


""" 3. Rounds to the nearest hundred (left of the decimal)."""
print(round(12345, -2))
# 12300


""" 4. Round using basic math rules"""
""" 4.1 The easiest way is to add 0.01"""
print(round(2.5 + 0.01))
# 3
print(round(3.5 + 0.01))
# 4

""" 4.2 Custom function"""
from decimal import Decimal, ROUND_HALF_UP

def traditional_round(number, ndigits=0):
    d = Decimal(str(number))
    quantize_exp = Decimal('1') if ndigits == 0 else Decimal('1e-{}'.format(ndigits))
    rounded = d.quantize(quantize_exp, rounding=ROUND_HALF_UP)
    if ndigits == 0:
        return int(rounded)
    else:
        return float(rounded)

# Examples:
print(traditional_round(2.5))
# 3
print(traditional_round(2.4))
# 2
print(traditional_round(-2.5))
# -3
print(traditional_round(2.55, 1))
# 2.6
print(traditional_round(-2.55, 1))
# -2.6


""" 5. How to round to 0.05 or custom value"""
number = 1.27
rounded_number = round(number / 0.05) * 0.05
print(rounded_number)
# 1.25

""" but due to the nature of floating-point arithmetic, you might get results with many decimal places,
such as 1.25000000000000001, so the decimal module will help"""
print(traditional_round(1.25000000000000001, 2))
# 1.25
