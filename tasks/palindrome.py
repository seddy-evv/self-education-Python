# The function checks if the number is a palindrome.


def palindrome(origin_number):
    total_number = 0
    num = origin_number
    while num > 0:
        dig = num % 10
        total_number = total_number * 10 + dig
        num = num // 10
    if origin_number == total_number:
        return True
    else:
        return False


if __name__ == '__main__':
    print(palindrome(187781))
    print(palindrome(187981))
