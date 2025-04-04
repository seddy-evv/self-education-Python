# You've been given a number N. Write a script that would calculate the sum of all even numbers between 1 and N,
# including N. For example, if N is 6, then the output should be 2 + 4 + 6, or 12.
# Three approaches:

def n_sum(number):
    # First

    # n = 0
    # for i in range(number + 1):
    #     n += i if not i % 2 else 0
    #
    # return n

    # Second

    # return sum([i for i in range(number + 1) if not i % 2])

    # Third
  
    return sum(list(range(2, number + 1, 2)))


if __name__ == '__main__':
    print(n_sum(6))
