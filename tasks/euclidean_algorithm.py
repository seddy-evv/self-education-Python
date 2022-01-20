# Implementation of Euclid's algorithm for calculating the greatest common
# divisor without using  recursion

def euclidean_algorithm(first_number, second_number):

    if second_number > first_number:
        first_number, second_number = second_number, first_number
    elif first_number == second_number:
        print("numbers are equal")

    while first_number > second_number:
        first_number = first_number % second_number
        if first_number:
            first_number, second_number = second_number, first_number
        else:
            print(second_number)
            break


if __name__ == '__main__':
    euclidean_algorithm(459, 4071)
