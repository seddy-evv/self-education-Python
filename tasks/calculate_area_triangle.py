# Initial data: three sides of a triangle. The function checks
# if these are the sides
# of the triangle. If the sides define a triangle, finds its area.
# If not, displays an invalid data message.


def calculate_area_triangle(a, b, c):
    if a + b > c and a + c > b and b + c > a:
        p = (a + b + c) / 2
        s = (p * (p - a) * (p - b) * (p - c))**0.5
        return "Area is:{}".format(s)
    else:
        return "Incorrect value"


if __name__ == '__main__':
    print(calculate_area_triangle(1, 3, 3))
    print(calculate_area_triangle(1, 2, 3))
