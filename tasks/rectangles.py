# Tasks for all() and any().

# Two rectangles are given, the sides of which are parallel
# to the coordinate axes.
# The coordinates of the lower-left are known x, y, and the width
# and height of the rectangle w, h.
# Determine if all points of the first rectangle belong to the second (all).
# Determine if these rectangles intersect (any, all).

# x1lb, y1lb, w1, h1 - first rectangle
# x2lb, y2lb, w2, h2 - second rectangle


def rectangle(x1lb, y1lb, w1, h1, x2lb, y2lb, w2, h2):
    x1rt = x1lb + w1
    y1rt = y1lb + h1
    x2rt = x2lb + w2
    y2rt = y2lb + h2
    belong = all([x1lb > x2lb, y1lb > y2lb, x1rt < x2rt, y1rt < y2rt])
    if belong:
        print("all points of the first rectangle belong to the second")

    set_x1 = {x for x in range(x1lb, x1lb + w1 + 1)}
    set_x2 = {x for x in range(x2lb, x2lb + w2 + 1)}
    set_y1 = {y for y in range(y1lb, y1lb + h1 + 1)}
    set_y2 = {y for y in range(y2lb, y2lb + h2 + 1)}
    cross = all([set_x1 & set_x2, set_y1 & set_y2, not belong])
    if cross:
        print("rectangles intersect")


if __name__ == '__main__':
    rectangle(-1, 3, 10, 4, -4, 2, 20, 8)
    rectangle(-5, -3, 10, 6, 3, -7, 9, 4)
