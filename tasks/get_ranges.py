# A function is implemented that receives a non-empty list of non-repeating
# integers sorted in ascending order as input, which collapses this list.


def get_ranges(list_):
    str_ = ""
    for i in range(len(list_) - 1):
        if (list_[i] - 1) != list_[i - 1] and (list_[i] + 1) == list_[i + 1]:
            str_ += str(list_[i]) + "-"
        elif (list_[i] - 1) == list_[i - 1] and (list_[i] + 1) == list_[i + 1]:
            continue
        else:
            str_ += str(list_[i]) + ","
    str_ += str(list_[len(list_) - 1])
    return str_


if __name__ == '__main__':
    print(get_ranges([0, 1, 2, 3, 4, 7, 8, 10]))
    print(get_ranges([4, 7, 10]))
    print(get_ranges([2, 3, 8, 9]))
