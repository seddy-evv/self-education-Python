# Calculate how many years ago the father was twice as old as his son


def twice_as_old(dad_years_old, son_years_old):
    twice_age = (dad_years_old - son_years_old) * 2
    if dad_years_old > twice_age:
        return dad_years_old - twice_age
    elif dad_years_old < twice_age:
        return twice_age - dad_years_old
    else:
        return 0


if __name__ == '__main__':
    print(twice_as_old(60, 35))
