class Iterable1:
    def __init__(self, collection=None):
        self._collection = collection

    def __iter__(self):
        return Iterator(self._collection)


class Iterator1:
    def __init__(self, collection):
        print('init')
        self.collection = collection
        self.cursor = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.cursor >= len(self.collection):
            raise StopIteration
        element = self.collection[self.cursor]
        self.cursor += 1
        return element


it = Iterable1([1, 2, 3])

# for gets the iterator
for element in it:
    print(element)
    # init
    # 1
    # 2
    # 3
for element in it:
    print(element)
    # init
    # 1
    # 2
    # 3


# Call the iterator from the standard collection:
a = [1, 2, 3].__iter__()
print(next(a))
