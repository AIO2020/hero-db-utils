"""
Helper data types
"""


class Literal:
    """
    Class to represent a literal constant of a value.
    Functionality used for this class may vary depending
    on the object initializing it.
    """

    def __init__(self, value):
        self.__val = value

    @property
    def value(self):
        return self.__val

    def __str__(self):
        return str(self.__val)

    def __call__(self):
        return self.__val
