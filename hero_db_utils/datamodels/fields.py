import pandas as pd

class ChoicesValidatorField:

    def __init__(self, *args, admits_null=True):
        self._choices = list(args)
        self._admits_null = admits_null

    def __call__(self, value):
        if (not value in self.choices) and (not pd.isnull(value) or not self._admits_null):
            possible_choices = [str(i) for i in self.choices]
            raise ValueError(
                f"Value '{str(value)}' is not supported. Expected one of: '{', '.join(possible_choices)}'"
            )
        return value

    @property
    def choices(self) -> list:
        return self._choices.copy()

class ListChoicesValidatorField:
    """
    Defines a field that validates
    a list of set values (Multi-selection).
    
    Example
    -------
    >>> myfield = ListChoicesValidatorField(
        "a","b","c","d"
    )
    myfield(["a","c","d"])
    ["a","c","d"]
    myfield(["a","b","x"])
    ValueError Exception: "Got Invalid element(s) '{'x'}'"
    """

    def __init__(self, *choices, admits_empty=True):
        self._choices = set(choices)
        self._admits_empty = admits_empty

    def __call__(self, values:list):
        if not values and not self._admits_empty:
            raise ValueError(
                "This field does not allow empty values."
            )
        elements_diff = set(values).difference(self.choices)
        if elements_diff:
            raise ValueError(
                f"Elements {elements_diff} are not valid choices for this field."
            )
        return list(set(values))

    @property
    def choices(self) -> set:
        return self._choices.copy()

class ListTextValuesField:
    """
    Initiates a field that holds a list of values
    as a comma separated set of strings.

    eg: 'apple,banana,pear,lemon'
    """

    def __init__(self, *items):
        self._items = list(items)
    
    @property
    def value(self):
        return ",".join(self._items)
    
    @staticmethod
    def clean(values_str:str, admits_null=True):
        """
        Checks if the values text is correct; i.e.
        a string representing a list of values
        as a comma separated set of strings.
        """
        if admits_null and pd.isnull(values_str):
            return None
        try:
            vals = values_str.split(',')
        except Exception as e:
            raise ValueError("Error splitting values on the comma.") from e
        if not vals:
            raise ValueError("No values were passed to the list")
        vals = pd.Series(vals).str.strip()
        return ",".join(vals)

class AutoSerialField:
    """
    Represents a field that
    the data models can ignore
    since it is handled by the data source.
    """
    
    def __call__(self, value):
        return value
