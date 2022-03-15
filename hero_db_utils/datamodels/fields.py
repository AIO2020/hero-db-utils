import pandas as pd
import abc

class BaseField(abc.ABC):
    
    def __init__(self, dtype=None, admits_null=True, admits_empty=True):
        self._admits_null = bool(admits_null)
        self._admits_empty = bool(admits_empty)
        self._dtype = dtype
    
    @property
    def dtype(self):
        return self._dtype
    
    @property
    def nullable(self):
        """
        True if the field admits null values
        """
        return self._admits_null

    @property
    def admits_empty(self):
        return self._admits_empty
    
    @abc.abstractmethod
    def parse(self, value):
        """
        Returns the actual value of the field
        that should be stored in the data source.
        Can also do some sort of validation.
        """
        pass
    
    def __call__(self, value):
        is_null = pd.isnull(value)
        if not self.nullable and is_null:
            raise ValueError(
                "This field does not allow null values."
            )
        elif is_null:
            return None
        if not self.admits_empty and not value:
            raise ValueError(
                "This field does not allow empty values."
            )
        if self.dtype is not None:
            value = self.dtype(value)
        return self.parse(value)

class ChoicesValidatorField(BaseField):

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._choices = list(args)

    def parse(self, value):
        if (not value in self.choices) and (not pd.isnull(value) or not self._admits_null):
            possible_choices = [str(i) for i in self.choices]
            raise ValueError(
                f"Value '{str(value)}' is not supported. Expected one of: '{', '.join(possible_choices)}'"
            )
        return value

    @property
    def choices(self) -> list:
        return self._choices.copy()

class ListChoicesValidatorField(BaseField):
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

    def __init__(self, *choices, **kwargs):
        super().__init__(**kwargs)
        self._choices = set(choices)

    def parse(self, values:list):
        elements_diff = set(values).difference(self.choices)
        if elements_diff:
            raise ValueError(
                f"Elements {elements_diff} are not valid choices for this field."
            )
        return list(set(values))

    @property
    def choices(self) -> set:
        return self._choices.copy()

class ListTextValuesField(BaseField):
    """
    Initiates a field that holds a list of values
    as a comma separated set of strings.

    eg: 'apple,banana,pear,lemon'
    """

    def parse(values):
        """
        Checks if the values text is correct; i.e.
        a string representing a list of values
        as a comma separated set of strings.
        """
        if not isinstance(values, str):
            return ",".join(values)
        try:
            vals = values.split(',')
        except Exception as e:
            raise ValueError("Error splitting values on the comma.") from e
        if not vals:
            raise ValueError("No values were passed to the list")
        vals = pd.Series(vals).str.strip()
        return ",".join(vals)

class BooleanField(BaseField):
    """
    Represents a boolean field
    that can also be nullable
    """
    
    def __init__(self, force_bool=False, **kwargs):
        super().__init__(**kwargs)
        self._force_bool = force_bool
    
    def parse(self, value):
        if self._force_bool:
            value = bool(value)
        else:
            if value not in [True, False]:
                raise ValueError("Value for boolean field must be true or false, got %s" % value)
        return value

class AutoSerialField(BaseField):
    """
    Represents a field that
    the data models can ignore
    since it is handled by the data source.
    """
    
    def parse(self, value):
        return value

class JsonField(BaseField):
    """
    Represents a field that can store a
    json-formatted string.
    """
    
    def __init__(self, json_kwargs={}, **kwargs):
        super().__init__(**kwargs)
        self._to_json_kwargs = json_kwargs

    def parse(self, value):
        kwargs = self._to_json_kwargs.copy()
        if isinstance(value, str):
            data = pd.io.json.loads(value)
        else:
            data = value
        if isinstance(data, pd.DataFrame):
            kwargs["orient"] = "recorsd"
        if not "indent" in kwargs:
            kwargs["indent"] = 4
        return pd.io.json.dumps(data, **kwargs)

class ForeignKeyField(BaseField):
    """
    Represents a field that relates
    to another data model's field
    """
    
    def __init__(self, ref_table, ref_column, **kwargs):
        super().__init__(**kwargs)
        self._ref_model = None
        self.__init_ref_table(ref_table)
        self._ref_column = ref_column
    
    def __init_ref_table(self, ref_table):
        from hero_db_utils.datamodels.models import DataModel
        if isinstance(ref_table, DataModel):
            ref_table = ref_table.dbtable
            self._ref_model = ref_table
        self._ref_table = ref_table

    @property
    def ref_table(self):
        return self._ref_table
    
    @property
    def ref_column(self):
        return self._ref_column
    
    @property
    def ref_model(self):
        return self._ref_model
    
    @property
    def is_ref_model(self):
        """
        Returns true if the reference model
        is a data model
        """
        return self._ref_model is not None

    def parse(self, value):
        return value

_relational_fields = (
    ForeignKeyField,
)

_read_only_fields = (
    AutoSerialField,
)

_identifier_fields = (
    AutoSerialField,
)
