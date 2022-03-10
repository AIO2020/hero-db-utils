import abc
from dataclasses import _MISSING_TYPE
import typing as tp
import pandas as pd
from hero_db_utils.clients._base import SQLBaseClient
from hero_db_utils.datamodels.exceptions import FieldsValidationError
from hero_db_utils.datamodels.sources import get_db_client

from hero_db_utils.utils.functional import classproperty
from dataclasses import dataclass

class DataModelDescriberValue():

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return f"<{self.__str__()}>"

    def __str__(self):
        return f"DescriberValue: '{self.value}' ({type(self.value).__name__})"

def datamodel(cls=None):
    """
    Returns the same class but as a DataModel.
    It also performs some class-specific validation.
    """
    def wrap(cls):
        if hasattr(cls, 'data'):
            raise AttributeError(
                f"Data model '{cls.__name__}' should not "
                "declare a 'data' attribute."
            )
        return type(
            cls.__name__,
            (DataModel,),
            dict(dataclass(cls).__dict__)
        )
    if cls is None:
        return wrap
    return wrap(cls)

class DataModel(abc.ABC):
    """
    Leverages the python's dataclass module to provide
    support for pandas objects, custom data types and
    validation for the goal of creating better data models.
    
    Children from this class must be declared as:
    
    ```
    from dataclasses import dataclass
    
    @dataclass
    class DataModelChild(DataModel):
        fieldname:fieldtype = default_value
    ```
    """

    @classproperty
    def fields(cls) -> tp.Dict[str, tp.Dict[str, tp.Any]]:
        fields = {}
        for key, field in cls.__dataclass_fields__.items():
            fields[key] = {'type':field.type}
            if not isinstance(field.default,_MISSING_TYPE):
                fields[key]['default'] = field.default
        return fields

    @property
    def data(self) -> pd.Series:
        data = {}
        for fieldname, field_data in self.fields.items():
            value = getattr(self,fieldname)
            has_default = "default" in field_data
            if has_default and value == field_data["default"]:
                data[fieldname] = value
            else:
                data[fieldname] = field_data["type"](value)
        return pd.Series(data)

    def _get_field_type(self, fieldname, value):
        if value is None:
            return None
        dtype = self.fields[fieldname]["type"]
        try:
            dvalue = dtype(value)
        except Exception as e:
            raise FieldsValidationError(
                f"Error parsing field '{fieldname}' as type '{dtype.__name__}' using value '{value}'"
            ) from e
        return dvalue

    def clean_field(self, fieldname, value):
        """
        Value cleaning for fields.
        """
        value = self._get_field_type(fieldname, value)
        return value

    @classproperty
    def dbtable(cls) -> str:
        """
        Name of the database table that this model
        can be associated with.
        """
        return cls.Meta.db_table or cls.__name__.lower()

    def validate_field(self, fieldname, value):
        """
        Custom validation for fields.
        """
        pass

    def validate(self):
        """
        Post-init class validation method, should
        raise an exception if a validation error occurs.
        """
        pass

    def __setattr__(self, name, value):
        if name in self.fields:
            if not isinstance(value, DataModelDescriberValue):
                value = self.clean_field(name, value)
                self.validate_field(name, value)
        return super().__setattr__(name, value)

    def __post_init__(self):
        self.validate()

    @classmethod
    def objects(cls, source=None):
        if source is None:
            source = get_db_client()
        if not source:
            raise ValueError(
                "Coult not find a valid source to search "
                "for data model objects."
            )
        return DataObjectsManager(cls, source)

    class Meta:
        db_table:str = None


class DataObjectsManager:
    """
    An object that can manage a
    collection of DataModels.
    Using a given source.
    """
    
    def __init__(self, model_cls, source):
        self._model_cls = model_cls
        if isinstance(source, SQLBaseClient):
            self._source = source
            self.source_type = "sql"
        else:
            raise TypeError(f"Manager does not support sources of type '{type(source)}'")

    @property
    def model_cls(self) -> tp.Type[DataModel]:
        return self._model_cls

    @property
    def connection(self):
        return self._source

    @property
    def source_type(self):
        return self._source_type
    
    @source_type.setter
    def source_type(self, value):
        allowed_values = ["sql"]
        if value not in allowed_values:
            raise ValueError("source type is not an allowd value")
        self._source_type = value

    def only(self, n:int=None):
        """
        Retrieves a number of objects
        from the data source.
        """
        if self.source_type == "sql":
            return self.connection.select(self.model_cls.dbtable, limit=n)

    def all(self):
        if self.source_type == "sql":
            return self.connection.select(self.model_cls.dbtable)

    def save(self, model):
        """
        Saves a new record of this data model
        in the data source.
        """
        if not isinstance(model, self.model_cls):
            raise TypeError("This manager does not support this model class")
        #TODO: Finish implementing 

class DataModelsCollection():
    """
    Represents a collection of data models
    that can be represented as a pandas dataframe.
    """
    
    def __init__(self, data_cls:tp.Type[DataModel], data=tp.List[dict]):
        self._rows = []
        if isinstance(data, pd.DataFrame):
            self._rows = [
                data_cls(**kwargs)
                for kwargs in data.to_dict(orient='records')
            ]
        for d in data:
            if not isinstance(d, data_cls):
                self._rows.append(data_cls(**d).data)
            else:
                self._rows.append(d)
        self._data_cls = data_cls

    @property
    def raw(self):
        return self._rows

    @property
    def size(self):
        return len(self._rows)

    @property
    def data_cls(self):
        return self._data_cls

    def asdf(self) -> pd.DataFrame:
        """
        Transforms a list of member of the fields class or list of dicts (kwargs) into
        a pandas dataframe compatible for this fields class.
        """
        return pd.DataFrame(
            [row.data for row in self._rows],
            columns=list(self.data_cls.fields())
        )

class DataModelDescriber():

    def __init__(self, dclass:tp.Type[DataModel], **kwargs):
        self._dclass_attrs = {}
        self.__class_fields = dclass.fields
        for key, value in kwargs.items():
            if key not in self.__class_fields:
                raise ValueError(f"'{key}' is not a valid field for class '{dclass.__name__}'")
            self._dclass_attrs[key] = DataModelDescriberValue(value)
        self._dclass = dclass
        self.instance = dclass(**self._dclass_attrs)

    def __getattr__(self, attr):
        if attr in self.__class_fields:
            return getattr(self.instance, attr).value
        raise AttributeError("%r object has no attribute %r" %
                             (self.__class__.__name__, attr))

    def __repr__(self):
        return f"<{self.__str__()[:100]}>"

    def __str__(self):
        return f"DModelDescriber for: {self._dclass.__name__}"
