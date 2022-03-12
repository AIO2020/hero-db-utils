import abc
from dataclasses import _MISSING_TYPE
import typing as tp
import pandas as pd
from hero_db_utils.clients._base import SQLBaseClient
import hero_db_utils.datamodels.exceptions as errors
from hero_db_utils.datamodels.fields import AutoSerialField
from hero_db_utils.datamodels.sources import get_db_client
from hero_db_utils.queries.postgres.op_builder import QueryFunc

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
    
    @classproperty
    def writable_fields(cls):
        """
        Retrieves the fields that can be written over.
        """
        return {
            key:field for key, field in cls.fields.items()
            if not isinstance(field["type"], AutoSerialField)
        }

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
            raise errors.FieldsValidationError(
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
    def objects(cls, source=None, source_type=None):
        return DataObjectsManager(cls, source, source_type)

    @classmethod
    def collection(cls, models=[]):
        return DataModelsCollection(cls, models)

    def save(self, source=None):
        return self.objects().save(self)

    class Meta:
        db_table:str = None

class DataModelsCollection():
    """
    Represents a collection of data models
    that can be represented as a pandas dataframe.
    """
    
    def __init__(self, model_cls:tp.Type[DataModel], data:tp.List[dict]=[]):
        self._rows = []
        if isinstance(data, pd.DataFrame):
            self._rows = [
                model_cls(**kwargs)
                for kwargs in data.to_dict(orient='records')
            ]
        if data and not isinstance(data[0], model_cls):
            for d in data:
                self._rows.append(model_cls(**d))
        elif data:
            assert all(isinstance(d, model_cls) for d in data), (
                "All members must be of the same class "
                f"('{model_cls.__name__}')"
            )
            self._rows = data.copy()
        self._model_cls = model_cls
    
    def __repr__(self):
        return str(self)

    def __str__(self):
        return (
            f"<{self.model_cls.__name__}> Collection({self.size})"
        )

    @property
    def raw(self):
        return self._rows

    @property
    def size(self):
        return len(self._rows)

    @property
    def model_cls(self):
        return self._model_cls

    def asdf(self, all_cols=False) -> pd.DataFrame:
        """
        Transforms a list of member of the fields class or list of dicts (kwargs) into
        a pandas dataframe compatible for this fields class.
        """
        cols = list(self.model_cls.fields) if not all_cols else list(self.model_cls.fields)
        return pd.DataFrame(
            map(lambda o: getattr(o, 'data')[cols], self.raw),
            columns=cols
        )

    def save(self, **kwargs):
        return self.model_cls.objects().save(self, **kwargs)
    
    def __getitem__(self, idx):
        return self._rows[idx]
    
    def __len__(self):
        return self.size

class DataObjectsManager:
    """
    An object that can manage a
    collection of DataModels.
    Using a given source.
    """
    
    #TODO: Support other data sources, like json, csv, etc.
    _ALLOWED_SOURCES = ["sql"]
    
    def __init__(self, model_cls, source=None, source_type=None):
        if source is None:
            source = get_db_client()
        if not source:
            raise ValueError(
                "No valid source to search "
                "for data model objects."
            )
        self._model_cls = model_cls
        if isinstance(source, SQLBaseClient):
            self._source = source
            self.source_type = "sql"
        else:
            self.source_type = source_type
            self._source = source
        self._limit = None

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
        if value not in self._ALLOWED_SOURCES:
            raise ValueError(f"source type '{value}' is not supported.")
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
    
    def filter(self, **kwargs):
        """
        Retrieves the records in the data source
        that match the parameters specified.
        """
        if not kwargs:
            raise ValueError(
                "No parameters to filter were given"
            )
        if self.source_type == "sql":
            results = self.connection.select(
                self.model_cls.dbtable,
                projection=list(self.model_cls.fields),
                filters=kwargs,
                limit=self._limit
            )
        if self._limit is None:
            self._limit = None
        return results
    
    def count(self, **kwargs):
        """
        Count the number of records that match
        a criteria (All records by default).
        """
        if self.source_type == "sql":
            results = self.connection.select(
                self.model_cls.dbtable,
                projection=[QueryFunc.count(alias="count")],
                filters=kwargs
            )
            count = results["count"][0]
        return count

    def get(self, **kwargs):
        """
        Retrieves one object in the data source that matches
        the parameters given or fails.
        """
        self._limit = 2
        results = self.filter(**kwargs)
        if results.empty:
            raise errors.NoResultsError(
                "No object matches the filters specified"
            )
        if len(results.index) > 1:
            raise errors.UnexpectedQueryResult(
                f"Got more than one result for this query"
            )
        result = results.iloc[0]
        return  self.model_cls(**result)

    def get_or_create(self, **kwargs):
        """
        Creates a new objects in the data source
        that matches the parameters given if a similar one
        does not exists in the data source already, otherwise
        retrieves the existing one.
        
        Returns
        -------
        `tuple (model, created)`
            - `model`:DataModel:
                An object in the data source matching the parameters given
            - `created`:boolean:
                True if the object was created in the data source,
                False if it was just retrieved.
        """
        created = False
        try:
            model = self.get(**kwargs)
        except errors.NoResultsError:
            pass
        model = self.model_cls(**kwargs)
        self.save(model)
        created = True
        return model, created

    def save(self, instance:tp.Union[DataModel, DataModelsCollection], **kwargs):
        """
        Saves a new record of this data model in the data source.
        """
        if isinstance(instance, DataModel):
            if not isinstance(instance, self.model_cls):
                raise TypeError("This manager does not support this model class")
            table = pd.DataFrame([instance.data])
            table_name = instance.dbtable
        elif isinstance(instance, DataModelsCollection):
            if not instance.model_cls == self.model_cls:
                raise TypeError("This manager does not support this model class")
            table = instance.asdf()
            table_name = instance.model_cls.dbtable
        if self.source_type == "sql":
            kwargs["chunksize"] = kwargs.get("chunksize", 1000)
            self.connection.insert_from_df(
                table[list(self.model_cls.writable_fields)], table_name,
                if_exists="append",
                index=False,
                **kwargs
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
