from hero_db_utils.datamodels.mappers import DataModelsMapper
from hero_db_utils.datamodels import DataModel
from hero_db_utils.utils.functional import classproperty

import typing as tp
import abc
import logging

import pandas as pd

class BaseMapper(abc.ABC):
    
    def get_logger(self):
        class_name = self.__class__.__name__
        package_name = self.__class__.__module__
        logger = logging.getLogger(f"{package_name}.{class_name}.")
        return logger

    @property
    def logger(self):
        return self.get_logger()

    @classproperty
    @abc.abstractmethod
    def model(cls) -> tp.Type[DataModel]:
        """
        Target data model for this mapper.
        """
        pass

    @property
    @abc.abstractmethod
    def table_mapping(self) -> tp.Union[
        tp.Dict[str, str],
        tp.Callable[[tp.Dict[str, pd.DataFrame]], pd.DataFrame]
    ]:
        """
        Returns the mapping or generating function
        that resolves the target data model of this mapper.
        """
        pass
    
    @property
    def columns_mapping(self) -> tp.Dict[
        str,
        tp.Callable[[tp.Dict[str, pd.DataFrame], str], pd.Series]
    ]:
        """
        Returns the mapping by columns of
        the target data model given an specific data source.
        
        This is useful when the table_mapping is not a generating
        funtion, then this property should return a dictionary where the
        keys are the names of the columns and the values functions that
        take the data source and source column as a parameter and returns
        a pandas series that represents the column of the table.
        """
        return None

    @property
    def src_filtering(self) -> tp.Dict[
        str,
        tp.Dict[
            str,
            tp.Union[
                str,
                tp.Callable[[pd.DataFrame, tp.Dict[str, pd.DataFrame]], pd.DataFrame]
            ]
        ]
    ]:
        """
        Filters the data of each source table before mapping to the data model
        It should return a dictionary where the keys are the names of the tables in the source data
        and the values can be a string or a python callable.

        - If the value is a string it will be mapped as pandas.DataFrame.query value.
        - If the value is a callable, it should accept the table to filter as a dataframe
        and the source data as a dictionary of dataframes, then return the filtered table as a pandas dataframe.
        
        The index of the resulting dataframes will be dropped and resetted.
        """
        return None

    @property
    def target_filtering(self) -> tp.Union[
        str,
        tp.Callable[[pd.DataFrame, tp.Dict[str, pd.DataFrame]], pd.DataFrame]
    ]:
        """
        Filters the data for the data model after the mapping is done similarly
        to the values from the dictionary that can be returned from self.src_filtering.
        """
        return None

    def map(self, mapper=None, source=None, **map_kwargs):
        """
        Maps the data from a source data to the target data
        model and retrieves the mapper.
        """
        if not mapper and not source:
            raise ValueError("You must provide a mapper or a source.")
        if not mapper:
            mapper = self.get_mapper(source)
        mapper.map(include=[self.model], **map_kwargs)
        return mapper

    def get_mapper(self, source):
        """
        Creates a mapper that can map the data for this
        data model from a data source.
        """
        mapper = DataModelsMapper(
            src_data=source,
            **self.get_mapper_kwargs()
        )
        return mapper

    def get_mapper_kwargs(self):
        """
        Retrieves the individual parts of this data model's 
        mapper that can be used to create a DataModelsMapper object.
        """
        kwargs = {}
        if self.columns_mapping is not None:
            kwargs["cols_mapping"] = {self.model:self.columns_mapping}
        if self.src_filtering is not None:
            kwargs["src_tables_conditions"] = {self.model:self.src_filtering}
        if self.target_filtering is not None:
            kwargs["dst_tables_conditions"] = {self.model:self.target_filtering}
        return {
            "tables_mapping":{self.model:self.table_mapping},
            **kwargs
        }

def make_mapper(
    source_data,
    mappers:tp.List[tp.Type[BaseMapper]],
    **init_kwargs
) -> DataModelsMapper:
    """
    From multiple base mapper objects
    returns a single DataModelsMapper object
    """
    kwargs = {}
    for mapper in mappers:
        mapper_kwargs = mapper.get_mapper_kwargs()
        for arg in mapper_kwargs:
            if not arg in kwargs:
                kwargs[arg] = {}
            kwargs[arg][mapper.model] = mapper_kwargs[arg]
    return DataModelsMapper(
        source_data,
        **kwargs,
        **init_kwargs
    )
