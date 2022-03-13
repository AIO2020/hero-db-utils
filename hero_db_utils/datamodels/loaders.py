from __future__ import annotations
from abc import ABCMeta, abstractmethod
import itertools
import typing as tp
import multiprocessing as mp
import logging

import pandas as pd
import numpy as np

from hero_db_utils.datamodels import DataModel
from hero_db_utils.datamodels.models import DataModelsCollection
from hero_db_utils.utils.functional import classproperty

import utils

class DataModelLoader(metaclass=ABCMeta):
    """
    Generic table loaders metaclass
    for dataclass loaders.
    """

    @classproperty
    @abstractmethod
    def DATAMODEL(cls) -> tp.Type[DataModel]:
        """
        The target data model class for this loader.
        """
        pass

    def __init__(
        self,
        table:pd.DataFrame=pd.DataFrame(),
        log_loaded=False,
        connection_params={}
    ):
        """
        Initiates an object capable of loading from the source
        'dataclass' table to the heroer's database tables.

        Parameters:
        ----------
        `table`:pd.Dataframe
            A dataframe with the data to load, the dataframe's
            columns must match the fields of the `datamodels.DataModel`
            type data class.
        `log_loaded`:bool
            If True then the loaded data will be kept in memory as a pandas dataframe.
        """
        if not self.DATACLASS:
            raise ValueError("The class constant 'DATACLASS' can't be null.")
        self._input_data:pd.DataFrame = table.copy()
        self._log_loaded = log_loaded
        self._loaded_data = pd.DataFrame()
        self._connection_params = connection_params

    @property
    def dataclass(self) -> tp.Type[DataModel]:
        return self.DATACLASS
    
    def to_datetime(self, date_repr, **kwargs):
        """
        Parses the date given to datetime from
        the hospital's entity timezone using
        the `utils.to_datetime` function.
        """
        kwargs["from_timezone"] = self.hospital_tz
        return utils.to_datetime(date_repr, **kwargs)
    
    @abstractmethod
    def _load_item(
        index:int,
        row:pd.Series
    ) -> tp.Union[tp.Tuple[tp.Type[DataModel], tp.Union[bool, None]],None]:
        """
        Implements the custom logic to load a row of the
        input dataframe in the database.
        This method is called in `self.load` for each row in the input data.

        Parameters:
        -----------
        `index`:int
            Index for a row of the table.
        `row`:pd.Series
            A namedtuple that represents a row of the input table.
        
        Returns
        -------
        Either a tuple that contains information about the record that was successfuly
        created in one of the database's tables or None if the operation failed.

        On success, the tuple returned must consist of only 2 elements:
        1. `type[DataModel]`:
            An instance of a data model.
        2. `bool`| `None`:
            True if the model instance was created, False it it wasn't or None if
            not relevant.
        """
        raise NotImplementedError("Method '_load_item' has not been implemented.")
    
    def preload(self):
        """
        Optional method that is ran before going through
        the data class table to load the information into the database.
        It can be used to populate the database with the records required
        by the _load_item method.
        """
        pass

    def load(
        self,
        map_func:tp.Callable[[tp.Type[DataModelLoader], pd.Series, tp.Type[DataModel]]]=None,
        input_data:pd.DataFrame=None,
        get_output:bool=False,
        bulk_create:bool=False,
        **save_kwargs
    ) -> tp.Union[pd.DataFrame, None]:
        """
        Loads the information of a dataframe of a field class' to
        one or more tables in the database (target tables).

        Parameters
        ----------
        `map_func`:callable(self, row, patient) -> None. Defaults to None.
            An optional function to use for applying an extra logic to the
            data in the data model table, it should take the current running
            DataModelLoader instance (self), a row of the dataframe
            and an instance of the model being created.
        `input_data`:pd.Dataframe. Defaults to None.
            Data of the table to load as a pandas dataframe, if None, self.input_data will be used.
        `get_output`:bool. Defaults to False.
            If True the method will return a dataframe with the records created/updated.
        `bulk_create`:bool. Defaults to False.
            If True a bulk create will be performed on all the objects returned
            by `_load_item` after all the rows of the input data have been exhausted.
        `**save_kwargs`: Named arguments
            Named arguments to pass to the DataObjectsManager.save method.
        Returns
        -------
            `pd.DataFrame` | `None`
               If `get_output` is true, then a dataframe is returned containing
               information about the table created in the database,
               otherwise returns None.
        """
        if self._connection_params and not save_kwargs.get("source_kwargs"):
            save_kwargs["source_kwargs"] = self._connection_params
        self.preload()
        input_data = input_data.copy() if input_data is not None else self._input_data
        logging.info(f"Loading '{len(input_data.index)}' records to the database")
        # Replace nan for python's 'None':
        input_data = input_data.where(pd.notnull(input_data), None)
        created_items = []
        retrieve_out_data = self._log_loaded or bulk_create or get_output
        for row in input_data.itertuples():
            item_resp = self._load_item(row.Index, row)
            if item_resp is None:
                continue
            model_instance, _ = item_resp
            if map_func:
                map_func(self, row, model_instance)
            if not bulk_create:
                model_instance.save(**save_kwargs)
            if retrieve_out_data:
                created_items.append(model_instance.data)
        if retrieve_out_data:
            out_data = pd.DataFrame(created_items)
        if bulk_create:
            self.make_bulk_create(out_data, **save_kwargs)
        if self._log_loaded:
            self._loaded_data = pd.concat([self._loaded_data, out_data], ignore_index=True)
        if get_output:
            return out_data
        return None

    def make_bulk_create(self, objects, **save_kwargs):
        collection = DataModelsCollection(
            self.dataclass,
        )
        collection._rows = objects
        collection.save(
            objects,
            **save_kwargs
        )

    def load_parallelly(
        self,
        input_data:pd.DataFrame=None,
        get_output:bool=False,
        processes:int=None,
        **kwargs
    ) -> tp.Union[pd.DataFrame, None]:
        """
        Distributes the data across a pool of worker processes that 
        run the 'load' method on each partition of the data in parallel.

        This method is convenient when running on CPUs with multiple cores.

        Parameters:
        -----------
        `input_data`: dataframe. Defaults to None.
            Data to partition, the partitions are passed
            as `data` to the `self.load` method on each process.
            If None, `self.data` is used.
        `get_output`:bool. Defaults to False.
            If True the method will return a dataframe with the records created/updated.
        `processes`:int. Defaults to None.
            Number of worker processes to use.
            If None it will use `cpu.count-1`
        `kwargs`: Any. 
            Named arguments to pass to the `load` method on all the processes.

        Returns
        -------
            `pd.DataFrame` | `None`
               If `get_output` is true, then a dataframe is returned containing
               information about the table created in the database,
               otherwise returns None.

        See also
        --------
        `DataModelLoader.load`:
            Like this method but doesn't run on multiple workers.
        """
        self.preload()
        pc_processes = mp.cpu_count()
        default_processes = pc_processes-1 if pc_processes>1 else pc_processes
        num_processes = processes or default_processes
        if not num_processes > 0:
            raise ValueError("processes must be greater than 0")
        input_data = input_data.copy() if input_data is not None else self.input_data
        retrieve_out_data = self._log_loaded or kwargs.get("bulk_create") or get_output
        data_partitions = np.array_split(input_data, num_processes)
        constant_kwargs = {
            "input_data":data_partitions,
            "get_output":itertools.repeat(retrieve_out_data)
        }
        func_params = utils.get_args_from_kwargs(self.load, {**kwargs, **constant_kwargs})
        args_arangement = []
        for arg in func_params:
            if arg in constant_kwargs:
                args_arangement.append(constant_kwargs[arg])
            else:
                args_arangement.append(itertools.repeat(func_params[arg]))
        load_args = zip(
            *args_arangement
        )
        logging.info(
            f"Loading '{len(input_data.index)}' records "
            f"to the database using '{num_processes}' workers."
        )
        with mp.Pool(num_processes) as pool:
            out_data = pool.starmap(self.load, load_args)
        if retrieve_out_data:
            out_data = pd.concat(out_data)
        if self._log_loaded:
            self._loaded_data = pd.concat([self._loaded_data, out_data], ignore_index=True)
        if get_output:
            return out_data
        return None

    @property
    def input_data(self) -> pd.DataFrame:
        """
        Dataframe with the initial input data passed
        on instantiation.
        """
        return self._input_data.copy()

    @property
    def loaded_data(self) -> pd.DataFrame:
        """
        Dataframe that represents the data
        that has been loaded to the database using this
        object.
        """
        return self._loaded_data.copy()
