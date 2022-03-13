import logging
import typing as tp
from hero_db_utils.clients import SQLBaseClient

import pandas as pd
from hero_db_utils.datamodels.helpers import database_to_dict

from hero_db_utils.datamodels.models import DataModel
from hero_db_utils.datamodels.preprocessing import apply_table_filters, get_col_from_id


class DataModelsMapper:

    def __init__(
        self,
        src_data:tp.Union[tp.Dict[str, pd.DataFrame], SQLBaseClient],
        tables_mapping:tp.Dict[tp.Type[DataModel],tp.Union[tp.Dict[str,str],tp.Callable[[tp.Dict[str, pd.DataFrame]], pd.DataFrame]]],
        cols_mapping:tp.Dict[tp.Type[DataModel],tp.Dict[str,tp.Callable[[tp.Dict[str,pd.DataFrame],str],pd.Series]]]={},
        src_tables_conditions:tp.Dict[str, tp.Union[tp.Callable[[pd.DataFrame], pd.DataFrame],str]]={},
        dst_tables_conditions:tp.Dict[tp.Type[DataModel],tp.Union[tp.Callable[[pd.DataFrame], pd.DataFrame],str]]={},
        db_dict_kwargs:tp.Dict[str, tp.Any]={},
    ):
        """
        Initiates an object that maps the tables from a source data to the
        destination data models.

        Parameters
        ----------
        `src_data`:dict[str, df]|SQLBaseClient
            Source data with tables to map, should be a dictionary of dataframes where the keys are the name
            of the tables and the dataframe the data inside them.
            If a SQLBaseClient is used instead, the dictionary will be obtained by querying the database
            for all the tables in it or by the tables defined in `db_dict_kwargs.src_tables`.
        `tables_mapping`:dict[DataModel, dict[str]|callable]. Defaults to empty dict
            Tables level mapping for tables of type `FieldClass`.
            The values accept a column mappings or a generating function.
        `cols_mapping`:dict[DataModel, dict[str, callable]]
            Mapping by columns, function given will be applied per column to the columns mappings from `tables_mappings`
            (The values of the inner dicts).
        `src_tables_conditions`:dict[str, callable|str]. Defaults to empty dict
            Functions to apply per table to the source data dataframes.
        `dst_tables_conditions`:dict[DataModel, callable|str]. Defaults to empty dict
            Functions to apply per FieldClass tot he destination data dataframes.
        `db_dict_kwargs`:dict. Defaults to empty dict.
            Named arguments to pass to the `database_to_dict` function when `src_data` is a SQLBaseClient,
            the function queries the database so `src_data` becomes a dictionary of dataframes.
        """
        if isinstance(src_data, SQLBaseClient):
            # Read dataset from database:
            src_data = database_to_dict(src_data, **db_dict_kwargs)
        self.src_data = src_data.copy()
        self.tables_mapping = tables_mapping
        self.cols_mapping = cols_mapping
        self.src_tables_conditions = src_tables_conditions
        self.dst_tables_conditions = dst_tables_conditions

    def clean_src_data(self):
        """
        Applies the functions in `src_tables_conditions`
        to the source data.
        """
        cleaned_data = self.src_data.copy()
        for tablename in self.src_tables_conditions:
            try:
                cleaned_data[tablename] = apply_table_filters(
                    cleaned_data[tablename],
                    self.src_tables_conditions[tablename],
                    cleaned_data
                )
            except TypeError as e:
                raise TypeError(
                    "Source table condition mapping for table "
                    f"{tablename} got an unexpected type error"
                ) from e
        self._cleaned_data = cleaned_data

    def map(self, force_dtypes=False, include:list=None):
        """
        Runs the mapping from the source tables to the
        destination ones.

        Parameters
        ----------
        `force_dtypes`:bool. Defaults to False.
            If true an exception will be raised if there's an
            error when casting the columns to their target dtype.
        `include`:list. Defaults to None.
            Only map the tables from `tables_mapping`
            that are in this list. If None include all.
        """
        self._dst_dataset = {}
        self.clean_src_data()
        tables_to_map = include if include is not None else self.tables_mapping.keys()
        for dclass in tables_to_map:
            if include is not None and dclass not in include:
                continue
            table_name = dclass.__name__
            src_data_cp = self.cleaned_data
            if not dclass in self.tables_mapping:
                logging.info(f"Skipping '{table_name}' since it's not in columns mappings.")
                continue
            logging.info(f"Importing to table schema: '{table_name}'")
            dclass_fields = dclass.fields
            if isinstance(self.tables_mapping[dclass], dict):# Is a mapping:
                table_data = {}
                for dst_colname, src_path in self.tables_mapping[dclass].items():
                    cond_func = self.cols_mapping.get(dclass,{}).get(dst_colname)
                    if cond_func is None:
                        col_data = get_col_from_id(src_path, src_data_cp)
                    elif callable(cond_func):
                        col_data = cond_func(src_data_cp, src_path)
                    table_data[dst_colname] = col_data
                    table_data[dst_colname] = table_data[dst_colname].values
                table_df = pd.DataFrame(table_data)
            elif callable(self.tables_mapping[dclass]): # Is a generating function
                gen_func = self.tables_mapping[dclass]
                table_df = gen_func(src_data_cp)
            else:
                raise TypeError(
                    f"Received unexpected type of mapping for table '{table_name}' ({dclass}). "
                    f"Got type '{type(self.tables_mapping[dclass])}'"
                )
            # Transform the columns to the correct dtypes:
            for field in dclass_fields:
                if table_df.empty:
                    table_df[field] = []
                elif field not in table_df:
                    try:
                        table_df[field] = dclass_fields[field]["default"]
                    except KeyError as e:
                        raise ValueError(
                            f"Required field '{field}' not found in data for table '{table_name}'"
                        ) from e
                else:
                    dtype = dclass_fields[field]["type"]
                    try:
                        if isinstance(dtype, type):
                            # Replace where not null:
                            table_df.loc[table_df[field].notnull(), field] = table_df.loc[table_df[field].notnull(), field].astype(dtype)
                        else:
                            table_df[field] = table_df[field].map(dtype)
                    except Exception as e:
                        if force_dtypes:
                            raise
                        else:
                            logging.debug(
                                f"Ignored error when casting the column '{table_name}.{field}' to '{dtype}'",
                                exc_info=True
                            )
            table_df = table_df[list(dclass_fields.keys())]
            self._dst_dataset[dclass] = table_df
        self._dst_dataset = self.clean_dst_data(self._dst_dataset)

    def clean_dst_data(
        self,
        tables:tp.Dict[tp.Type[DataModel], pd.DataFrame]
    ):
        """
        Applies the functions in `dst_tables_conditions`
        to the destination data before finishing the transformation.
        """
        tables_with_conds = set(tables.keys()).intersection(self.dst_tables_conditions.keys()) 
        for dclass in tables_with_conds:
            table_cond = self.dst_tables_conditions[dclass]
            try:
                table_df = apply_table_filters(
                    tables[dclass],
                    table_cond,
                    tables
                )
            except TypeError as e:
                raise TypeError(
                    f"Destination table condition mapping for table '{dclass}' "
                    "got an unexpected type error"
                ) from e
            tables[dclass] = table_df
        return tables

    def to_database(self, dbclient:SQLBaseClient, **import_kwargs):
        """
        Uses the database client given to export the data from the destination
        tables to a postgres database.

        Parameters
        ----------
        `dbclient`:SQLBaseClient
            Instance of a SQLBaseClient.
        `**import_kwargs`:Any
            Extra named arguments to pass to the `insert_from_df` method.
        """
        dst_dataset = self.dst_dataset
        for table in dst_dataset:
            import_kwargs["df"] = dst_dataset[table]
            import_kwargs["table_name"] = table.dbtable
            if not "drop" in import_kwargs and not "append" in import_kwargs:
                import_kwargs["drop"] = True
            logging.info(f"Exporting table '{table.dbtable}' to database '{dbclient.db_name}'")
            dbclient.insert_from_df(
                **import_kwargs
            )

    @property
    def cleaned_data(self) -> tp.Dict[str, pd.DataFrame]:
        """
        Retrieves a copy of the cleaned source data.
        """
        if not hasattr(self, '_cleaned_data'):
            raise AttributeError("You must run 'clean_src_data' first before accesing the cleaned source data.")
        return self._cleaned_data.copy()

    @property
    def dst_dataset(self) -> tp.Dict[tp.Type[DataModel],pd.DataFrame]:
        """
        Retrieves a copy of the dataset created after the mapping.
        """
        if not hasattr(self, '_dst_dataset'):
            raise AttributeError("You must run 'map' first before accesing the destination tables.")
        return self._dst_dataset.copy()

