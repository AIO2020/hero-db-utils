import numpy as np
import pandas as pd

import typing

from hero_db_utils.clients import SQLBaseClient

def get_table_from_datasrc(
    datasrc:typing.Union[typing.Dict[str,pd.DataFrame],SQLBaseClient],
    table_name:str
):
    if isinstance(datasrc, dict):
        return datasrc[table_name]
    elif isinstance(datasrc,SQLBaseClient):
        return datasrc.select(table_name)
    raise TypeError(f"datasrc type '{type(datasrc)}' is not supported")

def get_col_from_id(
    identifier:str,
    datasrc:typing.Union[typing.Dict[str,pd.DataFrame],SQLBaseClient],
    dtype=None
) -> pd.Series:
    """ From an id 'table.column' returns the column as a pandas series """
    table, column = identifier.split(".")
    if isinstance(datasrc, dict):
        if not dtype:
            return datasrc[table][column]
        return datasrc[table][column].astype(dtype)
    res = datasrc.select(table, projection=[column])[column]
    if not dtype:
        return res
    return res.astype(dtype)

def join_tables(
    src_table:str, ref_table:str, src_cols:list, ref_cols:list,
    data_src:typing.Union[typing.Dict[str,pd.DataFrame],SQLBaseClient],
    target_col:str=None, query_params:dict={}, **merge_kwargs
):
    """
    Joins two tables using the pd.merge method and returns the column `target_col`.
    If `target_col` is None it will return the joined dataframe.
    """
    src_df = get_table_from_datasrc(data_src, src_table)
    ref_df = get_table_from_datasrc(data_src, ref_table)
    if not "how" in merge_kwargs:
        merge_kwargs["how"] = "left"
    merged_df = pd.merge(
        src_df, ref_df, left_on=src_cols, right_on=ref_cols,
        **merge_kwargs
    )
    merged_df = query_from_dict(query_params, merged_df)
    if target_col:
        return merged_df[target_col]
    return merged_df

def query_from_dict(query_params:dict, df:pd.DataFrame):
    cond_cols = {}
    for key, value in query_params.items():
        _, colname = key.split('.')
        cond_cols[colname] = value
    query = "&".join(
        [f"{key}=='{value}'" for key, value in cond_cols.items()]
    )
    if query:
        return df.query(query)
    return df

def get_tables_columns_from_id(
    colids:list,
    data_source:typing.Union[typing.Dict[str,pd.DataFrame],SQLBaseClient],
    filter_params=None,
    **kwargs
) -> pd.DataFrame:
    """
    Gets the columns from a table using their identifier (<table>.<column>).
    All columns must belong to the same table.

    Arguments
    ---------
    `filter_params`
        Param to use when filtering the data in the table.
        When `data_source` is a SQLBaseClient this value is passed
        to the build_query method as 'filters', otherwise is passed to the
        `query_from_dict` function.
    `**kwargs`
        Extra named arguments to pass to the method or function that
        obtains the data from the data source. When `data_source` is a
        SQLBaseClient this is the `select` method, otherwise
        is the `query_from_dict` function.
    """
    tables_cols = pd.Series(colids).str.extract(r"(?P<table>[\w\d]+)\.(?P<column>[\w\d]+)")
    if tables_cols.isnull().any().any():
        raise ValueError("Wrong format in colids, they must match 'table.column'")
    if tables_cols["table"].nunique() > 1:
        raise ValueError("Table from the colids must be the same")
    table_name = tables_cols["table"][0]
    if isinstance(data_source, SQLBaseClient):
        kwargs["projection"] = tables_cols["column"].to_list()
        if filter_params:
            kwargs["filters"] = filter_params
        res = data_source.select(
            table_name, **kwargs
        )
        return res
    table_df = data_source[table_name]
    if filter_params:
        table_df = query_from_dict(filter_params, table_df, **kwargs)
    return table_df[tables_cols["column"]]

def get_as_fkey(
    src_col_id:typing.Union[str,typing.Iterable],
    ref_col_id:typing.Union[str,typing.Iterable],
    target_col_id:str,
    data_source,
    query_params:dict={}, src_query_params:dict={}
) -> pd.Series:
    """
    Gets the records of a column as a reference to another table's columns.
    It can be read as:
    ```
        GET "target_col_id"
        FROM "src_col_id".table
        WHERE "src_col_id".columns="ref_col_id".columns
    ```
    `query_params` and `src_query_params` can be used to filter
    the reference column(s) and source column(s) respectively.
    """
    def get_col_id(col_id, coltype=""):
        if isinstance(col_id, str):
            col_id = [col_id]
        else:
            try:
                iter(col_id)
            except TypeError:
                raise TypeError(f"Type '{type(col_id)}' for {coltype} column(s) is not supported.")
            col_id = list(col_id)
        return col_id
    src_col_ids = get_col_id(src_col_id, "source")
    ref_col_ids = get_col_id(ref_col_id, "reference")
    get_colnames = lambda c:c.split(".")[1]
    src_colnames = list(map(get_colnames, src_col_ids))
    ref_colnames = list(map(get_colnames, ref_col_ids))
    _, target_colname = target_col_id.split(".")
    ref_table = get_tables_columns_from_id(
        ref_col_ids+[target_col_id],
        data_source,
        filter_params=query_params
    )
    src_col = get_tables_columns_from_id(
        src_col_ids,
        data_source,
        filter_params=src_query_params
    )
    # Make left join:
    target_df = src_col.merge(
        ref_table,
        how="left",
        left_on=src_colnames,
        right_on=ref_colnames
    )
    return target_df[target_colname+"_y" if target_colname in src_col else target_colname]

def apply_table_filters(
    table_df:pd.DataFrame,
    df_filter:typing.Union[str, typing.Callable],
    full_data:typing.Dict[typing.Any,pd.DataFrame]=None
):
    """
    Applies the given filter to the dataframe 'table' given and
    resets the index of the resulting dataframe.
    """
    table_df = table_df.copy()
    if isinstance(df_filter,str):
        # Is a pandas.query condition:
        table_df = table_df.query(
            df_filter
        ).reset_index(drop=True)
    elif callable(df_filter):
        table_df = df_filter(table_df, full_data).reset_index(drop=True)
    else:
        raise TypeError(
            f"Table condition got unexpected type '{type(df_filter)}'"
        )
    if not isinstance(table_df,pd.DataFrame):
        raise ValueError(
            "Filtered table expected a DataFrame "
            f"but resolved into a {type(table_df)}"
        )
    return table_df
