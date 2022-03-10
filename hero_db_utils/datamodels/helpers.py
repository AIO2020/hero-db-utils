from hero_db_utils.clients import PostgresDatabaseClient
import pandas as pd

import typing

def database_to_dict(
    dbclient:PostgresDatabaseClient,
    table_names:typing.List[str]=None,
    schema_name:str="public"
) -> typing.Dict[str, pd.DataFrame]:
    """
    Reads the tables in `table_names` from the database client
    to a dictionary of pandas dataframes

    Parameters
    ----------
    `dbclient`:PostgresDatabaseClient
        Instance of a PostgresDatabaseClient to connect to the database.
    `table_names`:list of str
        List of tables to read. If `None` all tables in the schema
        `schema_name` will be used.
    `schema_name`:str
        Schema to use when `table_names` is None.
    """
    if table_names is None:
        table_names = dbclient.get_schema_tables(schema_name)["table_name"].to_list()
    if not table_names:
        raise ValueError("Argument 'table_names' can't resolve to empty.")
    dataset = {}
    for table in table_names:
        dataset[table] = dbclient.select(table)
    return dataset
