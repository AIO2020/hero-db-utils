"""
PostgreSQL Database client.

Copyright 2021 Artificial Intelligence Orchestrator (https://aio.ai/)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import logging
import os
import typing

import pandas as pd
import psycopg2.errors
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from hero_db_utils.queries.postgres import DBQuery
from hero_db_utils.exceptions import (
    HeroDatabaseConnectionError,
    HeroDatabaseOperationError,
)
from hero_db_utils.engines.postgres import PsycopgDBEngine, dbengine_from_psycopg
from hero_db_utils.constants import EnvVariablesConf
from hero_db_utils.queries.postgres.op_builder import QueryFunc
from hero_db_utils.utils.queries import PostgresQueries as pgqueries
from hero_db_utils.utils.utils import get_env_params
from hero_db_utils.clients._base import SQLBaseClient

class PostgresDatabaseClient(SQLBaseClient, PsycopgDBEngine):
    """
    Postgresql Database client using the psycopg2 engine.
    Contains useful methods to read/write into a postgres database
    using pandas' dataframes.
    """

    def __init__(
        self,
        db_name: str,
        db_username: str,
        db_password: str,
        db_host: str = "localhost",
        db_port: int = 5432,
        create_database: bool = False,
        ignore_exists: bool = False,
        **session_kwargs,
    ):
        """
        Initiates an instance of Postgres Database client.

        Parameters
        ----------
        `db_name`: str
            Name of the database to connect to.
        `db_username`: str
            Name of the user that will be used to connect to the database.
        `db_password`: str
            Password of the user that will be used to connect to the database.
        `db_host`: str
            Host of the database to connect to.
        `db_port`: int
            Port number to use when trying to connect to the database host.
        `create_database`: bool
            If True then it'll make an attempt to create the database.
        `ignore_exists`: bool
            If True and @create_database is True then it will not raise an exception
            if the database already exists.

        Raises
        ------
        `HeroDatabaseOperationError`
            If the database already exists when `create_database` is True and `ignore_exists` is False.
        """
        if not db_name:
            raise ValueError(
                "Parameter 'db_name' was evaluated to an empty or null value."
            )
        self.db_name = db_name
        if create_database:
            db_name = "postgres"
        session_kwargs = dict(
            db_name=db_name,
            db_user=db_username,
            db_psw=db_password,
            db_port=db_port,
            db_host=db_host,
            engine="postgresql",
            **session_kwargs,
        )
        if not "connect_timeout" in session_kwargs:
            session_kwargs["connect_timeout"] = 3
        if create_database:
            # Create database with 'postgres', close and initiate later with created database.
            super().__init__(**session_kwargs)
            self._db_created = self.create_database(raise_duplicated=not ignore_exists)
        session_kwargs["db_name"] = self.db_name
        super().__init__(**session_kwargs)

    def __str__(self):
        return f"'{self.db_name}' DBClient"

    def __repr__(self):
        return f"<PostgresDatabaseClient: {str(self)}>"

    @staticmethod
    def get_params_from_env_variables():
        """
        Gets the paramaters to initiate an instance of the class from the
        project's settings.py 'DATABASES' attribute [1].

        The environment variables should be named:  n

        - HERO_POSTGRES_DBNAME
        - HERO_POSTGRES_USERNAME
        - HERO_POSTGRES_PASSWORD
        - HERO_POSTGRES_HOST
        - HERO_POSTGRES_PORT

        Returns
        -------
        `dict`
            Dictionary of kwargs that can be passed as a mappable to the class constructor.
        """
        params = get_env_params("postgres")
        if "db_engine" in params:
            del params["db_engine"]
        return params

    def create_database(self, db_name: str = None, raise_duplicated: bool = True):
        """
        Tries to create a new database from the argument @db_name or
        using the 'db_name' attribute of the class instance.

        Parameters
        ----------
        `db_name`: str
            The name of the database to create.
        `raise_duplicated`: bool
            If True it will raise an exception if the database already exists.

        Returns
        -------
        `bool`:
            True if the database was created successfully, False otherwise.

        Raises
        ------
        `HeroDatabaseOperationError`
            If the database already exists and `raise_duplicated` is True
            or other errors creating the database.
        """
        if not db_name:
            db_name = self.db_name
        logging.debug(f"Attempting to create database '{db_name}'")
        scr = sql.SQL("CREATE DATABASE {dbname}").format(dbname=sql.Identifier(db_name))
        with self.connection as conn:
            isolation_level = conn.isolation_level
            try:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cursor:
                    cursor.execute(scr)
                conn.set_isolation_level(isolation_level)
            except psycopg2.errors.DuplicateDatabase as e:
                # Postgres error creating the database
                if raise_duplicated:
                    raise HeroDatabaseConnectionError(
                        f"Error creating database '{db_name}'. Database already exists.",
                        e,
                    ) from e
                logging.debug(
                    f"DuplicateDatabase error ignored with raise_duplicated=False.",
                    exc_info=True,
                )
                return False
            except psycopg2.errors.SyntaxError as e:
                raise HeroDatabaseOperationError(
                    f"Syntax error. Database name '{db_name}' is not valid.", e
                ) from e
            except psycopg2.errors.DatabaseError as e:
                raise HeroDatabaseOperationError(
                    f"PostgreSQL detected an error when creating the database '{db_name}'",
                    e,
                    format_orig=True,
                ) from e
        return True

    def check_table_exists(
        self, table_name, schema_name=None, **read_sql_kwargs
    ) -> bool:
        """
        Checks if the given table exists in the database.

        Parameters
        ----------
        `table_name`: str
            Name of the table to check if it exists.
        `schema_name`: str
            Filter for the tables that exist in the given schema.
            Default to None (Don't filter by schema_name).
        `**read_sql_kwargs`: Any
            Named arguments to pass to the pandas.read_sql function.
        
        Returns
        -------
        `bool`
            True if the given table exists in the database.
        """
        if schema_name is None:
            query = pgqueries.CHECK_TABLE_EXISTS
        else:
            query = pgqueries.CHECK_TABLE_EXISTS_IN_SCHEMA
        schema = str(schema_name)
        with self.connection as conn:
            with conn.cursor() as cursor:
                read_sql_kwargs["sql"] = cursor.mogrify(
                    query,
                    dict(
                        table_name=table_name,
                        schema_name=schema
                    )
                )
            print(read_sql_kwargs["sql"])
            read_sql_kwargs["con"] = conn
            df = pd.read_sql(**read_sql_kwargs)
        return df["exists"].any()

    def set_schemas(self, *schemas: str, add_public: bool = True):
        """
        Changes the database's search path to include the specified schemas.

        Arguments
        ---------
        `*schemas`: str
            Name of schemas to add to the search path.
        `add_public`: bool
            If true the "public" schema will be added to the end of the search path.

        Raises
        ------
        `ValueError`:
            If no schema was provided.
        `HeroDatabaseOperationError`:
            If there was an error adding the schemas to the search path.
        """
        schemas = list(schemas)
        if add_public and "public" not in schemas:
            schemas.append("public")
        if not schemas:
            raise ValueError("You must add at least one schema to the search path.")
        self.set_search_path(schemas)

    def delete_database(self, db_name, force=False):
        """
        Drops a database if it exists using the current connection.

        Parameters
        ----------
        `db_name`: str
            The name of the database to create.
        `force`: bool
            If True then it will terminate all connections to the
            database before attempting to delete.

        Return
        ------
        `bool`
            Returns True if the operation was successful, otherwise
            it will raise an error.
            Note: It will return True even if the database didn't exist
            before executing the method.

        Raises
        ------
        `ValueError`
            When trying to delete the currently opened database.
        `HeroDatabaseOperationError`
            If there was an error completing the operation.
        """
        logging.debug(f"Deleting database '{db_name}'")
        if self.db_name == db_name:
            raise ValueError("Cannot drop the currently open database.")
        scr = sql.SQL("DROP DATABASE IF EXISTS {dbname}").format(
            dbname=sql.Identifier(db_name)
        )
        with self.connection as conn:
            isolation_level = conn.isolation_level
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            try:
                with conn.cursor() as cursor:
                    if force:
                        # Drop connections to database:
                        cursor.execute(
                            pgqueries.DROP_DB_CONNECTIONS, {"db_name": db_name}
                        )
                    cursor.execute(scr)
            except psycopg2.errors.Error as e:
                raise HeroDatabaseOperationError(
                    f"Error dropping database '{db_name}'", e
                ) from e
            conn.set_isolation_level(isolation_level)
        return True

    def insert_from_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        drop: bool = False,
        append: bool = False,
        **to_sql_kwargs,
    ):
        """
        Inserts the rows from the dataframe into the table.
        By default if will fail if the table already exists.

        Requires `sqlalchemy` to use the `df.to_sql` method of `pandas`.

        Parameters
        ----------
        `df`: pandas.DataFrame
            Dataframe with data to insert.
        `table_name`: str
            Name of the table to insert the rows into.
        `drop`: bool
            If True the table will be dropped if it already exists.
        `append`: bool
            If True the rows will be inserted to the table if it already exists.
        `**to_sql_kwargs`: Any
            Named arguments to pass to the pandas.DataFrame.to_sql function.

        Raises
        ------
        `ValueError`:
            If `drop` and `append` are True.
        `HeroDatabaseOperationError`:
            When there was a problem inserting the data to the table, like
            when the table already exists and `drop` and `append` are False.
        """
        if drop and append:
            raise ValueError("Either drop or append can be True, not both.")
        exists_behavior = "append" if append else "replace" if drop else "fail"
        params = {}
        params["index"] = False
        params["if_exists"] = params.get("if_exists", exists_behavior)
        params.update(**to_sql_kwargs)
        params["name"] = table_name
        client = dbengine_from_psycopg(self)
        with client.engine.connect() as conn:
            params["con"] = conn
            try:
                df.to_sql(**params)
            except ValueError as e:
                raise HeroDatabaseOperationError(
                    f"Table '{table_name}' already exists in database '{self.db_name}'.",
                    e,
                ) from e
        client.close()

    def insert_from_df_v2(
        self,
        df: pd.DataFrame,
        table_name: str,
        drop: bool = False,
        append: bool = False,
        structure_def: dict = None,
    ):
        """
        Inserts the rows from a dataframe into the table given
        using a given structure definition to create the table's
        constraints if they don't already exist.
        Possibly removes the requirements to use sqlalchemy.

        #TODO: IMPLEMENT

        Arguments
        ---------
        (see insert_from_df)
        df
        table_name
        drop
        append
        structure_def <dict>:
            Structure definition for the table's columns,
            should be a list of dictionaries with keys:
            - column_name: Name of a column.
            - data_type: Data type of the column.
            - is_pk: True if the column is a primary key of the table.
            - fk_ref: String with format <table>.<column>
        """
        raise NotImplementedError("insert_from_df_v2 method has not been implemented yet")

    def drop_table(self, table_name):
        """
        Drops a table from the database if it exists.

        Parameters
        ----------
        `table_name`: str
            The name of the database to create.

        Return
        ------
        `bool`
            Returns True if the operation was successful, otherwise
            it will raise an error.
            Note: It will return True even if the table didn't exist
            before executing the method.

        Raises
        ------
        `HeroDatabaseOperationError`
            If there was an error completing the operation.
        """
        query = sql.SQL("DROP TABLE IF EXISTS {table_name}").format(
            table_name=sql.Identifier(table_name)
        )
        with self.connection as conn:
            isolation_level = conn.isolation_level
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query)
            except psycopg2.errors.Error as e:
                raise HeroDatabaseOperationError(
                    f"Error dropping table '{table_name}'", e
                ) from e
            conn.set_isolation_level(isolation_level)
        return True

    def build_query(
        self,
        table_name: str,
        projection: list = None,
        limit: int = None,
        filters: dict = None,
        having_filters: dict = None,
        group_by: list = None,
        order_by: list = None,
        join_table: str = None,
        join_on: dict = {},
        join_how: str = "INNER",
        ascending: bool = True,
        distinct:bool=False,
        offset:int=None
    ) -> DBQuery:
        """
        Build a query for the database table given using the given parameters.

        Example:
        >>> query = client.build_query(
        ...    "outcomes",
        ...    projection=["id","outcome"],
        ...    filters={"death":False,"outcome":QueryOp.not_equal("Discharged")},
        ...    limit=5
        )
        >>> client.get_query_value(query)
        'SELECT "id","outcome" FROM "outcomes" WHERE "death" IS false AND "outcome" <> \'Discharged\'  LIMIT 5'

        Parameters
        ----------
        `table_name`: str
            Name of the table to get the data from.
        `projection`: list
            Name of columns to get from the table.
            If a string given it will assume only one column with that name
        `limit`: int
            Number of rows to get from the table.
        `filters`: dict|queries.QueryOperation
            Map of filters to parse as a WHERE statement.
        `having_filters`: dict|queries.QueryOperation
            Map of filters to parse as a HAVING statement.
        `group_list`: list
            List of column names to use to perform a GROUP BY statement.
        `order_list`: list
            List of column names to use for an ORDER BY statement.
        `join_table`: str
            Name of a table to use for a JOIN statement.
        `join_on`: dict|queries.QueryOperation
            Statement to use for the ON statement of the JOIN.
            If dict it will be mapped as key1=value1 AND key2=value2, ..
            If QueryOperation then it will use the raw string format of the object.
        `join_how`: {{'left','right','outer','inner'}} default 'inner'
            Type of sql join to use.
        `ascending`: bool
            If True and `order_list` is not None it orders the columns with
            "ASC" (ascending order). If False it uses "DESC" (descending order). Defaults to True.
        `distinct`: bool
            If true then only returns the distinct values from all columns.
        `offset`: int
            Number of rows to set in the offset statement.

        Returns
        -------
        `DBQuery`
            Instance of a queries.DBQuery with the resolved query.

        References
        ----------
        - [1] `queries.postgres.DBQuery._parse_where_query`

        See also
        --------
            - `PostgresDatabaseClient.select`:
                Takes the same parameters but returns a `pandas.DataFrame` object.
        """
        if isinstance(projection, str):
            projection = [projection]
        elif projection is None:
            projection = ["*"]
        try:
            iter(projection)
        except TypeError:
            raise ValueError("Argument projection must be an iterable.")
        if not projection:
            raise ValueError("Argument projection can't resolve to an empty list.")
        projection = list(projection) if not isinstance(projection, list) else projection
        query = DBQuery()
        query.table(table_name)
        query.values(*projection)
        if limit is not None:
            query.limit(limit)
        if filters is not None:
            query.where(filters)
        if having_filters is not None:
            query.having(having_filters)
        if group_by is not None:
            query.group_by(*group_by)
        if order_by is not None:
            query.order_by(*order_by, ascending=ascending)
        if offset is not None:
            query.offset(offset)
        if distinct:
            query.distinct()
        if join_table:
            query.join(join_table, join_on, join_how)
        query.resolve()
        return query

    def select(
        self,
        *query_params_args,
        read_sql_query_kwargs: dict = {},
        **query_params_kwargs,
    ) -> pd.DataFrame:
        """
        Returns a dataframe with the selection after building the query using
        the given parameters.

        Parameters
        ----------
        `read_sql_query_kwargs`: dict
            Dictionary of named arguments to pass to the `read_sql_query` [1] method.
        `query_params_args` & `query_params_kwargs`
            Positional and named arguments to pass to the `build_query`
            function so it can resolve the query using the given parameters.
            Accepts:
                `table_name`: str
                `projection`: list
                `limit`: int
                `filters`: dict|queries.QueryOperation
                `having_filters`: dict|queries.QueryOperation
                `group_list`: list
                `order_list`: list
                `join_table`: str
                `join_on`: dict | queries.QueryOperation
                `join_how`: {{'left','right','outer','inner'}}, default 'inner'
                `ascending`: bool
                `distinct`: bool
                `offset`: int

        Returns
        -------
            `pandas.DataFrame`
                Dataframe with the result from the query.

        References
            - [1] `PostgresDatabaseClient.read_sql_query`

        See also
        --------
            - `PostgresDatabaseClient.build_query`:
                Like this but returns a `queries.postgres.DBQuery` object.
        """
        query = self.build_query(*query_params_args, **query_params_kwargs)
        return self.read_query(query, **read_sql_query_kwargs)

    def read_sql_query(self, sql_query: str, **read_sql_query_kwargs) -> pd.DataFrame:
        """
        Passes the `sql_query` argument as sql and the rest of named arguments
        to the `read_sql_query` method from pandas using the object's open
        database connection.

        Arguments
        ---------
        `sql_query`: str
            A string representing an SQL Query to run.
        `read_sql_query_kwargs`: Any
            Extra named arguments to pass to the pandas.read_sql_query method.

        Returns
        -------
        `pandas.DataFrame`
            Pandas Dataframe with results from query.
        """
        with self.connection as conn:
            query = DBQuery(sql_query)
            df = query.execute(conn, **read_sql_query_kwargs)
        return df

    def read_query(self, query: DBQuery, **read_sql_query_kwargs) -> pd.DataFrame:
        """
        Reads the sql query to a pandas dataframe using the
        `read_sql_query` method from pandas.

        Arguments
        ---------
        `query`: DBQuery
            A queries.DBQuery isntance with information about the query to run.
        `read_sql_query_kwargs`: Any
            Extra named arguments to pass to the `pandas.read_sql_query` method.

        Returns
        -------
        `pandas.DataFrame`
            Pandas Dataframe with results from query.
        """
        with self.connection as conn:
            df = query.execute(conn, **read_sql_query_kwargs)
        return df

    def get_schema_tables(self, schema:str="public", simple=False) -> pd.DataFrame:
        """
        Returns a pandas dataframe with the tables in the given schema and
        the count of rows of each table.

        Parameters
        ----------
        `schema`: str
            Name of the schema to get the tables from.
            Defaults to 'public' schema
        `simple`:bool
            If True only the table names will be retrieved.
            This method is a lot faster for big databases.

        Returns
        -------
        `pd.DataFrame`
            Pandas dataframe with 3 columns:
            - 'table_name': Name of the tables in the given schema.
            - 'nrows': Number of rows in the table.
            - 'ncols': Number of columns in the table.

            Or just 'table_name' if `simple` is True.

        References
        ----------
        [1] https://www.postgresql.org/docs/8.0/view-pg-tables.html

        """
        with self.connection as conn:
            if not simple:
                df = pd.read_sql_query(
                    pgqueries.GET_TABLES_INFO_IN_SCHEMA, con=conn, params={"schema_name":schema}
                )
            else:
                resp = pd.read_sql_query(
                    pgqueries.SELECT_TABLES_CATALOG_IN_SCHEMA, con=conn, params={"schema_name":schema}
                )
                df = resp[["tablename"]]
                df = df.rename(columns={"tablename":"table_name"})
        return df

    def tables_details(
        self, schema:str="public", tables_names:typing.List[str]=None
    ) -> typing.Dict[str, pd.DataFrame]:
        """
        Gets a dictionary with detailed information about the tables in
        the specified schema by querying the postgres' information tables [1].

        Parameters
        ----------
        `schema`: str
            Name of the schema to get the tables from.
            Defaults to 'public' schema.
        `tables_names`: list of str
            List with the names of the tables to get information from.
            If None it will get all the tables from the specified schema.
            A query to the database will be performed for each table
            specified in `tables_names` or one for all tables if None.

        Returns
        -------
        `dict of pd.DataFrame`
            Returns a dictionary of pandas dataframes, the keys of the dictionary
            represent the name of the tables, the dataframes have all the same format
            and contain information about the columns of the tables. The Dataframes' columns will be:
            `column_name`: str
                Name of the column.
            `data_type`: str
                Data type of the column.
            `is_pk`: bool
                True if the column is primary key.
            `fk_ref` str|None
                Name of the foreign key reference in style "table.column" or
                None if the column is not a foreign key.
            The index of the dataframes represent the real ordinal position of the columns of the table.

        References
        ----------
            [1] `queries.postgres.pg_queries.PostgresQueries.TABLE_COLUMNS_INFO`
        """
        if tables_names is None:
            tables_names = self.get_schema_tables(schema)["table_name"]
        tables_info = {}
        for t_name in tables_names:
            with self.connection as conn:
                df = pd.read_sql_query(
                    pgqueries.TABLE_COLUMNS_INFO,
                    con=conn,
                    params={"schema_name": schema, "table_name": t_name},
                )
                tables_info[t_name] = (
                    df.set_index(["ordinal_position"])
                    .rename_axis("")
                    .iloc[:, 1:]
                    .sort_index()
                )
        return tables_info

    def count(self, table_name: str) -> int:
        """
        Returns the number of rows in the table.

        Arguments
        ---------
        `table_name` <str> | relation
            Name of the table to count the rows.

        Returns
        -------
        `int`
            Number of rows in the table.
        """
        res = self.select(
            table_name, 
            projection=[QueryFunc.count()]
        )
        return res["count"][0]

    def parse_query(self, query: DBQuery) -> str:
        """
        Parses the value of the query to a valid sql string.
        The value returned should be able to be passed to the
        read_sql_query function.

        Arguments
        ---------
        `query`: DBQuery
            Properly initialized DBQuery object, like one
            obtained from the `build_query` function.

        Return
        ------
        `str`
            SQL Query as a string.

        Examples:
        --------
        >>> pgclient = PostgresDatabaseClient(**params)
        >>> query = pgclient.build_query("mytable", projection=["columnA"], order_list="columnB", limit=10)
        >>> pgclient.parse_query(query)
        'SELECT "columnA" FROM "myTable" ORDER BY "columnB" ASC' LIMIT 10
        """
        with self.connection as conn:
            return query.to_representation(conn)
