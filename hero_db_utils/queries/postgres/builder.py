"""
PostgreSQL query generation module.
"""
import pandas as pd

from psycopg2 import sql
from psycopg2.extensions import connection

from hero_db_utils.queries.postgres.op_builder import QueryOperation, QueryOp, QueryFunc, ResolvedQueryOp
from hero_db_utils.utils.dtypes import Literal
from hero_db_utils.utils.utils import any_duplicated


class DBQuery:
    """
    Represents query that can be performed to the database.
    """

    def __init__(self, query: str = "", query_params: dict = {}):
        if not isinstance(query_params, dict):
            raise TypeError(
                f"query_params argument must be a dictionary, it was resolved to: {type(query_params)}"
            )
        self.__query = query
        self.__query_params = query_params
        self.__distinct_clause = None
        self.__projection_elements: list = None
        self.__table_name = None
        self.__join_clause = None
        self.__where_clause = None
        self.__group_clause = None
        self.__having_clause = None
        self.__order_clause = None
        self.__offset_clause = None
        self.__limit_clause = None

    def to_dict(self) -> dict:
        """
        Returns the query and params of the query as a dictionary
        with 'query' and 'params' keys.

        If the query attribute does not have a value it will trigger the
        'resolve' method to get it from the query clauses.

        Returns
        -------
            `dict`
                Dictionary with keys:

                - "query": sql.SQL | str. Represents the SQL query to execute.
                - "params": dict. Named parameters to be used when executing the query.

        Raises
        ------
            `ValueError`
                If the object is not properly initialized. (eg no value for the query attribute or the clauses.)
        """
        if not self.__query:
            try:
                self.resolve()
            except AssertionError as e:
                raise ValueError(
                    "Can't get query value of badly initialized DBQuery"
                ) from e
        return {"query": self.__query, "params": self.__query_params}

    def __str__(self):
        return str(self.__query)

    def to_representation(self, conn:connection=None) -> str:
        """
        Uses a psycopg2 database connection to
        parse the query to a valid sql string.
        """
        if not self.__query:
            try:
                self.resolve()
            except AssertionError as e:
                raise ValueError(
                    "Can't get query value of badly initialized DBQuery"
                ) from e
        if isinstance(self.__query, sql.Composable):
            if conn is None:
                raise ValueError("conn must be a valid postgresql connection")
            with conn.cursor() as cursor:
                return cursor.mogrify(self.__query, self.__query_params).decode()
        return str(self.__query)

    def execute(self, conn, **read_sql_kwargs) -> pd.DataFrame:
        """
        Returns a pandas dataframe with the result
        of the execution of the query.
        """
        read_sql_kwargs["sql"] = self.to_representation(conn)
        read_sql_kwargs["con"] = conn
        df = pd.read_sql_query(**read_sql_kwargs)
        return df

    def values(self, *cols: str, add=False):
        """
        Column names to use in the SELECT statement.
        if add=True then the columns given will be added
        to the ones already selected.
        """
        if not cols:
            raise ValueError("You must add at least one value in cols.")
        if not add:
            self.__projection_elements = list(cols)
        else:
            if self.__projection_elements is not None:
                self.__projection_elements += list(cols)
            else:
                self.__projection_elements = list(cols)
        return self

    @property
    def __projection(self):
        if self.__projection_elements is not None:
            cols = self.__projection_elements.copy()
            fields = self.__fill_cols_fields(cols)
            projection = sql.SQL(",").join(fields)
            return projection
        else:
            return sql.SQL("*")

    def distinct(self):
        self.__distinct_clause = sql.SQL("DISTINCT ")
        return self
    
    def offset(self, start:int):
        """
        Sets the number of rows to use
        for the OFFSET statement
        """
        if not isinstance(start, int):
            raise TypeError(
                f"`start` number to use as offset must be an integer. Was resolved to {type(start)}"
            )
        if start < 0:
            raise ValueError(f"offset `start` must be greater than 0. Got '{start}'")
        self.__offset_clause = sql.SQL(" OFFSET {start} ROWS").format(
            start=sql.Literal(start)
        )
        return self

    def table(self, table_name: str):
        if not table_name:
            raise ValueError("table_name argument can't be resolved to False.")
        # Allow relation (for schemas):
        if isinstance(table_name, QueryFunc):
            self.__table_name=table_name.value
            return self
        self.__table_name = sql.SQL("{table_name}").format(
            table_name=sql.Identifier(table_name)
        )
        return self

    def join(self, table_name: str, on: dict = {}, how="INNER"):
        """
        Sets the FROM statement with a JOIN query.

        Parameters
        ----------
        `table_name` <str>
            Name of the table to join
        `on` <dict>|<queries.QueryOperation>
            If dictionary then the JOIN will be applied
            as key=value joined by an 'AND' if more than one key.
            If a QueryOperation it's value will be used.
        `how` <str>
            Type of JOIN to use. Defaults to INNER.
            Accepts:
                'RIGHT'['OUTER'],'LEFT'['OUTER'],'FULL'['OUTER'],'INNER'.
        """
        how_values = [
            "RIGHT",
            "LEFT",
            "FULL",
            "RIGHT OUTER",
            "LEFT OUTER",
            "FULL OUTER",
            "INNER",
        ]
        if not how.upper() in how_values:
            raise ValueError(f"Value of how '{how.upper()}' is not an allowed value.")
        if not table_name:
            raise ValueError("table_name argument can't be resolved to False.")
        if isinstance(table_name, QueryFunc): # Assume a relation
            table_id = table_name.value
        else:
            table_name = table_name.rstrip()
            table_id = sql.SQL("{table_name}").format(table_name=sql.Identifier(table_name))
        on_statement = None
        if isinstance(on, dict):
            on_ops = []
            for key, value in on.items():
                resolved_op = QueryOp.equals(value).resolve(key)
                on_ops.append(resolved_op)
            on = QueryOperation.q_and(*on_ops)
        if isinstance(on, QueryOperation):
            op_data = on.to_dict()
            on_statement = op_data["operation"]
            self.__query_params.update(op_data["params"])
        else:
            raise TypeError(
                f"Error, type of argument 'on' was not resolved. Got {type(on)}"
            )
        if on_statement:
            on_statement = sql.SQL(" ON ") + on_statement
        else:
            on_statement = sql.SQL("")
        self.__join_clause = (
            sql.SQL(" " + how.upper()) + sql.SQL(" JOIN ") + table_id + on_statement
        )
        return self

    def where(self, filter, filter_mappings={}, join_or=False):
        """
        Sets the filters to pass to the WHERE statement

        Parameters
        ----------
        `filter` <dict>|<queries.QueryOperation>
            Filters to parse as a WHERE statement,
            - If <dict>:
                It will be mapped as key1=value1 AND key2=value2, ..."
            - If <ResolvedQueryOp>
                It will be converted to a QueryOperation.
            - If <QueryOperation>:
                The operation as a raw string will be passed.
        `filter_mappings` <dict>
            Maps the types of a value in filters to the callable
            function in the dictionary values.
            Example if `filter`={'name':'dora'} and `filter_mappings`={'name':str.upper}
            Then it would be passed as name=DORA.
            Not supported when `filter` is a QueryOperation

        `join_or` <bool>
            If True then the filters will be joined with an OR operator instead
            of an AND operator. Not supported when `filter` is a QueryOperation
        """
        op_statement = ""
        if isinstance(filter, (dict, list)):
            op_statement = (
                self._parse_where_query(filter, filter_mappings, join_or)
                if filter is not None
                else None
            )
        elif isinstance(filter, ResolvedQueryOp):
            op_statement = filter.to_operation()
        elif isinstance(filter, QueryOperation):
            op_statement = filter
        else:
            raise TypeError(
                "Type not recognized for parameter 'filters'. "
                f"Was resolved to '{type(filter)}'"
            )
        sql_operation = op_statement.to_dict()
        self.__query_params.update(sql_operation["params"])
        self.__where_clause = sql.SQL(" WHERE ") + sql_operation["operation"]
        return self
    
    def having(self, filter, filter_mappings={}, join_or=False):
        """
        Sets the filters to pass to the HAVING statement.

        Arguments
        ---------
        Takes the same arguments as the `where` method.
        """
        op_statement = ""
        if isinstance(filter, dict):
            op_statement = (
                self._parse_where_query(filter, filter_mappings, join_or)
                if filter is not None
                else None
            )
        elif isinstance(filter, QueryOperation):
            op_statement = filter
        else:
            raise TypeError(
                "Type not recognized for parameter 'filters'. "
                f"Was resolved to '{type(filter)}'"
            )
        sql_operation = op_statement.to_dict()
        self.__query_params.update(sql_operation["params"])
        self.__having_clause = sql.SQL(" HAVING ") + sql_operation["operation"]
        return self

    def group_by(self, *cols):
        if not cols:
            raise ValueError("You must add at least one column in the args.")
        fields = self.__fill_cols_fields(cols)
        self.__group_clause = sql.SQL(" GROUP BY ") + sql.SQL(",").join(fields)
        return self

    def order_by(self, *cols: str, ascending: bool = True):
        """
        Sets the values for the ORDER BY statement.

        Arguments
        ---------
            `cols` <args of strings>
                Name of olumns to pass to the order by statement.

            `ascending` <bool>
                If True then ascending order will be used,
                otherwise it will use descending order.
                Defaults to True.
        """
        if not cols:
            raise ValueError("At least one column name must be passed to order by.")
        order_direction = " ASC" if ascending else " DESC"
        fields = self.__fill_cols_fields(cols)
        self.__order_clause = (
            sql.SQL(" ORDER BY ") + sql.SQL(",").join(fields) + sql.SQL(order_direction)
        )
        return self

    def limit(self, number: int):
        """
        Limits the number of results for the query.
        """
        if not isinstance(number, int):
            raise TypeError(
                f"Number to use as limit must be an integer. Was resolved to {type(number)}"
            )
        self.__limit_clause = sql.SQL(" LIMIT {number}").format(
            number=sql.Literal(number)
        )
        return self

    def resolve(self):
        """
        Resolves the built clauses into a full SQL query.
        """
        assert (
            self.__projection_elements or self.__projection_elements is None
        ), "Values for SELECT can't be empty"
        assert self.__table_name, "Table name for FROM clause can't be empty."
        assert (
            not any_duplicated(self.__projection_elements)
            or self.__projection_elements is None
        ), "Values for SELECT can't be repeated, Try using aliases."
        distinct = join = where = group = having = order = offset = limit = sql.SQL("")
        if self.__distinct_clause:
            distinct = self.__distinct_clause
        if self.__join_clause:
            join = self.__join_clause
        if self.__where_clause:
            where = self.__where_clause
        if self.__group_clause:
            group = self.__group_clause
        if self.__having_clause:
            having = self.__having_clause
        if self.__order_clause:
            order = self.__order_clause
        if self.__offset_clause:
            offset = self.__offset_clause
        if self.__limit_clause:
            limit = self.__limit_clause
        self.__query = (  # Join all sql.SQL objects
            sql.SQL("SELECT ")
            + distinct
            + self.__projection
            + sql.SQL(" FROM ")
            + self.__table_name
            + join
            + where
            + group
            + having
            + order
            + offset
            + limit
        )

    @staticmethod
    def __fill_cols_fields(cols):
        fields = []
        for c in cols:
            if isinstance(c, QueryFunc):
                fields.append(c.value)
            elif c == "*":
                fields.append(sql.SQL("*"))
            elif isinstance(c, Literal):
                fields.append(sql.SQL(c.value))
            elif isinstance(c, (sql.Literal, sql.Identifier, sql.Composable, sql.SQL)):
                fields.append(c)
            else:
                fields.append(sql.Identifier(c))
        return fields

    def _parse_where_query(
        self, filters: dict, type_mapping: dict = {}, join_or: bool = False
    ):
        """
        Parses a dictionary with the filters to apply to a WHERE
        query and returns the valid WHERE statement as a string.

        Parameters
        ----------
            `filters` <dict>
                Filters to apply. Should be a dictionary
                where the keys are name of columns and
                their values the filters you want to apply to the column.
                If a value is of QueryOp type, then its 'resolve'
                method will be called using its key.
                Otherwise it will be resolved as key=value.

            `type_mapping` <dict of callable>
                Mapping to apply a callable function to the values
                of the filters before adding it to the query.
                The keys in this dictionary should also be
                in `filters` to take effect.

            `join_or` <bool>
                If True the filters will be joined with an 'OR' operator,
                otherwise they'll be joined with an 'AND'.

        Returns
        -------
            `str`
                String with valid WHERE statement.

        Raises
        ------
            `ValueError`
                If no elements in `filters`
        """
        if not filters:
            raise ValueError("filters can't be empty.")
        ops = []
        for field, value in filters.items():
            if field in type_mapping:
                mapping = type_mapping[field]
                value = mapping(value)
            elif isinstance(value, QueryOp):
                ops.append(value.resolve(field))
            else:
                resolved_op = QueryOp.equals(value).resolve(field)
                ops.append(resolved_op)
        if join_or:
            operation = QueryOperation.q_or(*ops)
        else:
            operation = QueryOperation.q_and(*ops)
        return operation
