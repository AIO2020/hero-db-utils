"""
PostgreSQL query operations generation module.
"""
from typing import Type, Union
import pandas as pd
from datetime import datetime
import numbers

from psycopg2 import sql
from psycopg2.extensions import connection

from hero_db_utils.utils import short_random_id
from hero_db_utils.utils.dtypes import Literal
from hero_db_utils.utils.functional import is_iter


class QueryFunc:
    """
    Represents a function in the query that can be used for aggregations
    like count(*), min("column"), avg("column"), etc.

    Normally you wouldn't initialize an object of this class manually
    but using one of the @staticmethods that return an instance of the class.
    """

    @property
    def value(self):
        return self.__value

    def _set_func_value(self, val: sql.Composed):
        self.__value = val

    @staticmethod
    def alias(col: str, alias: str):
        """
        Sets an alias for the given column.
        """
        if not col or not alias:
            raise ValueError("None of the arguments to alias can't be empty.")
        col_alias = sql.Identifier(alias)
        col_format = QueryFunc._to_sql_format(col)
        val = sql.SQL("{col} AS {alias}").format(col=col_format, alias=col_alias)
        func = QueryFunc()
        func._set_func_value(val)
        return func

    @staticmethod
    def count(col=None, alias: str = None):
        """
        A count function on the specified identifier.
        By default it resolves to a count(*).
        """
        if not col:
            if not alias:
                val = sql.SQL("COUNT(*)")
            else:
                val = sql.SQL(f"COUNT(*) AS {alias}")
        else:
            return QueryFunc._resolve_func("COUNT", col, alias=alias)
        func = QueryFunc()
        func._set_func_value(val)
        return func

    @staticmethod
    def _resolve_func(func_name, col, alias: str = None):
        col_format = QueryFunc._to_sql_format(col)
        if alias:
            col_alias = sql.Identifier(alias)
            val = sql.SQL("%s({col}) AS {alias}" % (func_name.upper())).format(
                col=col_format, alias=col_alias
            )
        else:
            val = sql.SQL("%s({col})" % (func_name.upper())).format(col=col_format)
        func = QueryFunc()
        func._set_func_value(val)
        return func

    @staticmethod
    def _to_sql_format(col):
        if col == "*":
            return sql.SQL("*")
        if isinstance(col, QueryFunc):
            return col.value
        if isinstance(col, Literal):
            return sql.SQL(col.value)
        return sql.Identifier(col)

    @staticmethod
    def min(col, alias: str = None):
        function_name = "min"
        return QueryFunc._resolve_func(function_name, col, alias=alias)

    @staticmethod
    def max(col, alias: str = None):
        function_name = "max"
        return QueryFunc._resolve_func(function_name, col, alias=alias)

    @staticmethod
    def avg(col, alias: str = None):
        function_name = "avg"
        return QueryFunc._resolve_func(function_name, col, alias=alias)

    @staticmethod
    def upper(col, alias: str = None):
        function_name = "upper"
        return QueryFunc._resolve_func(function_name, col, alias=alias)

    @staticmethod
    def lower(col, alias: str = None):
        function_name = "lower"
        return QueryFunc._resolve_func(function_name, col, alias=alias)

    @staticmethod
    def relation(*relations: str, alias:str=None):
        """
        Joins the relations given as Identifier following
        the specified order.

        Example:
        --------
        >>> q = QueryFunc.relation("mytable","columnA")
        >>> q.value.as_string(conn)
        '"mytable"."columnA"'
        """
        rels = []
        for r in relations:
            sql_format = QueryFunc._to_sql_format(r)
            rels.append(sql.SQL("{}").format(sql_format))
        sql_value = sql.SQL(".").join(rels)
        if alias:
            col_alias = sql.Identifier(alias)
            sql_value = sql.SQL("{col} AS {alias}").format(
                col=sql_value, alias=col_alias
            )
        func = QueryFunc()
        func._set_func_value(sql_value)
        return func
    
    @staticmethod
    def distinct(col, alias: str = None):
        function_name = "distinct"
        return QueryFunc._resolve_func(function_name, col, alias=alias)


class ResolvedQueryOp:
    """
    Represents a single query operation statement
    with left and right operators like 'a=b'.

    Normally you wouldn't initiate an object of this class by
    yourself but using the resolve() method of a QueryOp object.
    """

    def __init__(self, value, query_params: dict = None):
        if query_params is None:
            query_params = {}
        elif not isinstance(query_params, dict):
            raise TypeError("params must be a dictionary or None")
        self._value = value
        self._query_params = query_params

    def to_operation(self):
        """
        Transforms the ResolvedQueryOp to a QueryOperation
        object that has a single operation statement.
        """
        return QueryOperation.q_and(self)

class QueryOp:
    """
    Represents the right hand side of an operation statement for
    a query. Like " = 'b'".

    Normally you wouldn't initiate an object of this class by
    yourself, instead you would use the provided staticmethods to create
    an operation that can then be resolved using obj.resolve()

    Example:
    ---------
    Let's pretend we have a table of persons "Person" and we want to select
    the names of the persons that are younger than 50 years old.
    Let 'conn' be an initialized psycopg2 connection to our database:

    >>> operator = QueryOp.less_than(50)
    >>> operation = operator.resolve("age") # '"age"<50' as a ResolvedQueryOp object.
    >>> query = DBQuery() # Initializes the query to build
    >>> query.table("Person") # Identifies the table we want to grab.
    >>> query.values(["name"]) # Selects the column 'name'.
    >>> query.filter({"age":operation}) # Creates the WHERE statement.
    >>> query.parse_str(conn) # Returns the query as a string.
    'SELECT "name" from Person WHERE "age"<50'
    """

    def __init__(self, value, params: dict = None):
        """
        Instantiates a query operation object.

        Normally you should instantiate objects of this class by calling
        one of the static methods of the class.
        """
        if params is None:
            params = {}
        elif not isinstance(params, dict):
            raise TypeError("params must be a dictionary or None")
        self.__operator = value
        self.__params = params

    @staticmethod
    def _parse_to_sql(value, right=False):
        if isinstance(value, bool):
            return sql.SQL("true" if value else "false"), {}
        elif pd.isnull(value):
            return sql.SQL("NULL"), {}
        elif isinstance(value, QueryFunc):
            return value.value, {}
        elif isinstance(value, Literal):
            return sql.SQL(f"{value}"), {}
        elif isinstance(value, (datetime, pd.Timestamp)):
            value = value.isoformat()
        plcholder = ("opleft_" if not right else "opright_") + short_random_id()
        param = {plcholder: value}
        return sql.SQL("{}").format(sql.Placeholder(plcholder)), param

    def resolve(self, value, isidentifier=True) -> ResolvedQueryOp:
        """
        Resolves the left hand side of the query operator using the given value.

        Parameters
        ----------
            `value`
                Value to use for the right part of the operation.
            `isidentifier`
                If true and value is a string it will be passed
                to the query as a column or table identifier.
                Otherwise it will be passed as normal string.
        """
        if not isidentifier or not isinstance(value, str):
            value, params = self._parse_to_sql(value, right=True)
            left_side = value + sql.SQL(" ")
            self.__params.update(params)
        else:
            value_format = sql.Identifier(value)
            left_side = sql.SQL("{value} ").format(value=value_format)
        operation = left_side + self.__operator
        return ResolvedQueryOp(operation, self.__params)

    @staticmethod
    def less_than(value):
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL("< ") + value
        return QueryOp(queryop, params)

    @staticmethod
    def less_equal_than(value):
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL("<= ") + value
        return QueryOp(queryop, params)

    @staticmethod
    def greater_than(value):
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL("> ") + value
        return QueryOp(queryop, params)

    @staticmethod
    def greater_equal_than(value):
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL(">= ") + value
        return QueryOp(queryop, params)

    @staticmethod
    def between(value1, value2):
        value1, params1 = QueryOp._parse_to_sql(value1)
        value2, params2 = QueryOp._parse_to_sql(value2)
        queryop = sql.SQL("BETWEEN ") + value1 + sql.SQL(" AND ") + value2
        return QueryOp(queryop, {**params1, **params2})

    @staticmethod
    def value_in(values):
        query, params = QueryOp._parse_in_query(values)
        queryop = sql.SQL("IN ") + query
        return QueryOp(queryop, params)

    @staticmethod
    def value_not_in(values):
        query, params = QueryOp._parse_in_query(values)
        queryop = sql.SQL("NOT IN ") + query
        return QueryOp(queryop, params)

    @staticmethod
    def not_equals(value):
        query_value, params = QueryOp._parse_to_sql(value)
        if value is None:
            queryop = sql.SQL("IS NOT ") + query_value
        else:
            queryop = sql.SQL("<> ") + query_value
        return QueryOp(queryop, params)

    @staticmethod
    def equals(value):
        query_value, params = QueryOp._parse_to_sql(value)
        if value is None:
            queryop = sql.SQL("IS ") + query_value
        else:
            queryop = sql.SQL("= ") + query_value
        return QueryOp(queryop, params)

    @staticmethod
    def iequals(value):
        """
        Case insensitive equality test with ILIKE
        Only works on PostgreSQL.
        """
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL("ILIKE ") + value
        return QueryOp(queryop, params)

    @staticmethod
    def not_iequals(value):
        """
        Case insensitive non-equality test with ILIKE
        Only works on PostgreSQL.
        """
        value, params = QueryOp._parse_to_sql(value)
        value, params = QueryOp._parse_to_sql(value)
        queryop = sql.SQL("NOT ILIKE ") + value
        return QueryOp(queryop, params)
    
    @staticmethod
    def _parse_in_query(values):
        from hero_db_utils.queries.postgres.builder import DBQuery
        if isinstance(values, DBQuery):
            query_attrs = values.to_dict()
            query = query_attrs["query"]
            params = query_attrs["params"]
        elif is_iter(values):
            params = {}
            sql_values = []
            for v in values:
                val, param = QueryOp._parse_to_sql(v)
                sql_values.append(val);
                params.update(param);
            query = sql.SQL("(") + sql.SQL(",").join(sql_values) + sql.SQL(")")
        else:
            raise TypeError("values must be a queries.DBQuery or an iterable.")
        return query, params


class QueryOperation:
    """
    Represents an operation statement that can be performed
    in the database.
    Like 'a=b AND b=c OR d=a'
    """

    def __init__(self, val=None, query_params:dict=None):
        if query_params is None:
            query_params = {}
        elif not isinstance(query_params, dict):
            raise TypeError("params must be a dictionary or None")
        self.__val = val
        self.__query_params = query_params.copy()
    
    def copy(self):
        """
        Returns a copy of this object.
        """
        return QueryOperation(self.__val, self.__query_params)

    def to_dict(self) -> dict:
        """
        Returns the value and params of the operation as a dictionary
        with 'operation' and 'params' keys.
        """
        if not self.__val:
            raise ValueError("Value of operation empty.")
        return {"operation": self.__val, "params": self.__query_params}

    def join_or(self, operation):
        op_value, op_param = self._parse_value_params(operation)
        if self.__val is not None:
            self.__val = (
                sql.SQL("(") + self.__val + sql.SQL(") OR (") + op_value + sql.SQL(")")
            )
            self.__query_params.update(op_param)
        else:
            self.__val = op_value
            self.__query_params.update(op_param)

    def join_and(self, operation):
        op_value, op_param = self._parse_value_params(operation)
        if self.__val is not None:
            self.__val = (
                sql.SQL("(") + self.__val + sql.SQL(") AND (") + op_value + sql.SQL(")")
            )
            self.__query_params.update(op_param)
        else:
            self.__val = op_value
            self.__query_params.update(op_param)
    
    def to_representation(self, conn:connection):
        """
        Uses a psycopg2 database connection to
        parse the query to a valid sql string.
        """
        q_data = self.to_dict()
        with conn.cursor() as cursor:
            return cursor.mogrify(q_data["operation"], q_data["params"]).decode()
    
    @staticmethod
    def _parse_value_params(op):
        """
        From the given object extracts the value
        and params parts so a query operation can be built.
        """
        if isinstance(op, QueryOperation):
            op_d = op.to_dict()
            return op_d["operation"], op_d["params"]
        elif isinstance(op, ResolvedQueryOp):
            return op._value, op._query_params
        raise TypeError("op object must be a QueryOperation or QueryOp.")

    @staticmethod
    def q_or(
        *operations:ResolvedQueryOp,
        values:list=[],
        key:str=None
    ):
        """
        Joins the query operations with an OR operator

        Alternatively, a list of `values` can be passed and they will
        be mapped as ResolvedQueryOp 'equals' objects using a common `key`.

        Arguments
        ---------
        `operations`: <args>
            ResolvedQueryOp objects to join.
        `values`: <list>
            List of values that will be in the right hand side of
            the equal sign ('=') of the operations.
        `key`: <Any>
            A common key that will be the in the left hand side
            of all the equal signs ('=') of the operations.

        Examples
        --------
        With 'ResolvedQueryOp' objects:
        >>> from hero_db_utils.query.postgres import QueryOp, QueryOperation
        >>> # Create a client to read the query:
        >>> client = PostgresDatabaseClient(**kwargs)
        >>> # Create the operation:
        >>> op1 = QueryOp.greater_than(10).resolve("score")
        >>> op2 = QueryOp.equals("golf").resolve("game")
        >>> operation = QueryOperation.q_or(op1, op2)
        >>> # Read query:
        >>> query = client.build_query("game_matches", filters=operation)
        >>> query.to_representation(client.connection)
        'SELECT * FROM "game_matches" WHERE ("score" > 10) OR ("game" = \\'golf\\')'

        With a key and values:
        >>> operation = QueryOperation.q_or(values=["golf","pacman","bowling"], key="game")
        >>> # Read query:
        >>> query = client.build_query("game_matches", filters=operation)
        >>> query.to_representation(client.connection)
        'SELECT * FROM "game_matches" WHERE ("game" = \\'golf\\') OR ("game" = \\'pacman\\') OR ("game"=\\'bowling\\')'
        """
        operations = list(operations)
        if bool(values)^bool(key):
            raise ValueError("Params `values` and `key` must both resolve to true or false.")
        for item in values:
            operations.append(QueryOp.equals(item).resolve(key))
        ops = []
        op_params = {}
        for op in operations:
            op_value, op_param = QueryOperation._parse_value_params(op)
            op_params.update(op_param)
            sql_op = sql.SQL("(") + op_value + sql.SQL(")")
            ops.append(sql_op)
        query_op = sql.SQL(" OR ").join(ops)
        return QueryOperation(query_op, op_params)

    @staticmethod
    def q_and(*operations: ResolvedQueryOp, mapping: dict = {}):
        """
        Joins the args query operations with an AND operator

        Arguments
        ---------
        `operations`: <args>
            ResolvedQueryOp objects to join.
        `mapping` <dict>
            Dictionary of key-value pairs to map as ResolvedQueryOp objects,
            items will be formatted as key1=value1 AND key2=value2 AND ...

        With 'ResolvedQueryOp' objects:
        >>> from hero_db_utils.query.postgres import QueryOp, QueryOperation
        >>> # Create a client to read the query:
        >>> client = PostgresDatabaseClient(**kwargs)
        >>> # Create the operation:
        >>> op1 = QueryOp.greater_than(10).resolve("score")
        >>> op2 = QueryOp.equals("golf").resolve("game")
        >>> operation = QueryOperation.q_and(op1, op2)
        >>> # Read query:
        >>> query = client.build_query("game_matches", filters=operation)
        >>> query.to_representation(client.connection)
        'SELECT * FROM "game_matches" WHERE ("score" > 10) AND ("game" = \\'golf\\')'

        With a mapping:
        >>> operation = QueryOperation.q_and(mapping={"game":"golf","player_id":212})
        >>> # Read query:
        >>> query = client.build_query("game_matches", filters=operation)
        >>> query.to_representation(client.connection)
        'SELECT * FROM "game_matches" WHERE ("game" = \\'golf\\') AND ("player_id" = 212)
        """
        operations = list(operations)
        for key, value in mapping.items():
            operations.append(QueryOp.equals(value).resolve(key))
        ops = []
        op_params = {}
        for op in operations:
            op_value, op_param = QueryOperation._parse_value_params(op)
            op_params.update(op_param)
            sql_op = sql.SQL("(") + op_value + sql.SQL(")")
            ops.append(sql_op)
        query_op = sql.SQL(" AND ").join(ops)
        return QueryOperation(query_op, op_params)
