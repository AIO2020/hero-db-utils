import abc
import pandas as pd
import logging

class SQLBaseClient(abc.ABC):

    def get_logger(self) -> logging.Logger:
        return logging.getLogger(
            f"hero_db_utils.{self.__class__.__name__}.{self.db_name}"
        )

    @abc.abstractmethod
    def select(*args, **kwargs)  -> pd.DataFrame:
        """
        Returns a dataframe with the selection after building the query using
        the given parameters.
        """
        pass

    @abc.abstractmethod
    def insert_from_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        **to_sql_kwargs,
    ):
        """
        Inserts the rows from the dataframe into the table.
        """
        pass

    @abc.abstractmethod
    def read_sql_query(*args, **kwargs) -> pd.DataFrame:
        """
        Passes the `sql_query` argument as sql and the rest of named arguments
        to the `read_sql_query` method from pandas using the object's open
        database connection.
        """
        pass

    @staticmethod
    def get_params_from_settings(db_profile: str = None, db_settings: dict = None):
        """
        Gets the paramaters to initiate an instance of the class from the
        django project's settings.py 'DATABASES' attribute [1] or a dictionary with settings.

        Parameters
        ----------
        `db_profile`: str
            Name of the key with the configuration of the database to use. Example: 'default'.
        `db_settings`: dict
            Optional dictionary with key-value pairs of the database configuration
            required to connect to it. Corresponds with the values of the inner dictionaries
            of django's DATABASES settings.
            Use this if you want get the parameters of a database not specified in settings.py.
            `db_profile` takes precedence if it is also defined when calling the method.

        Returns
        -------
        `dict`
            Dictionary of kwargs that can be passes as a mappable to the class constructor.

        Raises
        ------
        `ValueError`
            If no params were defined when calling this method.
        `KeyError`
            If the name given in 'db_profile' is not a key in the project
            settings' DATABASES attribute.

        References
        --------
        [1] https://docs.djangoproject.com/en/3.2/ref/settings/#databases
        """
        if db_profile is not None:
            from django.conf import settings

            db_config = settings.DATABASES[db_profile]
            return {
                "db_name": db_config["NAME"],
                "db_username": db_config.get("USER"),
                "db_password": db_config.get("PASSWORD"),
                "db_host": db_config.get("HOST"),
                "db_port": db_config.get("PORT"),
            }
        elif db_settings is not None:
            return {
                "db_name": db_settings["NAME"],
                "db_username": db_settings.get("USER"),
                "db_password": db_settings.get("PASSWORD"),
                "db_host": db_settings.get("HOST"),
                "db_port": db_settings.get("PORT"),
            }
        raise ValueError("You must specify a parameter db_profile or db_settings.")
    