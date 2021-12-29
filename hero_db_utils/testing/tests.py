"""
Copyright Notice:

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
import unittest
import os

from hero_db_utils.testing.helpers import assert_df_equal
from hero_db_utils.clients import PostgresDatabaseClient
from hero_db_utils.constants import EnvVariablesConf


class BaseTest(unittest.TestCase):

    def assertDataFrameEqual(
        self, a, b, ignore_idx=False, ignore_dtypes=False, **pd_test_kwargs
    ):
        """
        Checks if two pandas dataframes are equal.
        """
        try:
            assert_df_equal(a, b, ignore_idx, ignore_dtypes, **pd_test_kwargs)
        except AssertionError as e:
            raise self.failureException("Dataframes are not Equal") from e


class PostgresDatabaseBaseTest(BaseTest):
    """
    Base class for tests that use the heroer database
    Creates a new empty database '_hero-testing' to run the tests
    with and then deletes it when the object is destroyed.

    On Instantiation the database manager for this test database
    will be available as an attribute 'pgclient'.

    You must set the environment variables to connect to the
    database. See: constants.EnvVariablesConf
    """

    # Ensure sqlalchemy is installed so inserts can be performed:
    try:
        import sqlalchemy
    except ModuleNotFoundError as e:
        logging.error(
            "Module 'sqlalchemy' is required to run the tests. Install it with 'python -m pip install sqlalchemy==1.4.0'"
        )
        raise ModuleNotFoundError(
            "Optional third party module 'sqlalchemy' is required"
        ) from e

    @classmethod
    def setUpClass(cls):
        if os.path.exists(".env"):
            try:
                from dotenv import load_dotenv
            except ModuleNotFoundError:
                logging.warning("'.env' file was found but module python-dotenv is not installed.")
            else:
                try:
                    load_dotenv(".env")
                except Exception as e:
                    logging.error("Error loading '.env' file:")
                    logging.debug("dotenv error stack:", exc_info=True)

    def setUp(self):
        """
        Initiates the test database before each test.
        """
        self.__init_database()

    def __init_database(self):
        test_database_name = os.environ.get(
            EnvVariablesConf.TEST_DATABASE_NAME_KEY,
            EnvVariablesConf.TEST_DATABASE_NAME_DEFAULT,
        )
        os.environ[EnvVariablesConf.Postgres.KEY_NAMES["DBNAME"]] = test_database_name
        # Set database environment name for this context:
        conn_kwargs = PostgresDatabaseClient.get_params_from_env_variables()
        self.__drop_test_database()
        self.pgclient = PostgresDatabaseClient(**conn_kwargs, create_database=True,)
        self.setup_database()

    def setup_database(self):
        """
        Abstract method, called after database initialization
        that can be used to populate the tables of the database
        using the database_manager.
        """
        pass

    def tearDown(self):
        """
        Drops the test database created, after each test.
        """
        self.__drop_test_database()
        self.__dropped_at_exit = True

    def __drop_test_database(self):
        """
        Drops the test database
        """
        test_database_name = os.environ.get(
            EnvVariablesConf.TEST_DATABASE_NAME_KEY,
            EnvVariablesConf.TEST_DATABASE_NAME_DEFAULT,
        )
        os.environ[EnvVariablesConf.Postgres.KEY_NAMES["DBNAME"]] = "postgres"
        # Set database environment name for this context:
        conn_kwargs = PostgresDatabaseClient.get_params_from_env_variables()
        pgclient = PostgresDatabaseClient(**conn_kwargs,)
        pgclient.delete_database(test_database_name, force=True)

    def __del__(self):
        # Dlete database if exists:
        was_dropped = True
        try:
            was_dropped = self.__dropped_at_exit
        except AttributeError:
            was_dropped = False
        if not was_dropped:
            self.__drop_test_database()

