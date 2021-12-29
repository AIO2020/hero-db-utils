import datetime as dt


class EnvVariablesConf:
    """
    Key names of the environment variables used
    for configuration.
    """

    TEST_DATABASE_NAME_KEY = "HERO_TEST_DATABASE_NAME"
    TEST_DATABASE_NAME_DEFAULT = "_hero_testing_db"

    class Postgres:

        KEY_NAMES = {
            "DBNAME": "HERO_POSTGRES_DBNAME",
            "DBUSER": "HERO_POSTGRES_USERNAME",
            "DBPASSWORD": "HERO_POSTGRES_PASSWORD",
            "DBHOST": "HERO_POSTGRES_HOST",
            "DBPORT": "HERO_POSTGRES_PORT",
        }

        DEFAULT_VALUES = {"DBUSER": "postgres", "DBHOST": "localhost", "DBPORT": "5432"}


class DataTypes:

    PG_DATA_TYPES_MAP = {
        "integer": int,
        "bigint": int,
        "smallint": int,
        "serial": int,
        "bigserial": int,
        "float": float,
        "double precision": float,
        "text": str,
        "timestamp": dt.datetime,
        "date": dt.date,
        "time": dt.time,
        "interval": dt.datetime,
        "boolean": bool,
        "binary": bytes,
        "file": bytes,
    }
