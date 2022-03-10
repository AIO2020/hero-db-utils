

class EnvVariablesConf:
    """
    Key names of the environment variables used
    for configuration.
    """

    TEST_DATABASE_NAME_KEY = "HERO_TEST_DATABASE_NAME"
    TEST_DATABASE_NAME_DEFAULT = "_hero_testing_db"

    KEY_NAMES = {
        "DBENGINE":"HERO_DATABASE_ENGINE",
        "DBNAME": "HERO_DATABASE_NAME",
        "DBUSER": "HERO_DATABASE_USERNAME",
        "DBPASSWORD": "HERO_DATABASE_PASSWORD",
        "DBHOST": "HERO_DATABASE_HOST",
        "DBPORT": "HERO_DATABASE_PORT",
    }
    DEFAULT_VALUES = {
        "DBENGINE":"postgres"
    }

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
    import datetime as dt
    import io

    _default_ = {
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
        "interval": dt.timedelta,
        "boolean": bool,
        "binary": bytes,
        "file": io.FileIO,
    }
