"""
Database utils
"""

import collections
import os
from urllib.parse import quote_plus
import string
import random

from hero_db_utils.constants import EnvVariablesConf


def any_duplicated(l: list) -> bool:
    """
    Checks if the given list contains any duplicated values.
    """
    for _, count in collections.Counter(l).items():
        if count > 1:
            return True
    return False


def set_conn_url_params(conn_uri:str, params:dict={}, search_path=[]):
    if not params and not search_path:
        return conn_uri
    splt_params = conn_uri.split("?")
    if len(splt_params)>1:
        conn_uri, existing_params_str = conn_uri.split("?")
        existing_params_lst = existing_params_str.split("&")
        existing_params = {
            val[0]:val[1] for val in
            map(lambda v:v.split("="), existing_params_lst)
        }
        existing_params.update(params)
        params = existing_params
    if search_path :
        search_path_str = f"-csearch_path%3D" + ",".join(search_path)
        params["options"] = search_path_str
    params_str = "&".join([f"{key}={value}" for key,value in params.items()])
    conn_uri = (conn_uri + "?" + params_str)
    return conn_uri

def get_connection_url(
    db_name, db_user, db_psw, db_port, db_host, engine, search_path=[], **params
):
    db_name = quote_plus(db_name.encode() if isinstance(db_name, str) else db_name)
    db_user = quote_plus(db_user.encode() if isinstance(db_user, str) else db_user)
    db_psw = quote_plus(db_psw.encode() if isinstance(db_psw, str) else db_psw)
    engine = quote_plus(engine.encode() if isinstance(engine, str) else engine)
    conn_uri =  f"{engine}://{db_user}:{db_psw}@{db_host}:{db_port}/{db_name}"
    if params or search_path:
        conn_uri = set_conn_url_params(conn_uri, params, search_path=search_path)
    return conn_uri

def objs_to_dicts(l_objs: list) -> list:
    """
    Converts a list of objects to a list of dictionaries
    using the object's '__dict__'  attribute.
    """
    l = []
    for p in l_objs:
        try:
            d = {
                key: value
                for key, value in p.__dict__.items()
                if not key.startswith("_")
            }
        except AttributeError:
            # Maybe its a result row:
            d = {key: getattr(p, key) for key in p._fields}
        l.append(d)
    return l


def short_random_id() -> str:
    """
    Generates a random string of 10 characters that
    starts with a random letters of the ascii alphabet or '_'
    and is followed by 9 randomly chosen digits, letters or '_'.

    Example:
    --------
        >>> short_random_id()
        'mcy_t5l0fp'

    Returns
    -------
        `str`
            Randomly generated 10 character string.
    """
    alphabet = string.ascii_lowercase + string.digits + "_"
    return "".join(random.choices(string.ascii_lowercase + "_")) + "".join(
        random.choices(alphabet, k=9)
    )


def add_db_args(
    parser,
    default_username: str = None,
    default_port: int = None,
    default_host: str = None,
):
    """
    Adds arguments to connect to the database
    to an argparser.ArgumentParser object.
    """
    parser.add_argument(
        "--db_name",
        type=str,
        action="store",
        help="Name of database to use",
        required=True,
    )

    parser.add_argument(
        "--db_port",
        type=int,
        action="store",
        help="Port for database connection",
        default=None if not default_port else default_port,
        required=False if default_port else True,
    )

    parser.add_argument(
        "--db_username",
        type=str,
        action="store",
        help="Username for database connection",
        default=None if not default_username else default_username,
        required=False if default_username else True,
    )

    parser.add_argument(
        "--db_password",
        type=str,
        action="store",
        help="Password for database connection",
        required=True,
    )

    parser.add_argument(
        "--db_host",
        type=str,
        action="store",
        help="Host for database connection",
        default=None if not default_host else default_host,
        required=False if default_host else True,
    )

def get_env_params(engine=None):
    """
    Retrieves the present environment variables
    that can be used to configure the database connection.
    """
    conf = EnvVariablesConf
    env_db_engine = os.environ.get(
        conf.KEY_NAMES["DBENGINE"],
        conf.DEFAULT_VALUES["DBENGINE"],
    )
    engine = engine or env_db_engine
    if engine == "postgres":
        conf = EnvVariablesConf.Postgres
    return {
        "db_engine":engine,
        "db_name": os.environ.get(
            conf.KEY_NAMES["DBNAME"],
            conf.DEFAULT_VALUES.get("DBNAME"),
        ),
        "db_username": os.environ.get(
            conf.KEY_NAMES["DBUSER"],
            conf.DEFAULT_VALUES.get("DBUSER"),
        ),
        "db_password": os.environ.get(
            conf.KEY_NAMES["DBPASSWORD"],
            conf.DEFAULT_VALUES.get("DBPASSWORD"),
        ),
        "db_host": os.environ.get(
            conf.KEY_NAMES["DBHOST"],
            conf.DEFAULT_VALUES.get("DBHOST"),
        ),
        "db_port": os.environ.get(
            conf.KEY_NAMES["DBPORT"],
            conf.DEFAULT_VALUES.get("DBPORT"),
        )
    }
