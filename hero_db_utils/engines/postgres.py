"""
Module to represent database engines.
"""
import psycopg2

from hero_db_utils.exceptions import HeroDatabaseConnectionError
from hero_db_utils.utils import get_connection_url, set_conn_url_params
from hero_db_utils.engines.base import DBEngine

class PsycopgDBEngine:
    """
    A database engine that uses psycopg2 instead of sqlalchemy.
    """

    def __init__(self, **sess_kwargs):
        sess_kwargs["engine"] = "postgresql"
        for key, value in sess_kwargs.items():
            if value is None:
                raise ValueError(
                    f"Cant connect to database session with '{key}' value None."
                )
        conn_dsn = get_connection_url(**sess_kwargs)
        self.__sess = PsycopgSession(conn_dsn)
    
    def set_search_path(self, schemas:list):
        """
        Adds the search path to the postgres session.
        """
        dsn = self.__sess.conn_dsn
        new_dsn = set_conn_url_params(dsn, search_path=schemas)
        del self.__sess
        self.__sess = PsycopgSession(new_dsn)
    
    @property
    def sess(self):
        return self.__sess

    @property
    def connection(self):
        return self.sess.connect()

    def _get_dsn_connection(self) -> str:
        return self.sess.conn_dsn

class PsycopgSession:
    def __init__(self, dsn):
        self.__conn_dsn = dsn
        # Test connection:
        try:
            self.connect()
        except (psycopg2.DatabaseError) as e:
            raise HeroDatabaseConnectionError(
                f"Error trying to connect to database", e, format_orig=True
            ) from e

    def connect(self):
        return psycopg2.connect(dsn=self.__conn_dsn)

    def cursor(self):
        return self.connect().cursor()
    
    @property
    def conn_dsn(self):
        return self.__conn_dsn

def dbengine_from_psycopg(psycopg_eng: PsycopgDBEngine, **sess_kwargs):
    """
    From a PsycopgDBEngine object returns an initialized database engine
    that uses an sqlalchemy backend to connect to the postgreSQL database.
    """
    try:
        import sqlalchemy
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError("Optional module 'sqlalchemy' not installed.") from e
    dsn = psycopg_eng._get_dsn_connection()
    _Session = DBEngine.get_session(conn_dsn=dsn, **sess_kwargs)
    return DBEngine(session=_Session())
