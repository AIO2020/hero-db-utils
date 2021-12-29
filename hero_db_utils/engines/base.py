import logging

from hero_db_utils.exceptions import HeroDatabaseConnectionError
from hero_db_utils.utils import get_connection_url


class DBEngine(object):
    """
    Provides a Base class with session initialization
    and automatic closing to use as parent to classes
    meant for database operations.

    Uses an sqlalchemy backend.
    """

    def __init__(self, session=None, ignore_session=False, **session_kwargs):
        session_kwargs["scoped"] = True
        self._sess_kwargs = session_kwargs
        if not ignore_session:
            if not session:
                self.sess = self._get_session(**session_kwargs)
            else:
                self.sess = session
                self.engine = self.sess.get_bind()
            try:
                self.engine.connect()
            except Exception as e:
                self.close()
                raise HeroDatabaseConnectionError(
                    f"Error trying to connect to database", e, format_orig=True
                ) from e

    @staticmethod
    def get_session(*args, scoped=False, conn_dsn=None, **kwargs):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import scoped_session, sessionmaker

        if not conn_dsn:
            url_argnames = [
                "db_name",
                "db_user",
                "db_psw",
                "db_port",
                "db_host",
                "engine",
            ]
            if (
                not args
                and not kwargs
                or ((len(args) + len(kwargs)) < len(url_argnames))
            ):
                raise ValueError(
                    "Required args to connect to the database: '"
                    + ", ".join(url_argnames)
                    + "'"
                )
            conn_url = get_connection_url(*args, **kwargs)
        else:
            conn_url = conn_dsn
        engine = create_engine(conn_url,)
        if scoped:
            Session = scoped_session(sessionmaker(bind=engine))
        else:
            Session = sessionmaker(bind=engine)
        return Session

    def _get_session(self, **session_kwargs):
        self._Session = self.get_session(**session_kwargs)
        self.sess = self._Session()
        self.engine = self.sess.get_bind()
        return self.sess

    def __del__(self):
        self.close()

    def close(self):
        """
        Close database session if it was opened:
        """
        if hasattr(self, "sess") and self.sess is not None:
            self.sess.close()
            try:
                self.engine.dispose()
            except KeyError:
                logging.debug(f"KeyError when disposing engine.", exc_info=True)
            if hasattr(self, "_Session"):
                self._Session.remove()
            self.sess = None
