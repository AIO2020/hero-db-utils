"""
Provides custom exceptions for the database module.
"""
from psycopg2 import DatabaseError


class HeroDatabaseError(Exception):
    """
	Raised on generic database errors.
	"""

    def __init__(self, message, parent_error=None, format_orig=False, *args, **kwargs):

        if format_orig:
            err_msg = None
            if isinstance(parent_error, DatabaseError):
                err_msg = f": '{str(parent_error).capitalize()}'"
            elif hasattr(parent_error, "orig"):
                err_msg = f": '{str(parent_error.orig).capitalize()}'"
            if not parent_error or not err_msg:
                raise ValueError(
                    "@parent_error must be an object with an 'orig' attribute when @format_orig is True "
                    "like an sqlalchemy.exc.SQLAlchemyError object or a psycopg2.DatabaseError instace. "
                    f"It was evaluated to '{type(parent_error)}'"
                )
            message += err_msg
        self.__message = message
        self.parent = parent_error
        super().__init__(*args, **kwargs)

    def __str__(self):
        return self.message

    @property
    def message(self):
        """gets the message value"""
        return self.__message

    @message.setter
    def message(self, value):
        self.__message = value


class HeroDatabaseConnectionError(HeroDatabaseError):
    """
	Raised on a database connection error from sqlalchemy or psycopg2.
	"""

    pass


class HeroDatabaseOperationError(HeroDatabaseError):
    """
	Raised on a database operation error from sqlalchemy or psycopg2.
	"""

    pass

class UnexpectedOperationResultError(HeroDatabaseError):
    """
    Raised when an unexpected result is returned from
    a database operation.
    """
    pass