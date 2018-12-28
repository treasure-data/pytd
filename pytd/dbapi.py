from pytd.connection import Connection
from pytd.error import (  # noqa
    Error, Warning, InterfaceError, DatabaseError, InternalError,
    OperationalError, ProgrammingError, IntegrityError, DataError,
    NotSupportedError)

apilevel = '2.0'
threadsafety = 3
paramstyle = 'pyformat'


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
