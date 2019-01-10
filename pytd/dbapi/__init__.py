from pytd.dbapi.connection import Connection
from pytd.dbapi.error import (  # noqa
    Error, Warning, InterfaceError, DatabaseError, InternalError,
    OperationalError, ProgrammingError, IntegrityError, DataError,
    NotSupportedError)

apilevel = '2.0'
threadsafety = 3
paramstyle = 'pyformat'


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


__all__ = [
    'apilevel',
    'threadsafety',
    'paramstyle',
    'connect',
    'Connection',
    'Error',
    'Warning',
    'InterfaceError',
    'DatabaseError',
    'InternalError',
    'OperationalError',
    'ProgrammingError',
    'IntegrityError',
    'DataError',
    'NotSupportedError'
]
