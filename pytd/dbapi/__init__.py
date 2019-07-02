from ..client import Client
from .connection import Connection
from .error import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

apilevel = "2.0"
threadsafety = 3
paramstyle = "pyformat"


def connect(client=None):
    """Establish a DB-API connection to Treasure Data.

    Parameters
    ----------
    client : pytd.Client, optional
        A client used to connect to Treasure Data. If not given, a client is
        created using default options.
    """
    if client is None:
        client = Client()
    return Connection(client)


__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
]
