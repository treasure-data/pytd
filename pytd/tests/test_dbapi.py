from pytd import dbapi
from pytd.connection import Connection

# https://www.python.org/dev/peps/pep-0249/


def test_constructors():
    assert isinstance(dbapi.connect(), Connection)


def test_globals():
    assert hasattr(dbapi, 'apilevel')
    assert hasattr(dbapi, 'threadsafety')
    assert hasattr(dbapi, 'paramstyle')


def test_exceptions():
    assert issubclass(dbapi.Error, Exception)
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.InterfaceError, Exception)
    assert issubclass(dbapi.DatabaseError, Exception)
    assert issubclass(dbapi.InternalError, Exception)
    assert issubclass(dbapi.OperationalError, Exception)
    assert issubclass(dbapi.ProgrammingError, Exception)
    assert issubclass(dbapi.IntegrityError, Exception)
    assert issubclass(dbapi.DataError, Exception)
    assert issubclass(dbapi.NotSupportedError, Exception)
