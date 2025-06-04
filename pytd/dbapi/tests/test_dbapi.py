import os

from pytd import dbapi

# https://www.python.org/dev/peps/pep-0249/


def test_constructors():
    # Set mock API key for testing if not set
    original_key = os.environ.get("TD_API_KEY")
    if not original_key:
        os.environ["TD_API_KEY"] = "1/test-key"

    try:
        assert isinstance(dbapi.connect(), dbapi.Connection)
    finally:
        # Restore original environment
        if original_key:
            os.environ["TD_API_KEY"] = original_key
        else:
            os.environ.pop("TD_API_KEY", None)


def test_globals():
    assert hasattr(dbapi, "apilevel")
    assert hasattr(dbapi, "threadsafety")
    assert hasattr(dbapi, "paramstyle")


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
