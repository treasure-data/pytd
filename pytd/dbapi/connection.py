from .error import NotSupportedError


class Connection(object):
    """The DBAPI interface to Treasure Data.

    https://www.python.org/dev/peps/pep-0249/

    The interface internally bundles pytd.Client. Implementation is technically
    based on the DBAPI interface to Treasure Data's Presto query engine, which
    relies on presto-python-client:
    https://github.com/prestodb/presto-python-client.

    Parameters
    ----------
    client : pytd.Client, optional
        A client used to connect to Treasure Data.
    """

    def __init__(self, client):
        self.client = client

    def close(self):
        self.client.close()

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self):
        return self.client.default_engine.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
