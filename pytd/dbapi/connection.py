from ..client import Client
from .error import NotSupportedError


class Connection(object):
    """The DBAPI interface to Treasure Data.

    https://www.python.org/dev/peps/pep-0249/

    The interface internally bundles pytd.Client. All `kwargs` are directly
    passed to the Client interface for initialization. Implementation is
    technically based on the DBAPI interface to Treasure Data's Presto query
    engine, which relies on presto-python-client:
    https://github.com/prestodb/presto-python-client.

    Parameters
    ----------
    apikey : string, optional
        Treasure Data API key. If not given, a value of environment variable
        `TD_API_KEY` is used by default.

    endpoint : string, optional
        Treasure Data API server. If not given, https://api.treasuredata.com is
        used by default. List of available endpoints is:
        https://support.treasuredata.com/hc/en-us/articles/360001474288-Sites-and-Endpoints
    """

    def __init__(self, apikey=None, endpoint=None, **kwargs):
        kwargs['apikey'] = apikey
        kwargs['endpoint'] = endpoint
        self.client = Client(**kwargs)

    @property
    def apikey(self):
        return self.client.apikey

    @property
    def endpoint(self):
        return self.client.endpoint

    @property
    def database(self):
        return self.client.database

    def close(self):
        self.client.close()

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self):
        return self.client.engine.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
