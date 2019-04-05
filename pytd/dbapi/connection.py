from ..client import Client
from .error import NotSupportedError


class Connection(object):

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
