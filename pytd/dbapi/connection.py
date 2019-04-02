import os

from ..client import Client
from .error import NotSupportedError


class Connection(object):

    def __init__(self, apikey=None, endpoint=None, **kwargs):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']
        kwargs['apikey'] = apikey

        if endpoint is None:
            endpoint = 'https://api.treasuredata.com' if ('TD_API_SERVER' not in os.environ) else os.environ['TD_API_SERVER']
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
        return self.client.get_cursor()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
