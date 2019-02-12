from ..client import Client
from .error import NotSupportedError


class Connection(object):

    def __init__(self, apikey=None, database='sample_datasets', engine='presto'):
        self.client = Client(apikey, database, default_engine=engine)

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
