import pytd
from pytd.error import NotSupportedError


class Connection(object):

    def __init__(self, apikey=None, database='sample_datasets'):
        self.client = pytd.client.Client(apikey, database)

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
