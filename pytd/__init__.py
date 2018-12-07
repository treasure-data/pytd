import prestodb
import os


__version__ = '0.0.1'


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class Connection(object):

    def __init__(self, apikey=None, database='sample_datasets'):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        self.td_presto = prestodb.dbapi.connect(
            host='api-presto.treasuredata.com',
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database
        )

    def cursor(self):
        return self.td_presto.cursor()
