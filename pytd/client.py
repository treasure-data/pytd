import os
import logging

from pytd.writer import SparkWriter
from pytd.query_engine import PrestoQueryEngine, HiveQueryEngine

logger = logging.getLogger(__name__)


class Client(object):

    def __init__(self, apikey=None, endpoint=None, database='sample_datasets', engine='presto', header=True):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        if endpoint is None:
            if 'TD_API_SERVER' not in os.environ:
                raise ValueError("either argument 'endpoint' or environment variable 'TD_API_SERVER' should be set")
            endpoint = os.environ['TD_API_SERVER']

        self._connect_query_engine(apikey, endpoint, database, engine, header)

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database
        self.header = header

        self.writer = None

    def close(self):
        self.engine.close()
        if self.writer is not None:
            self.writer.close()

    def query(self, sql):
        header = self.engine.create_header('Client#query')
        return self.engine.execute(header + sql)

    def load_table_from_dataframe(self, df, table, if_exists='error'):
        if self.writer is None:
            self.writer = SparkWriter(self.apikey, self.endpoint)

        destination = table
        if '.' not in table:
            destination = self.database + '.' + table

        self.writer.write_dataframe(df, destination, if_exists)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _connect_query_engine(self, apikey, endpoint, database, engine, header):
        if engine == 'presto':
            self.engine = PrestoQueryEngine(apikey, endpoint, database, header)
        elif engine == 'hive':
            self.engine = HiveQueryEngine(apikey, endpoint, database, header)
        else:
            raise ValueError('`engine` should be "presto" or "hive"')
