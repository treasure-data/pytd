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

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.engine = self._get_engine(engine, header)

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

        self.writer.write_dataframe(df, self.database, table, if_exists)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _get_engine(self, engine, header):
        if engine == 'presto':
            return PrestoQueryEngine(self.apikey, self.endpoint, self.database, header)
        elif engine == 'hive':
            return HiveQueryEngine(self.apikey, self.endpoint, self.database, header)
        else:
            raise ValueError('`engine` should be "presto" or "hive"')
