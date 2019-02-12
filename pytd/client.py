import os
import logging

import pytd
from pytd.writer import SparkWriter
from pytd.query_engine import PrestoQueryEngine, HiveQueryEngine

logger = logging.getLogger(__name__)


class Client(object):

    def __init__(self, apikey=None, database='sample_datasets', default_engine='presto'):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        self._connect_query_engines(apikey, database)

        self.apikey = apikey
        self.database = database
        self.default_engine = default_engine

        self.writer = None

    def close(self):
        self.presto.close()
        self.hive.close()
        if self.writer is not None:
            self.writer.close()

    def query(self, sql, engine=None):
        if engine is None:
            engine = self.default_engine
        cur = self.get_cursor(engine)
        sql = "-- pytd/{0}\n-- Client#query(engine={1})\n".format(pytd.__version__, engine) + sql
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return {'data': rows, 'columns': columns}

    def load_table_from_dataframe(self, df, table, if_exists='error'):
        if self.writer is None:
            self.writer = SparkWriter(self.apikey)

        destination = table
        if '.' not in table:
            destination = self.database + '.' + table

        self.writer.write_dataframe(df, destination, if_exists)

    def get_cursor(self, engine=None):
        if engine is None:
            engine = self.default_engine

        if engine == 'presto':
            return self.presto.cursor()
        elif engine == 'hive':
            return self.hive.cursor()
        else:
            raise ValueError('`engine` should be "presto" or "hive"')

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _connect_query_engines(self, apikey, database):
        self.presto = PrestoQueryEngine(apikey, database)
        self.hive = HiveQueryEngine(apikey, database)
