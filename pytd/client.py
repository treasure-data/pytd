import os
import prestodb
import logging

import pytd
from pytd.writer import SparkWriter

logger = logging.getLogger(__name__)


class Client(object):

    def __init__(self, apikey=None, database='sample_datasets'):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        self.td_presto = self._connect_td_presto(apikey, database)

        self.apikey = apikey
        self.database = database

        self.writer = None

    def close(self):
        self.td_presto.close()
        if self.writer is not None:
            self.writer.close()

    def query(self, sql):
        cur = self.get_cursor()
        sql = "-- pytd/{0}\n-- Client#query\n".format(pytd.__version__) + sql
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

    def get_cursor(self):
        return self.td_presto.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _connect_td_presto(self, apikey, database):
        return prestodb.dbapi.connect(
            host='api-presto.treasuredata.com',
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database
        )
