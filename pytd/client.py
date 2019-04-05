import os
import logging

from .writer import SparkWriter
from .query_engine import PrestoQueryEngine, HiveQueryEngine

logger = logging.getLogger(__name__)


class Client(object):
    """Treasure Data client interface.

    A client instance establishes a connection to Presto or Hive query engine
    and Plazma primary storage. It allows us to easily and quickly read/write
    our data from/to Treasure Data.

    Parameters
    ----------
    apikey : string, optional
        Treasure Data API key. If not given, a value of environment variable
        `TD_API_KEY` is used by default.

    endpoint : string, optional
        Treasure Data API server. If not given, https://api.treasuredata.com is
        used by default. List of available endpoints is:
        https://support.treasuredata.com/hc/en-us/articles/360001474288-Sites-and-Endpoints

    database : string, default: 'sample_datasets'
        Name of connected database.

    engine : string, {'presto', 'hive'}, default: 'presto'
        Query engine.

    header : string or boolean, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.
    """

    def __init__(self, apikey=None, endpoint=None, database='sample_datasets', engine='presto', header=True, **kwargs):
        # `kwargs` is only for pandas-td compatibility

        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        if endpoint is None:
            endpoint = 'https://api.treasuredata.com' if ('TD_API_SERVER' not in os.environ) else os.environ['TD_API_SERVER']

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.engine = self._get_engine(engine, header)

        self.writer = None

    def close(self):
        """Close a client I/O session to Treasure Data.
        """
        self.engine.close()
        if self.writer is not None:
            self.writer.close()

    def query(self, sql):
        """Run query and get results.

        Parameters
        ----------
        sql : string
            Query issued on a specified query engine.

        Returns
        -------
        dict : keys ('data', 'columns')
            'data'
                List of rows. Every single row is represented as a list of
                column values.
            'columns'
                List of column names.
        """
        header = self.engine.create_header('Client#query')
        return self.engine.execute(header + sql)

    def load_table_from_dataframe(self, df, table, if_exists='error'):
        """Write a given DataFrame to a Treasure Data table.

        This function initializes a Writer interface at the first time. As a
        part of the initialization process, the latest version of td-spark will
        be downloaded.

        Parameters
        ----------
        df : pandas.DataFrame
            Data loaded to a target table.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
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
