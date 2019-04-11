import os
import tdclient

from .writer import SparkWriter
from .query_engine import QueryEngine, PrestoQueryEngine, HiveQueryEngine


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

    engine : string, {'presto', 'hive'}, or pytd.query_engine.QueryEngine, \
                default: 'presto'
        Query engine.

    header : string or boolean, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.
    """

    def __init__(self, apikey=None, endpoint=None, database='sample_datasets', engine='presto', header=True, **kwargs):
        # `kwargs` is only for pandas-td compatibility

        if isinstance(engine, QueryEngine):
            apikey = engine.apikey
            endpoint = engine.endpoint
            database = engine.database
        else:
            apikey = os.environ.get('TD_API_KEY')
            if apikey is None:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")

            endpoint = os.getenv('TD_API_SERVER', 'https://api.treasuredata.com')

            engine = self._fetch_engine(engine, apikey, endpoint, database, header)

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.engine = engine

        self.api_client = tdclient.Client(apikey=apikey, endpoint=endpoint, user_agent=engine.user_agent)

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

    def load_table_from_dataframe(self, dataframe, table, if_exists='error'):
        """Write a given DataFrame to a Treasure Data table.

        This function initializes a Writer interface at the first time. As a
        part of the initialization process, the latest version of td-spark will
        be downloaded.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
        if self.writer is None:
            self.writer = SparkWriter(self.apikey, self.endpoint)

        self.writer.write_dataframe(dataframe, self.database, table, if_exists)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _fetch_engine(self, engine, apikey, endpoint, database, header):
        if engine == 'presto':
            return PrestoQueryEngine(apikey, endpoint, database, header)
        elif engine == 'hive':
            return HiveQueryEngine(apikey, endpoint, database, header)
        else:
            raise ValueError('`engine` should be "presto" or "hive", or actual QueryEngine instance')
