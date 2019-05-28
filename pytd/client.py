import os

import tdclient

from .query_engine import HiveQueryEngine, PrestoQueryEngine, QueryEngine
from .table import Table


class Client(object):
    """Treasure Data client interface.

    A client instance establishes a connection to Presto or Hive query engine
    and Plazma primary storage. It allows us to easily and quickly read/write
    our data from/to Treasure Data.

    Parameters
    ----------
    apikey : string, optional
        Treasure Data API key. If not given, a value of environment variable
        ``TD_API_KEY`` is used by default.

    endpoint : string, optional
        Treasure Data API server. If not given, https://api.treasuredata.com is
        used by default. List of available endpoints is:
        https://support.treasuredata.com/hc/en-us/articles/360001474288-Sites-and-Endpoints

    database : string, default: 'sample_datasets'
        Name of connected database.

    default_engine : string, {'presto', 'hive'}, or pytd.query_engine.QueryEngine, \
                default: 'presto'
        Query engine. If a QueryEngine instance is given, ``apikey``,
        ``endpoint``, and ``database`` are overwritten by the values configured
        in the instance.

    header : string or boolean, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.
    """

    def __init__(
        self,
        apikey=None,
        endpoint=None,
        database="sample_datasets",
        default_engine="presto",
        header=True,
        **kwargs
    ):
        if isinstance(default_engine, QueryEngine):
            apikey = default_engine.apikey
            endpoint = default_engine.endpoint
            database = default_engine.database
        else:
            apikey = apikey or os.environ.get("TD_API_KEY")
            if apikey is None:
                raise ValueError(
                    "either argument 'apikey' or environment variable"
                    "'TD_API_KEY' should be set"
                )
            endpoint = endpoint or os.getenv(
                "TD_API_SERVER", "https://api.treasuredata.com"
            )
            default_engine = self._fetch_query_engine(
                default_engine, apikey, endpoint, database, header
            )

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.default_engine = default_engine

        self.api_client = tdclient.Client(
            apikey=apikey,
            endpoint=endpoint,
            user_agent=default_engine.user_agent,
            **kwargs
        )

    def list_databases(self):
        """Get a list of td-client-python Database objects.

        Returns
        -------
        list of tdclient.models.Database
        """
        return self.api_client.databases()

    def list_tables(self, database=None):
        """Get a list of td-client-python Table objects.

        Parameters
        ----------
        database : string, optional
            Database name. If not give, list tables in a table associated with
            this pytd.Client instance.

        Returns
        -------
        list of tdclient.models.Table
        """
        if database is None:
            database = self.database
        return self.api_client.tables(database)

    def list_jobs(self):
        """Get a list of td-client-python Job objects.

        Returns
        -------
        list of tdclient.models.Job
        """
        return self.api_client.jobs()

    def get_job(self, job_id):
        """Get a td-client-python Job object from ``job_id``.

        Parameters
        ----------
        job_id : integer
            Job ID.

        Returns
        -------
        tdclient.models.Job
        """
        return self.api_client.job(job_id)

    def close(self):
        """Close a client I/O session to Treasure Data.
        """
        self.default_engine.close()
        self.api_client.close()

    def query(self, query, engine=None):
        """Run query and get results.

        Parameters
        ----------
        query : string
            Query issued on a specified query engine.

        engine : string, {'presto', 'hive'}, or pytd.query_engine.QueryEngine, \
                optional
            Query engine. If not given, default query engine created in the
            constructor will be used.

        Returns
        -------
        dict : keys ('data', 'columns')
            'data'
                List of rows. Every single row is represented as a list of
                column values.
            'columns'
                List of column names.
        """
        if isinstance(engine, QueryEngine):
            pass  # use the given QueryEngine instance
        elif isinstance(engine, str):
            if (
                engine == "presto"
                and isinstance(self.default_engine, PrestoQueryEngine)
            ) or (
                engine == "hive" and isinstance(self.default_engine, HiveQueryEngine)
            ):
                engine = self.default_engine
            else:
                engine = self._fetch_query_engine(
                    engine,
                    self.apikey,
                    self.endpoint,
                    self.database,
                    self.default_engine.header,
                )
        else:
            engine = self.default_engine
        header = engine.create_header("Client#query")
        return engine.execute(header + query)

    def get_table(self, database, table):
        """Create a table control instance.

        Parameters
        ----------
        database : string
            Database name.

        table : string
            Table name.

        Returns
        -------
        pytd.table.Table
        """
        return Table(self, database, table)

    def load_table_from_dataframe(
        self, dataframe, destination, writer="bulk_import", if_exists="error"
    ):
        """Write a given DataFrame to a Treasure Data table.

        This function initializes a Writer interface at the first time. As a
        part of the initialization process for SparkWriter, the latest version
        of td-spark will be downloaded.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        destination : string, or pytd.table.Table
            Target table.

        writer : string, {'bulk_import', 'insert_into', 'spark'}, or \
                    pytd.writer.Writer, default: 'bulk_import'
            A Writer to choose writing method to Treasure Data. If not given,
            default Writer will be created with executing
            :func:`~pytd.Client.load_table_from_dataframe` at the first time.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists. 'append' is not
            supported in `bulk_import`.
        """
        from_string = isinstance(destination, str)

        if from_string:
            if "." in destination:
                database, table = destination.split(".")
            else:
                database, table = self.database, destination
            destination = self.get_table(database, table)

        destination.import_dataframe(dataframe, writer, if_exists)

        if from_string:
            destination.close()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def _fetch_query_engine(self, engine, apikey, endpoint, database, header):
        if engine == "presto":
            return PrestoQueryEngine(apikey, endpoint, database, header)
        elif engine == "hive":
            return HiveQueryEngine(apikey, endpoint, database, header)
        else:
            raise ValueError(
                '`engine` should be "presto" or "hive", or actual QueryEngine instance'
            )
