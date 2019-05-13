import os

import tdclient

from .query_engine import HiveQueryEngine, PrestoQueryEngine, QueryEngine
from .writer import BulkImportWriter, InsertIntoWriter, SparkWriter


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

    engine : string, {'presto', 'hive'}, or pytd.query_engine.QueryEngine, \
                default: 'presto'
        Query engine. If a QueryEngine instance is given, ``apikey``,
        ``endpoint``, and ``database`` are overwritten by the values configured
        in the instance.

    writer : string, {'bulk_import', 'insert_into', 'spark'}, or pytd.writer.Writer, \
                default: 'bulk_import'
        A Writer to choose writing method to Treasure Data. If not given, default Writer
        will be created with executing :func:`~pytd.Client.load_table_from_dataframe`
        at the first time.

    header : string or boolean, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.
    """

    def __init__(
        self,
        apikey=None,
        endpoint=None,
        database="sample_datasets",
        engine="presto",
        writer="bulk_import",
        header=True,
        **kwargs
    ):
        if isinstance(engine, QueryEngine):
            apikey = engine.apikey
            endpoint = engine.endpoint
            database = engine.database
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
            engine = self._fetch_query_engine(
                engine, apikey, endpoint, database, header
            )

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.engine = engine

        self.api_client = tdclient.Client(
            apikey=apikey, endpoint=endpoint, user_agent=engine.user_agent, **kwargs
        )

        self.initialized_writer = False
        self.writer = writer

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
            return self.api_client.tables(self.database)
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
        self.engine.close()
        self.api_client.close()

        # If Writer is initialized outside of Client (i.e., Writer instance is
        # directly passed to Client), Client should not close the instance.
        if self.initialized_writer:
            self.writer.close()

    def query(self, query):
        """Run query and get results.

        Parameters
        ----------
        query : string
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
        header = self.engine.create_header("Client#query")
        return self.engine.execute(header + query)

    def load_table_from_dataframe(self, dataframe, table, if_exists="error"):
        """Write a given DataFrame to a Treasure Data table.

        This function initializes a Writer interface at the first time. As a
        part of the initialization process for SparkWriter, the latest version
        of td-spark will be downloaded.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists. 'append' is not
            supported in BulkImportWriter.
        """
        if not self.initialized_writer and isinstance(self.writer, str):
            cls = self.writer.lower()
            if cls == "bulk_import":
                self.writer = BulkImportWriter(self.api_client)
            elif cls == "insert_into":
                presto = (
                    self.engine
                    if isinstance(self.engine, PrestoQueryEngine)
                    else self._fetch_query_engine(
                        "presto", self.apikey, self.endpoint, self.database, True
                    )
                )
                self.writer = InsertIntoWriter(self.api_client, presto)
            elif cls == "spark":
                self.writer = SparkWriter(self.apikey, self.endpoint)
            else:
                raise ValueError("unknown way to upload data to TD is specified")
            self.initialized_writer = True

        self.writer.write_dataframe(dataframe, self.database, table, if_exists)

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
