import logging
import os

import tdclient

from .query_engine import HiveQueryEngine, PrestoQueryEngine, QueryEngine
from .table import Table

logger = logging.getLogger(__name__)


class Client(object):
    """Treasure Data client interface.

    A client instance establishes a connection to Treasure Data. This interface
    gives easy and efficient access to Presto/Hive query engine and Plazma
    primary storage.

    Parameters
    ----------
    apikey : str, optional
        Treasure Data API key. If not given, a value of environment variable
        ``TD_API_KEY`` is used by default.

    endpoint : str, optional
        Treasure Data API server. If not given, ``https://api.treasuredata.com`` is
        used by default. List of available endpoints is:
        https://docs.treasuredata.com/display/public/PD/Sites+and+Endpoints

    database : str, default: 'sample_datasets'
        Name of connected database.

    default_engine : str, {'presto', 'hive'}, or \
                :class:`pytd.query_engine.QueryEngine`, default: 'presto'
        Query engine. If a QueryEngine instance is given, ``apikey``,
        ``endpoint``, and ``database`` are overwritten by the values configured
        in the instance.

    header : str or bool, default: `True`
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.

    Attributes
    ----------
    api_client : :class:`tdclient.Client`
        Connection to Treasure Data.

    query_executed : str or :class:`prestodb.client.PrestoResult`, default: `None`
        Query execution result returned from DB-API Cursor object.

        Examples
        ---------

        Presto query executed via ``prestodb`` returns ``PrestoResult`` object:

        >>> import pytd
        >>> client = pytd.Client()
        >>> client.query_executed
        >>> client.query('select 1')
        >>> client.query_executed
        <prestodb.client.PrestoResult object at 0x10b9826a0>

        Meanwhile, ``tdclient`` runs a job on Treasure Data, and Cursor returns
        its job id:

        >>> client.query('select 1', priority=0)
        >>> client.query_executed
        '669563342'

        Note that the optional argument ``priority`` forces the client to query
        via tdclient.
    """

    def __init__(
        self,
        apikey=None,
        endpoint=None,
        database="sample_datasets",
        default_engine="presto",
        header=True,
        **kwargs,
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
            if endpoint is None:
                endpoint = os.getenv("TD_API_SERVER", "https://api.treasuredata.com")
            default_engine = self._fetch_query_engine(
                default_engine, apikey, endpoint, database, header
            )

        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database

        self.default_engine = default_engine
        self.query_executed = None

        self.api_client = tdclient.Client(
            apikey=apikey,
            endpoint=endpoint,
            user_agent=default_engine.user_agent,
            **kwargs,
        )

    def list_databases(self):
        """Get a list of td-client-python Database objects.

        Returns
        -------
        list of :class:`tdclient.models.Database`
        """
        return self.api_client.databases()

    def list_tables(self, database=None):
        """Get a list of td-client-python Table objects.

        Parameters
        ----------
        database : string, optional
            Database name. If not give, list tables in a table associated with
            this :class:`pytd.Client` instance.

        Returns
        -------
        list of :class:`tdclient.models.Table`
        """
        if database is None:
            database = self.database
        return self.api_client.tables(database)

    def list_jobs(self):
        """Get a list of td-client-python Job objects.

        Returns
        -------
        list of :class:`tdclient.models.Job`
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
        :class:`tdclient.models.Job`
        """
        return self.api_client.job(job_id)

    def close(self):
        """Close a client I/O session to Treasure Data."""
        self.default_engine.close()
        self.api_client.close()

    def query(self, query, engine=None, **kwargs):
        """Run query and get results.

        Executed result stored in ``QueryEngine`` is retained in
        ``self.query_executed``.

        Parameters
        ----------
        query : str
            Query issued on a specified query engine.

        engine : str, {'presto', 'hive'}, or \
                :class:`pytd.query_engine.QueryEngine`, optional
            Query engine. If not given, default query engine created in the
            constructor will be used.

        **kwargs
            Treasure Data-specific optional query parameters. Giving these
            keyword arguments forces query engine to issue a query via Treasure
            Data REST API provided by ``tdclient``; that is, if ``engine`` is
            Presto, you cannot enjoy efficient direct access to the query
            engine provided by ``prestodb``.

            - ``db`` (str): use the database
            - ``result_url`` (str): result output URL
            - ``priority`` (int or str): priority

                - -2: "VERY LOW"
                - -1: "LOW"
                -  0: "NORMAL"
                -  1: "HIGH"
                -  2: "VERY HIGH"
            - ``retry_limit`` (int): max number of automatic retries
            - ``wait_interval`` (int): sleep interval until job finish
            - ``wait_callback`` (function): called every interval against job itself
            - ``engine_version`` (str): run query with Hive 2 if this parameter
              is set to ``"stable"`` and ``engine`` denotes Hive.
              https://docs.treasuredata.com/display/public/PD/Writing+Hive+Queries

            Meanwhile, when a following argument is set to ``True``, query is
            deterministically issued via ``tdclient``.

            - ``force_tdclient`` (bool): force Presto engines to issue a query
              via ``tdclient`` rather than its default ``prestodb`` interface.

        Returns
        -------
        dict : keys ('data', 'columns')
            'data'
                List of rows. Every single row is represented as a list of
                column values.
            'columns'
                List of column names.
        """
        self.query_executed = None
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
        res = engine.execute(header + query, **kwargs)
        self.query_executed = engine.executed
        return res

    def get_table(self, database, table):
        """Create a pytd table control instance.

        Parameters
        ----------
        database : str
            Database name.

        table : str
            Table name.

        Returns
        -------
        :class:`pytd.table.Table`
        """
        return Table(self, database, table)

    def exists(self, database, table=None):
        """Check if a database and table exists.

        Parameters
        ----------
        database : str
            Database name.

        table : str, optional
            Table name. If not given, just check the database existence.

        Returns
        -------
        bool
        """
        try:
            tbl = self.get_table(database, table)
        except ValueError:
            return False
        if table is None:
            return True
        return tbl.exists

    def create_database_if_not_exists(self, database):
        """Create a database on Treasure Data if it does not exist.

        Parameters
        ----------
        database : str
            Database name.
        """
        if self.exists(database):
            logger.info(f"database `{database}` already exists")
        else:
            self.api_client.create_database(database)
            logger.info(f"created database `{database}`")

    def load_table_from_dataframe(
        self, dataframe, destination, writer="bulk_import", if_exists="error", **kwargs
    ):
        """Write a given DataFrame to a Treasure Data table.

        This function may initialize a Writer instance. Note that, as a part of
        the initialization process for SparkWriter, the latest version of
        td-spark will be downloaded.

        Parameters
        ----------
        dataframe : :class:`pandas.DataFrame`
            Data loaded to a target table.

        destination : str, or :class:`pytd.table.Table`
            Target table.

        writer : str, {'bulk_import', 'insert_into', 'spark'}, or \
                    :class:`pytd.writer.Writer`, default: 'bulk_import'
            A Writer to choose writing method to Treasure Data. If not given or
            string value, a temporal Writer instance will be created.

        if_exists : str, {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        if isinstance(destination, str):
            if "." in destination:
                database, table = destination.split(".")
            else:
                database, table = self.database, destination
            destination = self.get_table(database, table)

        destination.import_dataframe(dataframe, writer, if_exists, **kwargs)

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
