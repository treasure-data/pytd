import logging
import os
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd
import tdclient
from tdclient.models import Database as TDClientDatabase
from tdclient.models import Job as TDClientJob
from tdclient.models import Table as TDClientTable

from .query_engine import HiveQueryEngine, PrestoQueryEngine, QueryEngine, QueryResult
from .table import Table

if TYPE_CHECKING:
    import trino.client

    from .writer import Writer

logger = logging.getLogger(__name__)


class Client:
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

    query_executed : str or :class:`trino.client.TrinoResult`, default: `None`
        Query execution result returned from DB-API Cursor object.

        Examples
        ---------

        Presto query executed via ``trino`` returns ``TrinoResult`` object:

        >>> import pytd
        >>> client = pytd.Client()
        >>> client.query_executed
        >>> client.query('select 1')
        >>> client.query_executed
        <trino.client.TrinoResult object at 0x10b9826a0>

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
        apikey: str | None = None,
        endpoint: str | None = None,
        database: str = "sample_datasets",
        default_engine: Literal["presto", "hive"] | QueryEngine = "presto",
        header: str | bool = True,
        **kwargs: Any,
    ) -> None:
        if isinstance(default_engine, QueryEngine):
            apikey = default_engine.apikey
            endpoint = default_engine.endpoint
            database = default_engine.database
        else:
            apikey = apikey or os.environ.get("TD_API_KEY")
            if apikey is None:
                raise ValueError(
                    "either argument 'apikey' or environment variable"
                    " 'TD_API_KEY' should be set"
                )
            if endpoint is None:
                endpoint = os.getenv("TD_API_SERVER", "https://api.treasuredata.com")
            default_engine = self._fetch_query_engine(
                default_engine, apikey, endpoint, database, header
            )

        self.apikey: str = apikey
        self.endpoint: str = endpoint
        self.database: str = database

        self.default_engine: QueryEngine = default_engine
        self.query_executed: str | trino.client.TrinoResult | None = None

        self.api_client: tdclient.Client = tdclient.Client(
            apikey=apikey,
            endpoint=endpoint,
            user_agent=default_engine.user_agent,
            **kwargs,
        )

    def list_databases(self) -> list[TDClientDatabase]:
        """Get a list of td-client-python Database objects.

        Returns
        -------
        list of :class:`tdclient.models.Database`
        """
        return self.api_client.databases()

    def list_tables(self, database: str | None = None) -> list[TDClientTable]:
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

    def list_jobs(self) -> list[TDClientJob]:
        """Get a list of td-client-python Job objects.

        Returns
        -------
        list of :class:`tdclient.models.Job`
        """
        return self.api_client.jobs()

    def get_job(self, job_id: int) -> TDClientJob:
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

    def close(self) -> None:
        """Close a client I/O session to Treasure Data."""
        self.default_engine.close()
        self.api_client.close()

    def query(
        self,
        query: str,
        engine: Literal["presto", "hive"] | QueryEngine | None = None,
        **kwargs: Any,
    ) -> QueryResult:
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
            engine provided by ``trino``.

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
              via ``tdclient`` rather than its default ``trino`` interface.

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

    def get_table(self, database: str, table: str) -> Table:
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

    def exists(self, database: str, table: str | None = None) -> bool:
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

    def create_database_if_not_exists(self, database: str) -> None:
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
        self,
        dataframe: pd.DataFrame,
        destination: str | Table,
        writer: (
            Literal["bulk_import", "insert_into", "spark"] | "Writer"
        ) = "bulk_import",
        if_exists: Literal["error", "overwrite", "append", "ignore"] = "error",
        **kwargs: Any,
    ) -> None:
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

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _fetch_query_engine(
        self,
        engine: Literal["presto", "hive"],
        apikey: str,
        endpoint: str,
        database: str,
        header: str | bool,
    ) -> QueryEngine:
        if engine == "presto":
            return PrestoQueryEngine(apikey, endpoint, database, header)
        elif engine == "hive":
            return HiveQueryEngine(apikey, endpoint, database, header)
        else:
            raise ValueError(
                '`engine` should be "presto" or "hive", or actual QueryEngine instance'
            )
