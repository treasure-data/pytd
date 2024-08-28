import abc
import importlib.metadata
import logging
import os
from urllib.parse import urlparse

import prestodb
import tdclient

__version__ = importlib.metadata.version("pytd")

logger = logging.getLogger(__name__)


class QueryEngine(metaclass=abc.ABCMeta):
    """An interface to Treasure Data query engine.

    Parameters
    ----------
    apikey : str
        Treasure Data API key.

    endpoint : str
        Treasure Data API server.

    database : str
        Name of connected database.

    header : str or bool
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        if len(urlparse(endpoint).scheme) == 0:
            endpoint = f"https://{endpoint}"
        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database
        self.header = header
        self.executed = None

    @property
    def user_agent(self):
        """User agent passed to a query engine connection."""
        return f"pytd/{__version__}"

    def execute(self, query, **kwargs):
        """Execute a given SQL statement and return results.

        Executed result returned by Cursor object is stored in
        ``self.executed``.

        Parameters
        ----------
        query : str
            Query.

        **kwargs
            Treasure Data-specific optional query parameters. Giving these
            keyword arguments forces query engine to issue a query via Treasure
            Data REST API provided by ``tdclient``, rather than using a direct
            connection established by the ``prestodb`` package.

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
              is set to ``"stable"`` in ``HiveQueryEngine``.
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
        cur = self.cursor(**kwargs)
        self.executed = cur.execute(query)
        rows = cur.fetchall()
        # cur.description is None for CREATE and DROP statements in recent version of
        # Trino
        columns = [desc[0] for desc in cur.description] if cur.description else None
        return {"data": rows, "columns": columns}

    def create_header(self, extra_lines=[]):
        """Build header comments.

        Parameters
        ----------
        extra_lines : string or array-like, default: []
            Comments appended to the default one, which corresponds to a user
            agent string. If ``self.header=None``, empty string is returned
            regardless of this argument.

        Returns
        -------
        str
        """
        if self.header is False:
            return ""

        if isinstance(self.header, str):
            header = f"-- {self.header}\n"
        else:
            header = f"-- client: {self.user_agent}\n"

        if isinstance(extra_lines, str):
            header += f"-- {extra_lines}\n"
        elif isinstance(extra_lines, (list, tuple)):
            header += "".join([f"-- {line}\n" for line in extra_lines])

        return header

    @abc.abstractmethod
    def cursor(self, force_tdclient=False, **kwargs):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def _connect(self):
        pass

    def _get_tdclient_cursor(self, con, **kwargs):
        """Get DB-API cursor from tdclient Connection instance.

        ``kwargs`` are for setting specific parameters to Treasure Data REST
        API requests. For that purpose, this method runs workaround to
        dynamically configure custom parameters for ``tdclient.cursor.Cursor``
        such as "priority"; because the only way to set the custom parameters
        is using an instance attribute
        ``tdclient.connection.Connection._cursor_kwargs``, this method
        temporarily overwrites the attribute before calling
        ``Connection#cursor``.

        See implementation of the Connection interface:
        https://github.com/treasure-data/td-client-python/blob/78e1e187c3e15d009fa2ce697dc938fc0ab02ada/tdclient/connection.py

        Parameters
        ----------
        con : :class:`tdclient.connection.Connection`
            Handler created by ``tdclient#connect``.

        **kwargs
            Treasure Data-specific optional query parameters. Giving these
            keyword arguments forces query engine to issue a query via Treasure
            Data REST API provided by ``tdclient``, rather than using a direct
            connection established by the ``prestodb`` package.

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
              is set to ``"stable"`` in ``HiveQueryEngine``.
              https://docs.treasuredata.com/display/public/PD/Writing+Hive+Queries

        Returns
        -------
        :class:`tdclient.cursor.Cursor`
        """
        api_param_names = set(
            [
                "db",
                "result_url",
                "priority",
                "retry_limit",
                "wait_interval",
                "wait_callback",
                "engine_version",
            ]
        )

        if "type" in kwargs:
            raise RuntimeError(
                "optional query parameter 'type' is unsupported. Issue query "
                "from a proper QueryEngine instance: "
                "{PrestoQueryEngine, HiveQueryEngine}."
            )

        # update a clone of the original params
        cursor_kwargs = con._cursor_kwargs.copy()
        for k, v in kwargs.items():
            if k not in api_param_names:
                raise RuntimeError(
                    "unknown parameter for Treasure Data query execution API; "
                    f"'{k}' is not in [{', '.join(api_param_names)}]."
                )
            cursor_kwargs[k] = v

        # keep the original `_cursor_kwargs`
        original_cursor_kwargs = con._cursor_kwargs.copy()

        # overwrite the original params
        con._cursor_kwargs = cursor_kwargs

        # `Connection#cursor` internally refers the customized
        # ``_cursor_kwargs``
        cursor = con.cursor()

        # write the original params back to `_cursor_kwargs`
        con._cursor_kwargs = original_cursor_kwargs

        logger.warning(
            "returning `tdclient.cursor.Cursor`. This cursor, `Cursor#fetchone` "
            "in particular, might behave different from your expectation, "
            "because it actually executes a job on Treasure Data and fetches all "
            "records at once from the job result."
        )
        return cursor


class PrestoQueryEngine(QueryEngine):
    """An interface to Treasure Data Presto query engine.

    Parameters
    ----------
    apikey : str
        Treasure Data API key.

    endpoint : str
        Treasure Data API server.

    database : str
        Name of connected database.

    header : str or bool
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        super(PrestoQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.prestodb_connection, self.tdclient_connection = self._connect()

    @property
    def user_agent(self):
        """User agent passed to a Presto connection."""
        return (
            f"pytd/{__version__} "
            f"(prestodb/{prestodb.__version__}; "
            f"tdclient/{tdclient.__version__})"
        )

    @property
    def presto_api_host(self):
        """Presto API host obtained from ``TD_PRESTO_API`` env variable or
        inferred from Treasure Data REST API endpoint.
        """
        return os.getenv(
            "TD_PRESTO_API", urlparse(self.endpoint).netloc.replace("api", "api-presto")
        )

    def cursor(self, force_tdclient=False, **kwargs):
        """Get cursor defined by DB-API.

        Parameters
        ----------
        force_tdclient : bool
            Specify whether the method always returns
            :class:`tdclient.cursor.Cursor`.  If ``False``, returned cursor
            type is dynamically selected depending on ``**kwargs``.

        **kwargs
            Treasure Data-specific optional query parameters. Giving these
            keyword arguments forces query engine to issue a query via Treasure
            Data REST API provided by ``tdclient``, rather than using a direct
            connection established by the ``prestodb`` package.

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

        Returns
        -------
        :class:`prestodb.dbapi.Cursor`, or :class:`tdclient.cursor.Cursor`
        """
        if not force_tdclient and len(kwargs) == 0:
            return self.prestodb_connection.cursor()

        return self._get_tdclient_cursor(self.tdclient_connection, **kwargs)

    def close(self):
        """Close a connection to Presto."""
        self.prestodb_connection.close()
        self.tdclient_connection.close()

    def _connect(self):
        return (
            prestodb.dbapi.connect(
                host=self.presto_api_host,
                port=443,
                http_scheme="https",
                user=self.apikey,
                catalog="td-presto",
                schema=self.database,
                http_headers={"user-agent": self.user_agent},
            ),
            tdclient.connect(
                apikey=self.apikey,
                endpoint=self.endpoint,
                db=self.database,
                user_agent=self.user_agent,
                type="presto",
            ),
        )


class HiveQueryEngine(QueryEngine):
    """An interface to Treasure Data Hive query engine.

    Parameters
    ----------
    apikey : str
        Treasure Data API key.

    endpoint : str
        Treasure Data API server.

    database : str
        Name of connected database.

    header : str or bool
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        super(HiveQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    @property
    def user_agent(self):
        """User agent passed to a Hive connection."""
        return f"pytd/{__version__} (tdclient/{tdclient.__version__})"

    def cursor(self, force_tdclient=True, **kwargs):
        """Get cursor defined by DB-API.

        Parameters
        ----------
        force_tdclient : bool
            Specify whether the method always returns
            :class:`tdclient.cursor.Cursor`.  Currently, the parameter changes
            nothing in ``HiveQueryEngine`` since
            :class:`tdclient.cursor.Curosr` is the only option as a type of
            returned value.

        **kwargs
            Treasure Data-specific optional query parameters. Giving these
            keyword arguments forces query engine to issue a query via Treasure
            Data REST API provided by ``tdclient``.

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
              is set to ``"stable"``.
              https://docs.treasuredata.com/display/public/PD/Writing+Hive+Queries

        Returns
        -------
        :class:`tdclient.cursor.Cursor`
        """
        return self._get_tdclient_cursor(self.engine, **kwargs)

    def close(self):
        """Close a connection to Hive."""
        self.engine.close()

    def _connect(self):
        return tdclient.connect(
            apikey=self.apikey,
            endpoint=self.endpoint,
            db=self.database,
            user_agent=self.user_agent,
            type="hive",
        )
