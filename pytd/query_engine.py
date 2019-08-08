import abc
import logging
import os
from urllib.parse import urlparse

import prestodb
import tdclient

from .version import __version__

logger = logging.getLogger(__name__)


class QueryEngine(metaclass=abc.ABCMeta):
    """An interface to Treasure Data query engine.

    Parameters
    ----------
    apikey : string
        Treasure Data API key.

    endpoint : string
        Treasure Data API server.

    database : string
        Name of connected database.

    header : string or boolean
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database
        self.header = header

    @property
    def user_agent(self):
        """User agent passed to a query engine connection.
        """
        return "pytd/{0}".format(__version__)

    def execute(self, query, **kwargs):
        """Execute a given SQL statement and return results.

        Parameters
        ----------
        query : string
            Query.

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
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
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
        string
        """
        if self.header is False:
            return ""

        if isinstance(self.header, str):
            header = "-- {0}\n".format(self.header)
        else:
            header = "-- client: {0}\n".format(self.user_agent)

        if isinstance(extra_lines, str):
            header += "-- {0}\n".format(extra_lines)
        elif isinstance(extra_lines, (list, tuple)):
            header += "".join(["-- {0}\n".format(line) for line in extra_lines])

        return header

    @abc.abstractmethod
    def cursor(self, **kwargs):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def _connect(self):
        pass


class PrestoQueryEngine(QueryEngine):
    """An interface to Treasure Data Presto query engine.

    Parameters
    ----------
    apikey : string
        Treasure Data API key.

    endpoint : string
        Treasure Data API server (e.g.,
        'https://api.treasuredata.com/') or
        Presto API host (e.g.,
        'api-presto.treasuredata.com'). If the
        latter is given, connect directly to the
        Presto engine and disable Treasure
        Data-specific query parameters like
        ``priority``.

    database : string
        Name of connected database.

    header : string or boolean
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        super(PrestoQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    @property
    def user_agent(self):
        """User agent passed to a Presto connection.
        """
        return "pytd/{0} (prestodb/{1})".format(__version__, prestodb.__version__)

    def cursor(self, **kwargs):
        """Get cursor defined by DB-API.

        Returns
        -------
        prestodb.dbapi.Cursor, or tdclient.cursor.Cursor
        """
        if isinstance(self.engine, prestodb.dbapi.Connection):
            return self.engine.cursor()

        logger.warning(
            "returning `tdclient.cursor.Cursor`. This cursor, `Cursor#fetchone` "
            "in particular, might behave different from your expectation, "
            "because it actually executes a job on Treasure Data and fetches all "
            "records at once from the job result."
        )

        original_cursor_kwargs = self.engine._cursor_kwargs.copy()

        params = self.engine._cursor_kwargs
        if "type" in kwargs:
            params["type"] = kwargs["type"]
        if "db" in kwargs:
            params["db"] = kwargs["db"]
        if "result_url" in kwargs:
            params["result_url"] = kwargs["result_url"]
        if "priority" in kwargs:
            params["priority"] = kwargs["priority"]
        if "retry_limit" in kwargs:
            params["retry_limit"] = kwargs["retry_limit"]
        if "wait_interval" in kwargs:
            params["wait_interval"] = kwargs["wait_interval"]
        if "wait_callback" in kwargs:
            params["wait_callback"] = kwargs["wait_callback"]
        cursor = self.engine.cursor()

        self.engine._cursor_kwargs = original_cursor_kwargs
        return cursor

    def close(self):
        """Close a connection to Presto.
        """
        self.engine.close()

    def _connect(self):
        if "api-presto" in self.endpoint:
            return prestodb.dbapi.connect(
                host=self.endpoint,
                port=443,
                http_scheme="https",
                user=self.apikey,
                catalog="td-presto",
                schema=self.database,
                http_headers={"user-agent": self.user_agent},
            )

        return tdclient.connect(
            apikey=self.apikey,
            endpoint=self.endpoint,
            db=self.database,
            user_agent=self.user_agent,
            type="presto",
        )

    @staticmethod
    def get_api_host(endpoint="https://api.treasuredata.com/"):
        """Presto API host obtained from ``TD_PRESTO_API`` env variable or
        inferred from Treasure Data REST API endpoint.

        Parameters
        ----------
        endpoint : string, default: 'https://api.treasuredata.com/'
            Treasure Data API server.

        Returns
        -------
        Presto API host.
        """
        return os.getenv(
            "TD_PRESTO_API", urlparse(endpoint).netloc.replace("api", "api-presto")
        )


class HiveQueryEngine(QueryEngine):
    """An interface to Treasure Data Hive query engine.

    Parameters
    ----------
    apikey : string
        Treasure Data API key.

    endpoint : string
        Treasure Data API server.

    database : string
        Name of connected database.

    header : string or boolean
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        super(HiveQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    @property
    def user_agent(self):
        """User agent passed to a Hive connection.
        """
        return "pytd/{0} (tdclient/{1})".format(__version__, tdclient.__version__)

    def cursor(self, **kwargs):
        """Get cursor defined by DB-API.

        Returns
        -------
        tdclient.cursor.Cursor
        """
        logger.warning(
            "returning `tdclient.cursor.Cursor`. This cursor, `Cursor#fetchone` "
            "in particular, might behave different from your expectation, "
            "because it actually executes a job on Treasure Data and fetches all "
            "records at once from the job result."
        )
        original_cursor_kwargs = self.engine._cursor_kwargs.copy()

        params = self.engine._cursor_kwargs
        if "type" in kwargs:
            params["type"] = kwargs["type"]
        if "db" in kwargs:
            params["db"] = kwargs["db"]
        if "result_url" in kwargs:
            params["result_url"] = kwargs["result_url"]
        if "priority" in kwargs:
            params["priority"] = kwargs["priority"]
        if "retry_limit" in kwargs:
            params["retry_limit"] = kwargs["retry_limit"]
        if "wait_interval" in kwargs:
            params["wait_interval"] = kwargs["wait_interval"]
        if "wait_callback" in kwargs:
            params["wait_callback"] = kwargs["wait_callback"]
        cursor = self.engine.cursor()

        self.engine._cursor_kwargs = original_cursor_kwargs
        return cursor

    def close(self):
        """Close a connection to Hive.
        """
        self.engine.close()

    def _connect(self):
        return tdclient.connect(
            apikey=self.apikey,
            endpoint=self.endpoint,
            db=self.database,
            user_agent=self.user_agent,
            type="hive",
        )
