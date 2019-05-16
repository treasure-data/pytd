import abc
import re
import prestodb
import tdclient

from .version import __version__

import logging
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

    def execute(self, query):
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
        cur = self.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return {'data': rows, 'columns': columns}

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
            return ''

        if isinstance(self.header, str):
            header = "-- {0}\n".format(self.header)
        else:
            header = "-- client: {0}\n".format(self.user_agent)

        if isinstance(extra_lines, str):
            header += "-- {0}\n".format(extra_lines)
        elif isinstance(extra_lines, (list, tuple)):
            header += ''.join(["-- {0}\n".format(line) for line in extra_lines])

        return header

    @abc.abstractmethod
    def cursor(self):
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
        Treasure Data API server.

    database : string
        Name of connected database.

    header : string or boolean
        Prepend comment strings, in the form "-- comment", as a header of queries.
    """

    def __init__(self, apikey, endpoint, database, header):
        super(PrestoQueryEngine, self).__init__(apikey, endpoint, database, header)
        api_presto = re.compile(r'(?:https?://)?(api(?:-.+?)?)\.')
        self.presto_endpoint = api_presto.sub('\\1-presto.', self.endpoint).strip('/')
        self.engine = self._connect()

    @property
    def user_agent(self):
        """User agent passed to a Presto connection.
        """
        return "pytd/{0} (prestodb/{1})".format(__version__, prestodb.__version__)

    def cursor(self):
        """Get cursor defined by DB-API.

        Returns
        -------
        prestodb.dbapi.Cursor
        """
        return self.engine.cursor()

    def close(self):
        """Close a connection to Presto.
        """
        self.engine.close()

    def _connect(self):
        return prestodb.dbapi.connect(
            host=self.presto_endpoint,
            port=443,
            http_scheme='https',
            user=self.apikey,
            catalog='td-presto',
            schema=self.database,
            http_headers={'user-agent': self.user_agent}
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

    def cursor(self):
        """Get cursor defined by DB-API.

        Returns
        -------
        tdclient.cursor.Cursor
        """
        return self.engine.cursor()

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
            type='hive'
        )
