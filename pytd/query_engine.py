import abc
import re
import six
import prestodb
import tdclient

from pytd.version import __version__


class QueryEngine(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, apikey, endpoint, database, header):
        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database
        self.header = header

    def execute(self, sql):
        cur = self.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return {'data': rows, 'columns': columns}

    def create_header(self, extra_lines=None):
        if self.header is False:
            header = ''
        elif isinstance(self.header, six.string_types):
            header = "-- {0}\n".format(self.header)
        else:
            header = "-- client: pytd/{0}\n".format(__version__)

        if isinstance(extra_lines, six.string_types):
            header += "-- {0}\n".format(extra_lines)
        elif isinstance(extra_lines, (list, tuple)):
            header += ''.join(["-- {0}\n".format(row) for row in extra_lines])

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

    def __init__(self, apikey, endpoint, database, header):
        super(PrestoQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self):
        http = re.compile(r'https?://')
        user_agent = 'pytd/%s (Presto; prestodb/%s)' % (__version__, prestodb.__version__)
        return prestodb.dbapi.connect(
            host=http.sub('', self.endpoint).strip('/'),
            port=443,
            http_scheme='https',
            user=self.apikey,
            catalog='td-presto',
            schema=self.database,
            http_headers={'user-agent': user_agent}
        )


class HiveQueryEngine(QueryEngine):

    def __init__(self, apikey, endpoint, database, header):
        super(HiveQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self):
        user_agent = 'pytd/%s (Hive; tdclient/%s)' % (__version__, tdclient.__version__)
        return tdclient.connect(
            apikey=self.apikey,
            endpoint=self.endpoint,
            db=self.database,
            user_agent=user_agent,
            type='hive'
        )
