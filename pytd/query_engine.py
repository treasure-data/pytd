import abc
import re
import prestodb
import tdclient

from pytd.version import __version__

from six import with_metaclass


class QueryEngine(with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def cursor(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def _connect(self, apikey, endpoint, database):
        pass


class PrestoQueryEngine(QueryEngine):

    def __init__(self, apikey, endpoint, database):
        self.engine = self._connect(apikey, endpoint, database)

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self, apikey, endpoint, database):
        http = re.compile(r'https?://')
        user_agent = 'pytd/%s (Presto; prestodb/%s)' % (__version__, prestodb.__version__)
        return prestodb.dbapi.connect(
            host=http.sub('', endpoint).strip('/'),
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database,
            http_headers={'user-agent': user_agent}
        )


class HiveQueryEngine(QueryEngine):

    def __init__(self, apikey, endpoint, database):
        self.engine = self._connect(apikey, endpoint, database)

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self, apikey, endpoint, database):
        user_agent = 'pytd/%s (Hive; tdclient/%s)' % (__version__, tdclient.__version__)
        return tdclient.connect(
            apikey=apikey,
            endpoint=endpoint,
            db=database,
            user_agent=user_agent,
            type='hive'
        )
