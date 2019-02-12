import abc
import prestodb
import tdclient

from six import with_metaclass


class QueryEngine(with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def cursor(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def _connect(self, apikey, database):
        pass


class PrestoQueryEngine(QueryEngine):

    def __init__(self, apikey, database):
        self.engine = self._connect(apikey, database)

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self, apikey, database):
        return prestodb.dbapi.connect(
            host='api-presto.treasuredata.com',
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database
        )


class HiveQueryEngine(QueryEngine):

    def __init__(self, apikey, database):
        self.engine = self._connect(apikey, database)

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self, apikey, database):
        return tdclient.connect(apikey=apikey, db=database, type='hive')
