import abc
import re
import six
import prestodb
import tdclient

from .version import __version__

import logging
logger = logging.getLogger(__name__)


class QueryEngine(six.with_metaclass(abc.ABCMeta)):

    def __init__(self, apikey, endpoint, database, header):
        self.apikey = apikey
        self.endpoint = endpoint
        self.database = database
        self.header = header

        self.client = tdclient.Client(apikey=apikey, endpoint=endpoint, user_agent=self.get_user_agent())

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
            header = "-- client: {0}\n".format(self.get_user_agent())

        if isinstance(extra_lines, six.string_types):
            header += "-- {0}\n".format(extra_lines)
        elif isinstance(extra_lines, (list, tuple)):
            header += ''.join(["-- {0}\n".format(line) for line in extra_lines])

        return header

    def get_job_result(self, job, wait=True):
        if wait:
            job.wait()

        if not job.success():
            if job.debug and job.debug['stderr']:
                logger.error(job.debug['stderr'])
            raise RuntimeError("job {0} {1}".format(job.job_id, job.status()))

        if not job.finished():
            job.wait()

        columns = [c[0] for c in job.result_schema]
        rows = job.result()

        return {'data': rows, 'columns': columns}

    @abc.abstractmethod
    def get_user_agent(self):
        pass

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
        return prestodb.dbapi.connect(
            host=http.sub('', self.endpoint).strip('/').replace('api', 'api-presto'),
            port=443,
            http_scheme='https',
            user=self.apikey,
            catalog='td-presto',
            schema=self.database,
            http_headers={'user-agent': self.get_user_agent()}
        )

    def get_user_agent(self):
        return 'pytd/%s (Presto; prestodb/%s)' % (__version__, prestodb.__version__)


class HiveQueryEngine(QueryEngine):

    def __init__(self, apikey, endpoint, database, header):
        super(HiveQueryEngine, self).__init__(apikey, endpoint, database, header)
        self.engine = self._connect()

    def cursor(self):
        return self.engine.cursor()

    def close(self):
        self.engine.close()

    def _connect(self):
        return tdclient.connect(
            apikey=self.apikey,
            endpoint=self.endpoint,
            db=self.database,
            user_agent=self.get_user_agent(),
            type='hive'
        )

    def get_user_agent(self):
        return 'pytd/%s (Hive; tdclient/%s)' % (__version__, tdclient.__version__)
