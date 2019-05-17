import os
import re
import abc
import logging

from urllib.error import HTTPError
from urllib.request import urlopen

logger = logging.getLogger(__name__)

TD_SPARK_BASE_URL = 'https://s3.amazonaws.com/td-spark/%s'
TD_SPARK_JAR_NAME = 'td-spark-assembly_2.11-1.1.0.jar'


class Writer(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def write_dataframe(self, df, database, table, if_exists):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class SparkWriter(Writer):
    """A writer module that loads Python data to Treasure Data.

    Parameters
    ----------
    apikey : string
        Treasure Data API key.

    endpoint : string
        Treasure Data API server.

    td_spark_path : string, optional
        Path to td-spark-assembly_x.xx-x.x.x.jar. If not given, seek a path
        ``__file__ + TD_SPARK_JAR_NAME`` by default.

    download_if_missing : boolean, default: True
        Download td-spark if it does not exist at the time of initialization.
    """

    def __init__(self, apikey, endpoint, td_spark_path=None, download_if_missing=True):
        site = 'us'
        if '.co.jp' in endpoint:
            site = 'jp'
        if 'eu01' in endpoint:
            site = 'eu01'

        self.td_spark = self._fetch_td_spark(apikey, site, td_spark_path, download_if_missing, endpoint)

    def write_dataframe(self, df, database, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into Spark
        DataFrame, and directly writes to Treasure Data's main storage
        so-called Plazma through a PySpark session.

        Parameters
        ----------
        df : pandas.DataFrame
            Data loaded to a target table.

        database : string
            Name of target database. Ignored if a given table name contains
            ``.`` as ``database.table``.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
        from py4j.protocol import Py4JJavaError

        if if_exists not in ('error', 'overwrite', 'append', 'ignore'):
            raise ValueError('invalid valud for if_exists: %s' % if_exists)

        destination = table
        if '.' not in table:
            destination = database + '.' + table

        # normalize column names so it contains only alphanumeric and `_`
        df = df.rename(lambda c: re.sub(r'[^a-zA-Z0-9]', ' ', str(c)).lower().replace(' ', '_'), axis='columns')

        sdf = self.td_spark.createDataFrame(df)
        try:
            sdf.write.mode(if_exists).format('com.treasuredata.spark').option('table', destination).save()
        except Py4JJavaError as e:
            if 'API_ACCESS_FAILURE' in str(e.java_exception):
                raise PermissionError('failed to access to Treasure Data Plazma API. Contact customer support to enable access rights.')
            raise RuntimeError('failed to load table via td-spark: ' + str(e.java_exception))

    def close(self):
        """Close a PySpark session connected to Treasure Data.
        """
        self.td_spark.stop()

    def _fetch_td_spark(self, apikey, site, td_spark_path, download_if_missing, endpoint):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            raise RuntimeError('PySpark is not installed')

        if td_spark_path is None:
            td_spark_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TD_SPARK_JAR_NAME)

        available = os.path.exists(td_spark_path)

        if not available and download_if_missing:
            self._download_td_spark(td_spark_path)
        elif not available:
            raise IOError('td-spark is not found and `download_if_missing` is False')

        api_conf = "--conf spark.td.site={}".format(site)

        _dev_env = [env for env in ['development', 'staging'] if env in endpoint]
        if len(_dev_env) > 0:
            api_regex = re.compile(r'(?:https?://)?(api(?:-.+?)?)\.')
            api_host = api_regex.sub('\\1.', endpoint).strip('/')
            plazma_api = api_regex.sub('\\1-plazma.', endpoint).strip('/')
            presto_api = api_regex.sub('\\1-presto.', endpoint).strip('/')
            api_conf = """\
            --conf spark.td.api.host=%s
            --conf spark.td.plazma_api.host=%s
            --conf spark.td.presto_api.host=%s
            """ % (api_host, plazma_api, presto_api)

        os.environ['PYSPARK_SUBMIT_ARGS'] = """\
        --jars %s
        --conf spark.td.apikey=%s
        %s
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
        --conf spark.sql.execution.arrow.enabled=true
        pyspark-shell
        """ % (td_spark_path, apikey, api_conf)

        try:
            return SparkSession.builder.master('local[*]').getOrCreate()
        except Exception as e:
            raise RuntimeError('failed to connect to td-spark: ' + str(e))

    def _download_td_spark(self, destination):
        download_url = TD_SPARK_BASE_URL % TD_SPARK_JAR_NAME
        try:
            response = urlopen(download_url)
        except HTTPError:
            raise RuntimeError('failed to access to the download URL: ' + download_url)

        logger.info('Downloading td-spark...')
        try:
            with open(destination, 'w+b') as f:
                f.write(response.read())
        except Exception:
            os.remove(destination)
            raise
        logger.info('Completed to download')

        response.close()
