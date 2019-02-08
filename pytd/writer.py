import os
import re
import abc
import logging

from six import with_metaclass

try:
    from urllib.error import HTTPError
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen, HTTPError

logger = logging.getLogger(__name__)

TD_SPARK_BASE_URL = 'https://s3.amazonaws.com/td-spark/%s'


class Writer(with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def write_dataframe(self, df, destination, if_exists):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class SparkWriter(Writer):

    def __init__(self, apikey):
        self._setup_td_spark(apikey)

    def write_dataframe(self, df, destination, if_exists):
        from py4j.protocol import Py4JJavaError

        if if_exists not in ('error', 'overwrite', 'append', 'ignore'):
            raise ValueError('invalid valud for if_exists: %s' % if_exists)

        # normalize column names so it contains only alphanumeric and `_`
        df = df.rename(lambda c: re.sub(r'[^a-zA-Z0-9]', ' ', str(c)).lower().replace(' ', '_'), axis='columns')

        sdf = self.td_spark.createDataFrame(df)
        try:
            sdf.write.mode(if_exists).format('com.treasuredata.spark').option('table', destination).save()
        except Py4JJavaError as e:
            if 'API_ACCESS_FAILURE' in str(e.java_exception):
                raise PermissionError('failed to access to Treasure Data Plazma API. Contact <sales@treasure-data.com> to enable access rights.')
            raise RuntimeError('failed to load table via td-spark: ' + str(e.java_exception))

    def close(self):
        self.td_spark.stop()

    def _setup_td_spark(self, apikey):
        try:
            from pyspark.sql import SparkSession

            jarname = 'td-spark-assembly_2.11-1.1.0.jar'
            path_td_spark = os.path.join(os.path.dirname(os.path.abspath(__file__)), jarname)

            if not os.path.exists(path_td_spark):
                download_url = TD_SPARK_BASE_URL % jarname
                try:
                    response = urlopen(download_url)
                except HTTPError:
                    raise RuntimeError('failed to access to the download URL: ' + download_url)

                logger.info('Downloading td-spark...')
                try:
                    with open(path_td_spark, 'w+b') as f:
                        f.write(response.read())
                except Exception:
                    os.remove(path_td_spark)
                    raise
                logger.info('Completed to download')

                response.close()

            os.environ['PYSPARK_SUBMIT_ARGS'] = """
            --jars %s
            --conf spark.td.apikey=%s
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
            --conf spark.sql.execution.arrow.enabled=true
            pyspark-shell
            """ % (path_td_spark, apikey)

            self.td_spark = SparkSession.builder.master('local[*]').getOrCreate()
        except ImportError:
            raise RuntimeError('PySpark is not installed')
        except Exception as e:
            raise RuntimeError('failed to connect to td-spark: ' + e)
