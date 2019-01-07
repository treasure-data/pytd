import os
import prestodb

from urllib.error import HTTPError
from urllib.request import urlopen

from pytd.error import NotSupportedError

TD_SPARK_BASE_URL = 'https://s3.amazonaws.com/td-spark/%s'


class Connection(object):

    def __init__(self, apikey=None, database='sample_datasets'):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        self.td_presto = prestodb.dbapi.connect(
            host='api-presto.treasuredata.com',
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database
        )

        self.apikey = apikey
        self.database = database

        self.td_spark = None

    def close(self):
        self.td_presto.close()
        if self.td_spark is not None:
            self.td_spark.stop()

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    def cursor(self):
        return self.td_presto.cursor()

    def setup_td_spark(self):
        try:
            from pyspark.sql import SparkSession

            jarname = 'td-spark-assembly_2.11-1.0.0.jar'
            path_td_spark = os.path.join(os.path.dirname(os.path.abspath(__file__)), jarname)

            if not os.path.exists(path_td_spark):
                download_url = TD_SPARK_BASE_URL % jarname
                try:
                    response = urlopen(download_url)
                except HTTPError:
                    raise RuntimeError('failed to access to the download URL: ' + download_url)

                print('Downloading td-spark...')
                try:
                    with open(path_td_spark, 'w+b') as f:
                        f.write(response.read())
                except Exception:
                    os.remove(path_td_spark)
                    raise
                print('Completed to download')

                response.close()

            os.environ['PYSPARK_SUBMIT_ARGS'] = """
            --jars %s
            --conf spark.td.apikey=%s
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
            --conf spark.sql.execution.arrow.enabled=true
            pyspark-shell
            """ % (path_td_spark, self.apikey)

            self.td_spark = SparkSession.builder.master('local[*]').getOrCreate()
        except ImportError:
            raise RuntimeError('PySpark is not installed')
        except Exception as e:
            raise RuntimeError('failed to connect to td-spark: ' + e)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
