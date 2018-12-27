import os
import prestodb

from urllib.error import HTTPError
from urllib.request import urlopen

TD_SPARK_BASE_URL = 'https://s3.amazonaws.com/td-spark/%s'


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    return [column_names] + rows


def query_iterrows(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    index = 0
    column_names = None
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if index == 0:
            column_names = [desc[0] for desc in cur.description]
        yield index, dict(zip(column_names, row))
        index += 1


def write(df, table, connection, if_exists='error'):
    if connection.td_spark is None:
        try:
            connection.setup_td_spark()
        except Exception as e:
            raise e

    if if_exists not in ('error', 'overwrite', 'append', 'ignore'):
        raise ValueError('invalid valud for if_exists: %s' % if_exists)

    destination = table
    if '.' not in table:
        destination = connection.database + '.' + table

    sdf = connection.td_spark.createDataFrame(df)
    sdf.write.mode(if_exists).format('com.treasuredata.spark').option('table', destination).save()


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

    def cursor(self):
        return self.td_presto.cursor()

    def setup_td_spark(self):
        try:
            from pyspark.sql import SparkSession

            directory = os.path.dirname(os.path.abspath(__file__))

            path_conf = os.path.join(directory, 'td-spark.conf')
            with open(path_conf, 'w') as f:
                f.write('spark.td.apikey=%s\n' % self.apikey)
                f.write('spark.serializer=org.apache.spark.serializer.KryoSerializer\n')
                f.write('spark.sql.execution.arrow.enabled=true\n')

            jarname = 'td-spark-assembly_2.11-1.0.0.jar'
            path_td_spark = os.path.join(directory, jarname)

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

            os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars %s --properties-file %s pyspark-shell' % (path_td_spark, path_conf)

            self.td_spark = SparkSession.builder.master("local[*]").getOrCreate()
        except ImportError:
            raise RuntimeError('PySpark is not installed')
        except Exception as e:
            raise RuntimeError('failed to connect to td-spark: ' + e)
