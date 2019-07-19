import abc
import logging
import os
import re
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import urlopen

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/{}"
TD_SPARK_JAR_NAME = "td-spark-assembly_2.11-19.7.0.jar"
logger = logging.getLogger(__name__)


class Writer(metaclass=abc.ABCMeta):
    def __init__(self):
        self.closed = False

    @abc.abstractmethod
    def write_dataframe(self, dataframe, table, if_exists):
        pass

    def close(self):
        self.closed = True

    @staticmethod
    def from_string(writer, **kwargs):
        writer = writer.lower()
        if writer == "bulk_import":
            return BulkImportWriter()
        elif writer == "insert_into":
            return InsertIntoWriter()
        elif writer == "spark":
            return SparkWriter(**kwargs)
        else:
            raise ValueError("unknown way to upload data to TD is specified")


class InsertIntoWriter(Writer):
    """A writer module that loads Python data to Treasure Data by issueing
    INSERT INTO query in Presto.
    """

    def write_dataframe(self, dataframe, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method translates a given pandas.DataFrame into a `INSERT INTO ...
        VALUES ...` Presto query.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        table : pytd.table.Table
            Target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.
            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        if self.closed:
            raise RuntimeError("this writer is already closed and no longer available")

        column_names, column_types = [], []
        for c, t in zip(dataframe.columns, dataframe.dtypes):
            if t == "int64":
                presto_type = "bigint"
            elif t == "float64":
                presto_type = "double"
            else:
                presto_type = "varchar"
                dataframe[c] = dataframe[c].astype(str)
                logger.info(
                    "column '{}' has non-numeric numpy.dtype '{}', and be "
                    "converted into and stored as 'varchar' type on Treasure "
                    "Data.".format(c, t)
                )
            column_names.append(c)
            column_types.append(presto_type)

        self._insert_into(
            table, dataframe.values.tolist(), column_names, column_types, if_exists
        )

    def _insert_into(self, table, list_of_list, column_names, column_types, if_exists):
        """Write a given lists to a Treasure Data table.

        This method translates the given data into an ``INSERT INTO ...  VALUES
        ...`` Presto query.

        Parameters
        ----------
        table : pytd.table.Table
            Target table.

        list_of_list : list of lists
            Data loaded to a target table. Each element is a list that
            represents single table row.

        column_names : list of string
            Column names.

        column_types : list of string
            Column types corresponding to the names. Note that Treasure Data
            supports limited amount of types as documented in:
            https://support.treasuredata.com/hc/en-us/articles/360001266468-Schema-Management

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.
            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """

        if table.exist:
            if if_exists == "error":
                raise RuntimeError(
                    "target table '{}.{}' already exists".format(
                        table.database, table.table
                    )
                )
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                pass
            elif if_exists == "overwrite":
                table.delete()
                table.create(column_names, column_types)
            else:
                raise ValueError("invalid valud for if_exists: {}".format(if_exists))
        else:
            table.create(column_names, column_types)

        rows = []
        for lst in list_of_list:
            list_of_value_strings = [
                "'{}'".format(e.replace("'", '"')) if isinstance(e, str) else str(e)
                for e in lst
            ]
            rows.append("({})".format(", ".join(list_of_value_strings)))

        q_insert = "INSERT INTO {}.{} ({}) VALUES {}".format(
            table.database,
            table.table,
            ", ".join(map(str, column_names)),
            ", ".join(rows),
        )
        table.client.query(q_insert, engine="presto")


class BulkImportWriter(Writer):
    """A writer module that loads Python data to Treasure Data by using
    td-client-python's bulk importer.
    """

    def write_dataframe(self, dataframe, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into a
        temporary CSV file, and upload the file to Treasure Data via bulk
        import API.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        table : pytd.table.Table
            Target table.

        if_exists : {'error', 'overwrite', 'ignore'}
            What happens when a target table already exists.
            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - ignore: do nothing.
        """
        if self.closed:
            raise RuntimeError("this writer is already closed and no longer available")

        if "time" not in dataframe.columns:  # need time column for bulk import
            dataframe["time"] = int(time.time())

        fp = tempfile.NamedTemporaryFile(suffix=".csv")
        dataframe.to_csv(fp.name)

        self._bulk_import(table, fp, if_exists)

        fp.close()

    def _bulk_import(self, table, csv, if_exists):
        """Write a specified CSV file to a Treasure Data table.

        This method uploads the file to Treasure Data via bulk import API.

        Parameters
        ----------
        table : pytd.table.Table
            Target table.

        csv : File pointer of a CSV file
            Data in this file will be loaded to a target table.

        if_exists : {'error', 'overwrite', 'ignore'}
            What happens when a target table already exists.
            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - ignore: do nothing.
        """
        if table.exist:
            if if_exists == "error":
                raise RuntimeError(
                    "target table '{}.{}' already exists".format(
                        table.database, table.table
                    )
                )
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                raise ValueError("Bulk import API does not support `append`")
            elif if_exists == "overwrite":
                table.delete()
                table.create()
            else:
                raise ValueError("invalid valud for if_exists: {}".format(if_exists))
        else:
            table.create()

        session_name = "session-{}".format(int(time.time()))

        bulk_import = table.client.api_client.create_bulk_import(
            session_name, table.database, table.table
        )
        try:
            logger.info("uploading data converted into a CSV file")
            bulk_import.upload_file("part", "csv", csv.name)
            bulk_import.freeze()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError("failed to upload file: {}".format(e))

        logger.info("performing a bulk import job")
        bulk_import.perform(wait=True)

        if 0 < bulk_import.error_records:
            logger.warning(
                "detected {} error records.".format(bulk_import.error_records)
            )

        if 0 < bulk_import.valid_records:
            logger.info("imported {} records.".format(bulk_import.valid_records))
        else:
            raise RuntimeError(
                "no records have been imported: {}".format(bulk_import.name)
            )
        bulk_import.commit(wait=True)
        bulk_import.delete()


class SparkWriter(Writer):
    """A writer module that loads Python data to Treasure Data.

    Parameters
    ----------
    td_spark_path : string, optional
        Path to td-spark-assembly_x.xx-x.x.x.jar. If not given, seek a path
        ``__file__ + TD_SPARK_JAR_NAME`` by default.

    download_if_missing : boolean, default: True
        Download td-spark if it does not exist at the time of initialization.
    """

    def __init__(self, td_spark_path=None, download_if_missing=True):
        self.td_spark_path = td_spark_path
        self.download_if_missing = download_if_missing

        self.td_spark = None
        self.fetched_apikey, self.fetched_endpoint = "", ""

    @property
    def closed(self):
        return self.td_spark is not None and self.td_spark._jsc.sc().isStopped()

    def write_dataframe(self, dataframe, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into Spark
        DataFrame, and directly writes to Treasure Data's main storage
        so-called Plazma through a PySpark session.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        table : pytd.table.Table
            Target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.
            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        if self.closed:
            raise RuntimeError("this writer is already closed and no longer available")

        if if_exists not in ("error", "overwrite", "append", "ignore"):
            raise ValueError("invalid valud for if_exists: {}".format(if_exists))

        if self.td_spark is None:
            self.td_spark = self._fetch_td_spark(
                table.client.apikey,
                table.client.endpoint,
                self.td_spark_path,
                self.download_if_missing,
            )
            self.fetched_apikey, self.fetched_endpoint = (
                table.client.apikey,
                table.client.endpoint,
            )
        elif (
            table.client.apikey != self.fetched_apikey
            or table.client.endpoint != self.fetched_endpoint
        ):
            raise ValueError(
                "given Table instance and SparkSession have different apikey"
                "and/or endpoint. Create and use a new SparkWriter instance."
            )

        from py4j.protocol import Py4JJavaError

        sdf = self.td_spark.createDataFrame(dataframe)
        try:
            sdf.write.mode(if_exists).format("com.treasuredata.spark").option(
                "table", "{}.{}".format(table.database, table.table)
            ).save()
        except Py4JJavaError as e:
            if "API_ACCESS_FAILURE" in str(e.java_exception):
                raise PermissionError(
                    "failed to access to Treasure Data Plazma API."
                    "Contact customer support to enable access rights."
                )
            raise RuntimeError(
                "failed to load table via td-spark: " + str(e.java_exception)
            )

    def close(self):
        """Close a PySpark session connected to Treasure Data.
        """
        if self.td_spark is not None:
            self.td_spark.stop()

    def _fetch_td_spark(self, apikey, endpoint, td_spark_path, download_if_missing):
        try:
            from pyspark.conf import SparkConf
            from pyspark.sql import SparkSession
        except ImportError:
            raise RuntimeError("PySpark is not installed")

        conf = (
            SparkConf()
            .setMaster("local[*]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.sql.execution.arrow.enabled", "true")
        )
        conf.set("spark.td.apikey", apikey)

        if td_spark_path is None:
            td_spark_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), TD_SPARK_JAR_NAME
            )

        available = os.path.exists(td_spark_path)

        if not available and download_if_missing:
            self._download_td_spark(td_spark_path)
        elif not available:
            raise IOError("td-spark is not found and `download_if_missing` is False")

        conf.set("spark.jars", td_spark_path)

        plazma_api = os.getenv("TD_PLAZMA_API")
        presto_api = os.getenv("TD_PRESTO_API")

        if plazma_api and presto_api:
            api_regex = re.compile(r"(?:https?://)?(api(?:-.+?)?)\.")
            conf.set("spark.td.api.host", api_regex.sub("\\1.", endpoint).strip("/"))
            conf.set("spark.td.plazma_api.host", plazma_api)
            conf.set("spark.td.presto_api.host", presto_api)

        site = "us"
        if ".co.jp" in endpoint:
            site = "jp"
        if "eu01" in endpoint:
            site = "eu01"
        conf.set("spark.td.site", site)

        try:
            return SparkSession.builder.config(conf=conf).getOrCreate()
        except Exception as e:
            raise RuntimeError("failed to connect to td-spark: " + str(e))

    def _download_td_spark(self, destination):
        download_url = TD_SPARK_BASE_URL.format(TD_SPARK_JAR_NAME)
        try:
            response = urlopen(download_url)
        except HTTPError:
            raise RuntimeError("failed to access to the download URL: " + download_url)

        logger.info("Downloading td-spark...")
        try:
            with open(destination, "w+b") as f:
                f.write(response.read())
        except Exception:
            os.remove(destination)
            raise
        logger.info("Completed to download")

        response.close()
