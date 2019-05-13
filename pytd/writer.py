import abc
import logging
import os
import re
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import urlopen

import numpy as np
from tdclient.errors import NotFoundError

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/%s"
TD_SPARK_JAR_NAME = "td-spark-assembly_2.11-1.1.0.jar"
logger = logging.getLogger(__name__)


class Writer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write_dataframe(self, df, database, table, if_exists):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def _validate_if_exists(
        self, if_exists, candidates=["error", "overwrite", "append", "ignore"]
    ):
        if if_exists not in candidates:
            raise ValueError("invalid valud for if_exists: %s" % if_exists)


class InsertIntoWriter(Writer):
    """A writer module that loads Python data to Treasure Data by issueing
    INSERT INTO query in Presto.

    Parameters
    ----------
    api_client : tdclient.Client
        Treasure Data Client instance created by td-client-python.

    presto : pytd.query_engine.PrestoQueryEngine
        A pre-defined Presto query engine instance.
    """

    def __init__(self, api_client, presto):
        self.api_client = api_client
        self.presto = presto

    def write_dataframe(self, df, database, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method translates a given pandas.DataFrame into a `INSERT INTO ...
        VALUES ...` Presto query.

        Parameters
        ----------
        df : pandas.DataFrame
            Data loaded to a target table.

        database : string
            Name of target database. Ignored if a given table name contains
            ``.`` as ``database.table``.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.
        """
        self._validate_if_exists(if_exists)

        destination = table
        if "." not in table:
            destination = database + "." + table
        else:
            database, table = table.split(".")

        schema = []
        for c, t in zip(df.columns, df.dtypes):
            if t == "int64":
                presto_type = "bigint"
            elif t == "float64":
                presto_type = "double"
            else:  # TODO: Support more array type
                presto_type = "varchar"
                df[c] = df[c].astype(str)
            schema.append(str(c) + " " + presto_type)

        q_delete = "DROP TABLE IF EXISTS %s" % (destination,)

        q_create = "CREATE TABLE %s (%s)" % (destination, ", ".join(schema))

        try:
            self.api_client.table(database, table)
        except NotFoundError:  # new table
            self.presto.execute(q_delete)
            self.presto.execute(q_create)
        else:  # exits
            if if_exists == "error":
                raise RuntimeError("target table already exists")
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                pass
            else:  # overwrite
                self.presto.execute(q_delete)
                self.presto.execute(q_create)

        values = np.array2string(df.values, separator=", ")[1:-1]

        # convert [] into (), but keep [] for array-like values
        values = re.sub(r'\]($|[^"])', r")\1", re.sub(r'(^|[^"])\[', r"\1(", values))

        # TODO: support array type
        # e.g., array value can be preprocessed as:
        #   values = re.sub(r'\"\[(.+?)\]\"', r'array[\1]', values)

        q_insert = "INSERT INTO "
        q_insert += destination
        q_insert += " (" + (", ".join(map(str, df.columns))) + ") "
        q_insert += "VALUES " + values
        self.presto.execute(q_insert)

    def close(self):
        """Close an insert into writer.
        """
        self.api_client = None
        self.presto = None


class BulkImportWriter(Writer):
    """A writer module that loads Python data to Treasure Data by using
    td-client-python's bulk importer.

    Parameters
    ----------
    api_client : tdclient.Client
        Treasure Data Client instance created by td-client-python.
    """

    def __init__(self, api_client):
        self.api_client = api_client

    def write_dataframe(self, df, database, table, if_exists):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into a
        temporary CSV file, and upload the file to Treasure Data via bulk
        import API.

        Parameters
        ----------
        df : pandas.DataFrame
            Data loaded to a target table.

        database : string
            Name of target database. Ignored if a given table name contains
            ``.`` as ``database.table``.

        table : string
            Name of target table.

        if_exists : {'error', 'overwrite', 'ignore'}
            What happens when a target table already exists.
        """
        self._validate_if_exists(if_exists, candidates=["error", "overwrite", "ignore"])

        if "." in table:
            database, table = table.split(".")

        try:
            self.api_client.table(database, table)
        except NotFoundError:  # new table
            self.api_client.create_log_table(database, table)
        else:  # exits
            if if_exists == "error":
                raise RuntimeError("target table already exists")
            elif if_exists == "ignore":
                return
            elif if_exists == "overwrite":
                self.api_client.delete_table(database, table)
                self.api_client.create_log_table(database, table)
            else:
                raise ValueError("invalid valud for if_exists: %s" % if_exists)

        ts = int(time.time())

        if "time" not in df.columns:  # need time column for bulk import
            df["time"] = ts

        session_name = "session-%d" % ts

        bulk_import = self.api_client.create_bulk_import(session_name, database, table)
        try:
            fp = tempfile.NamedTemporaryFile(suffix=".csv")
            df.to_csv(fp.name)  # XXX: split into multiple CSV files?

            bulk_import.upload_file("part", "csv", fp.name)
            bulk_import.freeze()

            fp.close()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError("failed to upload file: " + str(e))

        bulk_import.perform(wait=True)

        if 0 < bulk_import.error_records:
            logger.warning("detected %d error records." % bulk_import.error_records)

        if 0 < bulk_import.valid_records:
            logger.info("imported %d records." % bulk_import.valid_records)
        else:
            raise RuntimeError(
                "no records have been imported: %s" % repr(bulk_import.name)
            )
        bulk_import.commit(wait=True)
        bulk_import.delete()

    def close(self):
        """Close a bulk import writer.
        """
        self.api_client = None


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
        self.td_spark = self._fetch_td_spark(
            apikey, endpoint, td_spark_path, download_if_missing
        )

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

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.
        """
        self._validate_if_exists(if_exists)

        if if_exists not in ("error", "overwrite", "append", "ignore"):
            raise ValueError("invalid valud for if_exists: %s" % if_exists)

        from py4j.protocol import Py4JJavaError

        destination = table
        if "." not in table:
            destination = database + "." + table

        # normalize column names so it contains only alphanumeric and `_`
        df = df.rename(
            lambda c: re.sub(r"[^a-zA-Z0-9]", " ", str(c)).lower().replace(" ", "_"),
            axis="columns",
        )

        sdf = self.td_spark.createDataFrame(df)
        try:
            sdf.write.mode(if_exists).format("com.treasuredata.spark").option(
                "table", destination
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
        self.td_spark.stop()

    def _fetch_td_spark(self, apikey, endpoint, td_spark_path, download_if_missing):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            raise RuntimeError("PySpark is not installed")

        if td_spark_path is None:
            td_spark_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), TD_SPARK_JAR_NAME
            )

        available = os.path.exists(td_spark_path)

        if not available and download_if_missing:
            self._download_td_spark(td_spark_path)
        elif not available:
            raise IOError("td-spark is not found and `download_if_missing` is False")

        plazma_api = os.getenv("TD_PLAZMA_API")
        presto_api = os.getenv("TD_PRESTO_API")

        api_conf = ""
        if plazma_api and presto_api:
            api_regex = re.compile(r"(?:https?://)?(api(?:-.+?)?)\.")
            api_host = api_regex.sub("\\1.", endpoint).strip("/")
            api_conf = """\
            --conf spark.td.api.host=%s
            --conf spark.td.plazma_api.host=%s
            --conf spark.td.presto_api.host=%s
            """ % (
                api_host,
                plazma_api,
                presto_api,
            )

        site = "us"
        if ".co.jp" in endpoint:
            site = "jp"
        if "eu01" in endpoint:
            site = "eu01"

        os.environ[
            "PYSPARK_SUBMIT_ARGS"
        ] = """\
        --jars %s
        --conf spark.td.apikey=%s
        --conf spark.td.site=%s
        %s
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
        --conf spark.sql.execution.arrow.enabled=true
        pyspark-shell
        """ % (
            td_spark_path,
            apikey,
            site,
            api_conf,
        )

        try:
            return SparkSession.builder.master("local[*]").getOrCreate()
        except Exception as e:
            raise RuntimeError("failed to connect to td-spark: " + str(e))

    def _download_td_spark(self, destination):
        download_url = TD_SPARK_BASE_URL % TD_SPARK_JAR_NAME
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
