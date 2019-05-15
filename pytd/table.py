import logging
import os
import re
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import urlopen

import numpy as np
import tdclient

logger = logging.getLogger(__name__)

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/%s"
TD_SPARK_JAR_NAME = "td-spark-assembly_2.11-1.1.0.jar"


class Table(object):
    """A table writer module that imports Python data to a table.

    Parameters
    ----------
    client : pytd.Client
        Treasure Data client.

    database : string
        Database name.

    table : string
        Table name.
    """

    def __init__(self, client, database, table):
        try:
            client.api_client.database(database)
        except tdclient.errors.NotFoundError as e:
            raise ValueError(
                "faild to create pytd.table.Table instance for `{}.{}`: {}".format(
                    database, table, e
                )
            )

        self.database = database
        self.table = table
        self.client = client
        self.td_spark = None

    @property
    def exist(self):
        """Check if a configured table exists.

        Returns
        -------
        boolean
        """
        try:
            self.client.api_client.table(self.database, self.table)
        except tdclient.errors.NotFoundError:
            return False
        return True

    def create(self, column_names=[], column_types=[]):
        """Create a table named as configured.

        When ``column_names`` and ``column_types`` are given, table is created
        by a Presto query with the specified schema.

        Parameters
        ----------
        column_names : list of string, optional
            Column names.

        column_types : list of string, optional
            Column types corresponding to the names. Note that Treasure Data
            supports limited amount of types as documented in:
            https://support.treasuredata.com/hc/en-us/articles/360001266468-Schema-Management
        """
        if len(column_names) > 0:
            schema = ", ".join(
                map(
                    lambda t: "{} {}".format(t[0], t[1]),
                    zip(column_names, column_types),
                )
            )
            q_create = "CREATE TABLE {}.{} ({})".format(
                self.database, self.table, schema
            )
            self.client.query(q_create, engine="presto")
        else:
            self.client.api_client.create_log_table(self.database, self.table)

    def delete(self):
        """Delete a table from Treasure Data.
        """
        self.client.api_client.delete_table(self.database, self.table)

    def insert_into(self, dataframe, if_exists="error"):
        """Write a given DataFrame to a Treasure Data table.

        This method translates a given pandas.DataFrame into an ``INSERT INTO
        ...  VALUES ...`` Presto query.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
        column_names, column_types = [], []
        for c, t in zip(dataframe.columns, dataframe.dtypes):
            if t == "int64":
                presto_type = "bigint"
            elif t == "float64":
                presto_type = "double"
            else:  # TODO: Support more array type
                presto_type = "varchar"
                dataframe[c] = dataframe[c].astype(str)
            column_names.append(c)
            column_types.append(presto_type)

        if self.exist:
            if if_exists == "error":
                raise RuntimeError("target table already exists")
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                pass
            elif if_exists == "overwrite":
                self.delete()
                self.create(column_names, column_types)
            else:
                raise ValueError("invalid valud for if_exists: {}".format(if_exists))
        else:
            self.create(column_names, column_types)

        values = np.array2string(dataframe.values, separator=", ")[1:-1]

        # convert [] into (), but keep [] for array-like values
        values = re.sub(r'\]($|[^"])', r")\1", re.sub(r'(^|[^"])\[', r"\1(", values))

        # TODO: support array type
        # e.g., array value can be preprocessed as:
        #   values = re.sub(r'\"\[(.+?)\]\"', r'array[\1]', values)

        q_insert = "INSERT INTO {}.{} ({}) VALUES {}".format(
            self.database, self.table, ", ".join(map(str, dataframe.columns)), values
        )
        self.client.query(q_insert, engine="presto")

    def bulk_import(self, dataframe, if_exists="error"):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into a
        temporary CSV file, and upload the file to Treasure Data via bulk
        import API.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        if_exists : {'error', 'overwrite', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
        if self.exist:
            if if_exists == "error":
                raise RuntimeError("target table already exists")
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                raise ValueError("Bulk import API does not support `append`")
            elif if_exists == "overwrite":
                self.delete()
                self.create()
            else:
                raise ValueError("invalid valud for if_exists: {}".format(if_exists))
        else:
            self.create()

        ts = int(time.time())

        if "time" not in dataframe.columns:  # need time column for bulk import
            dataframe["time"] = ts

        session_name = "session-{}".format(ts)

        bulk_import = self.client.api_client.create_bulk_import(
            session_name, self.database, self.table
        )
        try:
            fp = tempfile.NamedTemporaryFile(suffix=".csv")
            dataframe.to_csv(fp.name)  # XXX: split into multiple CSV files?

            bulk_import.upload_file("part", "csv", fp.name)
            bulk_import.freeze()

            fp.close()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError("failed to upload file: {}".format(e))

        bulk_import.perform(wait=True)

        if 0 < bulk_import.error_records:
            logger.warning(
                "detected {} error records.".format(bulk_import.error_records)
            )

        if 0 < bulk_import.valid_records:
            logger.info("imported {} records.".format(bulk_import.valid_records))
        else:
            raise RuntimeError(
                "no records have been imported: {}".format(repr(bulk_import.name))
            )
        bulk_import.commit(wait=True)
        bulk_import.delete()

    def spark_import(
        self, dataframe, if_exists="error", td_spark_path=None, download_if_missing=True
    ):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given pandas.DataFrame into Spark
        DataFrame, and directly writes to Treasure Data's main storage
        so-called Plazma through a PySpark session.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.

        td_spark_path : string, optional
            Path to td-spark-assembly_x.xx-x.x.x.jar. If not given, seek a path
            ``__file__ + TD_SPARK_JAR_NAME`` by default.

        download_if_missing : boolean, default: True
            Download td-spark if it does not exist at the time of initialization.
        """
        if if_exists not in ["error", "overwrite", "append", "ignore"]:
            raise ValueError("invalid valud for if_exists: %s" % if_exists)

        if self.td_spark is None:
            site = "us"
            if ".co.jp" in self.client.endpoint:
                site = "jp"
            if "eu01" in self.client.endpoint:
                site = "eu01"
            self.td_spark = self._fetch_td_spark(
                self.client.apikey, site, td_spark_path, download_if_missing
            )

        from py4j.protocol import Py4JJavaError

        destination = "{}.{}".format(self.database, self.table)

        # normalize column names so it contains only alphanumeric and `_`
        dataframe = dataframe.rename(
            lambda c: re.sub(r"[^a-zA-Z0-9]", " ", str(c)).lower().replace(" ", "_"),
            axis="columns",
        )

        sdataframe = self.td_spark.createDataFrame(dataframe)
        try:
            sdataframe.write.mode(if_exists).format("com.treasuredata.spark").option(
                "table", destination
            ).save()
        except Py4JJavaError as e:
            if "API_ACCESS_FAILURE" in str(e.java_exception):
                raise PermissionError(
                    "failed to access to Treasure Data Plazma API. Contact"
                    "customer support to enable access rights."
                )
            raise RuntimeError(
                "failed to load table via td-spark: {}".format(e.java_exception)
            )

    def close(self):
        self.client = None
        if self.td_spark is not None:
            self.td_spark.stop()

    def _fetch_td_spark(self, apikey, site, td_spark_path, download_if_missing):
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

        os.environ[
            "PYSPARK_SUBMIT_ARGS"
        ] = """
        --jars {}
        --conf spark.td.apikey={}
        --conf spark.td.site={}
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
        --conf spark.sql.execution.arrow.enabled=true
        pyspark-shell
        """.format(
            td_spark_path, apikey, site
        )

        try:
            return SparkSession.builder.master("local[*]").getOrCreate()
        except Exception as e:
            raise RuntimeError("failed to connect to td-spark: {}".format(e))

    def _download_td_spark(self, destination):
        download_url = TD_SPARK_BASE_URL % TD_SPARK_JAR_NAME
        try:
            response = urlopen(download_url)
        except HTTPError:
            raise RuntimeError(
                "failed to access to the download URL: {}".format(download_url)
            )

        logger.info("Downloading td-spark...")
        try:
            with open(destination, "w+b") as f:
                f.write(response.read())
        except Exception:
            os.remove(destination)
            raise
        logger.info("Completed to download")

        response.close()
