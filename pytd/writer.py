import abc
import logging
import tempfile
import time

import pandas as pd

from .spark import fetch_td_spark_context

logger = logging.getLogger(__name__)


def _cast_dtypes(dataframe, inplace=True):
    """Convert dtypes into one of {int, float, str} type.

    A character code (one of ‘biufcmMOSUV’) identifying the general kind of data.

    b  boolean
    i  signed integer
    u  unsigned integer
    f  floating-point
    c  complex floating-point
    m  timedelta
    M  datetime
    O  object
    S  (byte-)string
    U  Unicode
    V  void
    """
    df = dataframe if inplace else dataframe.copy()

    for column, kind in dataframe.dtypes.apply(lambda dtype: dtype.kind).iteritems():
        if kind == "i" or kind == "u":
            t = "Int64" if df[column].isnull().any() else "int64"
        elif kind == "f":
            t = float
        else:
            t = str
        df[column] = df[column].astype(t)

        # Bulk Import API internally handles boolean string as a boolean type,
        # and hence "True" ("False") will be stored as "true" ("false"). Align
        # to lower case here.
        if kind == "b":
            df[column] = df[column].apply(lambda s: s.lower())

    if not inplace:
        return df


def _get_schema(dataframe):
    column_names, column_types = [], []
    for c, t in zip(dataframe.columns, dataframe.dtypes):
        # Compare nullable integer type by using pandas function because `t ==
        # "Int64"` causes a following warning for some reasons:
        #     DeprecationWarning: Numeric-style type codes are deprecated and
        #     will result in an error in the future.
        if t == "int64" or pd.core.dtypes.common.is_dtype_equal(t, "Int64"):
            presto_type = "bigint"
        elif t == "float64":
            presto_type = "double"
        else:
            presto_type = "varchar"
            logger.info(
                "column '{}' has non-numeric. The values are stored as "
                "'varchar' type on Treasure Data.".format(c)
            )
        column_names.append(c)
        column_types.append(presto_type)
    return column_names, column_types


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

        _cast_dtypes(dataframe)

        column_names, column_types = _get_schema(dataframe)

        self._insert_into(
            table,
            list(dataframe.itertuples(index=False, name=None)),
            column_names,
            column_types,
            if_exists,
        )

    def _insert_into(self, table, list_of_tuple, column_names, column_types, if_exists):
        """Write a given lists to a Treasure Data table.

        Parameters
        ----------
        table : pytd.table.Table
            Target table.

        list_of_tuple : list of tuples
            Data loaded to a target table. Each element is a tuple that
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

        q_insert = self._build_query(
            table.database, table.table, list_of_tuple, column_names
        )
        table.client.query(q_insert, engine="presto")

    def _build_query(self, database, table, list_of_tuple, column_names):
        """Translates the given data into an ``INSERT INTO ...  VALUES ...``
        Presto query.

        Parameters
        ----------
        database : string
            Target database name.

        table : string
            Target table name.

        list_of_tuple : list of tuples
            Data loaded to a target table. Each element is a tuple that
            represents single table row.

        column_names : list of string
            Column names.
        """
        rows = []
        for tpl in list_of_tuple:
            list_of_value_strings = [
                (
                    "'{}'".format(e.replace("'", '"'))
                    if isinstance(e, str)
                    else ("null" if pd.isnull(e) else str(e))
                )
                for e in tpl
            ]
            rows.append("({})".format(", ".join(list_of_value_strings)))

        return "INSERT INTO {}.{} ({}) VALUES {}".format(
            database, table, ", ".join(map(str, column_names)), ", ".join(rows)
        )


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

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        if self.closed:
            raise RuntimeError("this writer is already closed and no longer available")

        if "time" not in dataframe.columns:  # need time column for bulk import
            dataframe["time"] = int(time.time())

        fp = tempfile.NamedTemporaryFile(suffix=".csv")

        _cast_dtypes(dataframe)
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

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        params = None
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
                params = {"mode": "append"}
            elif if_exists == "overwrite":
                table.delete()
                table.create()
            else:
                raise ValueError("invalid valud for if_exists: {}".format(if_exists))
        else:
            table.create()

        session_name = "session-{}".format(int(time.time()))

        bulk_import = table.client.api_client.create_bulk_import(
            session_name, table.database, table.table, params=params
        )
        try:
            logger.info("uploading data converted into a CSV file")
            bulk_import.upload_file("part", "csv", csv.name)
            bulk_import.freeze()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError("failed to upload file: {}".format(e))

        logger.info("performing a bulk import job")
        job = bulk_import.perform(wait=True)

        if 0 < bulk_import.error_records:
            logger.warning(
                "[job id {}] detected {} error records.".format(
                    job.id, bulk_import.error_records
                )
            )

        if 0 < bulk_import.valid_records:
            logger.info(
                "[job id {}] imported {} records.".format(
                    job.id, bulk_import.valid_records
                )
            )
        else:
            raise RuntimeError(
                "[job id {}] no records have been imported: {}".format(
                    job.id, bulk_import.name
                )
            )
        bulk_import.commit(wait=True)
        bulk_import.delete()


class SparkWriter(Writer):
    """A writer module that loads Python data to Treasure Data.

    Parameters
    ----------
    td_spark_path : string, optional
        Path to td-spark-assembly_x.xx-x.x.x.jar. If not given, seek a path
        ``TDSparkContextBuilder.default_jar_path()`` by default.

    download_if_missing : boolean, default: True
        Download td-spark if it does not exist at the time of initialization.

    spark_configs : dict, optional
        Additional Spark configurations to be set via ``SparkConf``'s ``set`` method.
    """

    def __init__(
        self, td_spark_path=None, download_if_missing=True, spark_configs=None
    ):
        self.td_spark_path = td_spark_path
        self.download_if_missing = download_if_missing
        self.spark_configs = spark_configs

        self.td_spark = None
        self.fetched_apikey, self.fetched_endpoint = "", ""

    @property
    def closed(self):
        return self.td_spark is not None and self.td_spark.spark._jsc.sc().isStopped()

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
            self.td_spark = fetch_td_spark_context(
                table.client.apikey,
                table.client.endpoint,
                self.td_spark_path,
                self.download_if_missing,
                self.spark_configs,
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

        _cast_dtypes(dataframe)
        sdf = self.td_spark.spark.createDataFrame(dataframe)
        try:
            destination = "{}.{}".format(table.database, table.table)
            self.td_spark.write(sdf, destination, if_exists)
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
            self.td_spark.spark.stop()
