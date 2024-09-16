import abc
import gzip
import logging
import os
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

import msgpack
import numpy as np
import pandas as pd
from tdclient.util import normalized_msgpack

from .spark import fetch_td_spark_context

logger = logging.getLogger(__name__)


def _is_pd_na(x):
    is_na = pd.isna(x)
    return isinstance(is_na, bool) and is_na


def _is_np_nan(x):
    return isinstance(x, float) and np.isnan(x)


def _is_0d_ary(x):
    return isinstance(x, np.ndarray) and len(x.shape) == 0


def _is_0d_nan(x):
    return _is_0d_ary(x) and x.dtype.kind == "f" and np.isnan(x)


def _isnull(x):
    return x is None or _is_np_nan(x) or _is_pd_na(x)


def _isinstance_or_null(x, t):
    return _isnull(x) or isinstance(x, t)


def _replace_pd_na(dataframe):
    """Replace np.nan to None to avoid Int64 conversion issue"""
    if dataframe.isnull().any().any():
        dataframe.replace({np.nan: None}, inplace=True)


def _to_list(ary):
    # Return None if None, np.nan, or np.nan in 0-d array given
    if ary is None or _is_np_nan(ary) or _is_0d_nan(ary):
        return None

    # Return Python primitive value if 0-d array given
    if _is_0d_ary(ary):
        return ary.tolist()

    _ary = np.asarray(ary)
    # Replace numpy.nan to None which will be converted to NULL on TD
    kind = _ary.dtype.kind
    if kind == "f":
        _ary = np.where(np.isnan(_ary), None, _ary)
    elif kind == "U":
        _ary = np.where(_ary == "nan", None, _ary)
    elif kind == "O":
        _ary = np.array([None if _is_np_nan(x) or _is_pd_na(x) else x for x in _ary])
    return _ary.tolist()


def _convert_nullable_str(x, t, lower=False):
    v = str(x).lower() if lower else str(x)
    return v if isinstance(x, t) else None


def _cast_dtypes(dataframe, inplace=True, keep_list=False):
    """Convert dtypes into one of {int, float, str, list} type.

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

    for column, kind in dataframe.dtypes.apply(lambda dtype: dtype.kind).items():
        t = str
        if kind in ("i", "u"):
            t = "Int64" if df[column].isnull().any() else "int64"
        elif kind == "f":
            t = float
        elif kind in ("b", "O"):
            t = object
            if df[column].apply(_isinstance_or_null, args=((list, np.ndarray),)).all():
                if keep_list:
                    df[column] = df[column].apply(_to_list)
                else:
                    df[column] = df[column].apply(
                        _convert_nullable_str, args=((list, np.ndarray),)
                    )
            elif df[column].apply(_isinstance_or_null, args=(bool,)).all():
                # Bulk Import API internally handles boolean string as a boolean type,
                # and hence "True" ("False") will be stored as "true" ("false"). Align
                # to lower case here.
                df[column] = df[column].apply(
                    _convert_nullable_str, args=(bool,), lower=True
                )
            elif df[column].apply(_isinstance_or_null, args=(str,)).all():
                df[column] = df[column].apply(_convert_nullable_str, args=(str,))
            else:
                t = str
        df[column] = df[column].astype(t)

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
                f"column '{c}' has non-numeric. The values are stored as "
                "'varchar' type on Treasure Data."
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
        dataframe : :class:`pandas.DataFrame`
            Data loaded to a target table.

        table : :class:`pytd.table.Table`
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

        if isinstance(table, str):
            raise TypeError(f"table '{table}' should be pytd.table.Table, not str")

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
        table : :class:`pytd.table.Table`
            Target table.

        list_of_tuple : list of tuples
            Data loaded to a target table. Each element is a tuple that
            represents single table row.

        column_names : list of str
            Column names.

        column_types : list of str
            Column types corresponding to the names. Note that Treasure Data
            supports limited amount of types as documented in:
            https://docs.treasuredata.com/display/public/PD/Schema+Management

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """

        if table.exists:
            if if_exists == "error":
                raise RuntimeError(
                    f"target table '{table.database}.{table.table}' already exists"
                )
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                pass
            elif if_exists == "overwrite":
                table.delete()
                table.create(column_names, column_types)
            else:
                raise ValueError(f"invalid valud for if_exists: {if_exists}")
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
        database : str
            Target database name.

        table : str
            Target table name.

        list_of_tuple : list of tuples
            Data loaded to a target table. Each element is a tuple that
            represents single table row.

        column_names : list of str
            Column names.
        """
        rows = []
        for tpl in list_of_tuple:
            # InsertIntoWriter kicks Presto (Trino).
            # Following the list comprehension makes a single quote duplicated because
            # Presto allows users to escape a single quote with another single quote.
            # e.g. 'John Doe''s name' is converted to "John Doe's name" on Presto.
            list_of_value_strings = [
                (
                    f"""'{e.replace("'", "''")}'"""
                    if isinstance(e, str)
                    else ("null" if pd.isnull(e) else str(e))
                )
                for e in tpl
            ]
            rows.append(f"({', '.join(list_of_value_strings)})")

        return (
            f"INSERT INTO {database}.{table} "
            f"({', '.join(map(str, column_names))}) "
            f"VALUES {', '.join(rows)}"
        )


class BulkImportWriter(Writer):
    """A writer module that loads Python data to Treasure Data by using
    td-client-python's bulk importer.
    """

    def write_dataframe(
        self,
        dataframe,
        table,
        if_exists,
        fmt="csv",
        keep_list=False,
        max_workers=5,
        chunk_record_size=10_000,
    ):
        """Write a given DataFrame to a Treasure Data table.

        This method internally converts a given :class:`pandas.DataFrame` into a
        temporary CSV/msgpack file, and upload the file to Treasure Data via bulk
        import API.

        Note:
            If you pass a dataframe with ``Int64`` column, the column will be converted
            as ``varchar`` on Treasure Data schema due to BulkImport API restriction.

        Parameters
        ----------
        dataframe : :class:`pandas.DataFrame`
            Data loaded to a target table.

        table : :class:`pytd.table.Table`
            Target table.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.

        fmt : {'csv', 'msgpack'}, default: 'csv'
            Format for bulk_import.

            - csv
                Convert dataframe to temporary CSV file. Stable option but slower
                than msgpack option because pytd saves dataframe as temporary CSV file,
                then td-client converts it to msgpack.
                Types of columns are guessed by ``pandas.read_csv`` and it causes
                unintended type conversion e.g., 0-padded string ``"00012"`` into
                integer ``12``.
            - msgpack
                Convert to temporary msgpack.gz file. Fast option but there is a
                slight difference on type conversion compared to csv.

        keep_list : boolean, default: False
            If this argument is True, keep list or numpy.ndarray column as list, which
            will be converted array<T> on Treasure Data table.
            Each type of element of list will be converted by
            ``numpy.array(your_list).tolist()``.

            If True, ``fmt`` argument will be overwritten with ``msgpack``.

            Examples
            ---------

            A dataframe containing list will be treated array<T> in TD.

            >>> import pytd
            >>> import numpy as np
            >>> import pandas as pd
            >>> df = pd.DataFrame(
            ...     {
            ...         "a": [[1, 2, 3], [2, 3, 4]],
            ...         "b": [[0, None, 2], [2, 3, 4]],
            ...         "c": [np.array([1, np.nan, 3]), [2, 3, 4]]
            ...     }
            ... )
            >>> client = pytd.Client()
            >>> table = pytd.table.Table(client, "mydb", "test")
            >>> writer = pytd.writer.BulkImportWriter()
            >>> writer.write_dataframe(df, table, if_exists="overwrite", keep_list=True)

            In this case, the type of columns will be:
            ``{"a": array<int>, "b": array<string>, "c": array<string>}``

            If you want to set the type after ingestion, you need to run
            ``tdclient.Client.update_schema`` like:

            >>> client.api_client.update_schema(
            ...     "mydb",
            ...     "test",
            ...     [
            ...         ["a", "array<long>", "a"],
            ...         ["b", "array<int>", "b"],
            ...         ["c", "array<int>", "c"],
            ...     ],
            ... )

            Note that ``numpy.nan`` will be converted as a string value as ``"NaN"`` or
            ``"nan"``, so pytd will convert ``numpy.nan`` to ``None`` only when the
            dtype of a ndarray is `float`.
            Also, numpy converts integer array including ``numpy.nan`` into float array
            because ``numpy.nan`` is a Floating Point Special Value. See also:
            https://docs.scipy.org/doc/numpy-1.13.0/user/misc.html#ieee-754-floating-point-special-values

            Or, you can use :func:`Client.load_table_from_dataframe` function as well.

            >>> client.load_table_from_dataframe(df, "bulk_import", keep_list=True)

        max_workers : int, optional, default: 5
            The maximum number of threads that can be used to execute the given calls.
            This is used only when ``fmt`` is ``msgpack``.

        chunk_record_size : int, optional, default: 10_000
            The number of records to be written in a single file. This is used only when
            ``fmt`` is ``msgpack``.
        """
        if self.closed:
            raise RuntimeError("this writer is already closed and no longer available")

        if isinstance(table, str):
            raise TypeError(f"table '{table}' should be pytd.table.Table, not str")

        if "time" not in dataframe.columns:  # need time column for bulk import
            dataframe["time"] = int(time.time())

        # We enforce using "msgpack" format for list since CSV can't handle list.
        if keep_list:
            fmt = "msgpack"

        _cast_dtypes(dataframe, keep_list=keep_list)

        with ExitStack() as stack:
            fps = []
            if fmt == "csv":
                fp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
                stack.callback(os.unlink, fp.name)
                stack.callback(fp.close)
                dataframe.to_csv(fp.name)
                fps.append(fp)
            elif fmt == "msgpack":
                _replace_pd_na(dataframe)
                num_rows = len(dataframe)
                # chunk number of records should not exceed 200 to avoid OSError
                _chunk_record_size = max(chunk_record_size, num_rows//200)
                try:
                    for start in range(0, num_rows, _chunk_record_size):
                        records = dataframe.iloc[
                            start : start + _chunk_record_size
                        ].to_dict(orient="records")
                        fp = tempfile.NamedTemporaryFile(
                            suffix=".msgpack.gz", delete=False
                        )
                        fp = self._write_msgpack_stream(records, fp)
                        fps.append(fp)
                        stack.callback(os.unlink, fp.name)
                        stack.callback(fp.close)
                except OSError as e:
                    raise RuntimeError(
                        "failed to create a temporary file. "
                        "Larger chunk_record_size may mitigate the issue."
                    ) from e
            else:
                raise ValueError(
                    f"unsupported format '{fmt}' for bulk import. "
                    "should be 'csv' or 'msgpack'"
                )
            self._bulk_import(table, fps, if_exists, fmt, max_workers=max_workers)
            stack.close()

    def _bulk_import(self, table, file_likes, if_exists, fmt="csv", max_workers=5):
        """Write a specified CSV file to a Treasure Data table.

        This method uploads the file to Treasure Data via bulk import API.

        Parameters
        ----------
        table : :class:`pytd.table.Table`
            Target table.

        file_likes : List of file like objects
            Data in this file will be loaded to a target table.

        if_exists : str, {'error', 'overwrite', 'append', 'ignore'}
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.

        fmt : str, optional, {'csv', 'msgpack'}, default: 'csv'
            File format for bulk import. See also :func:`write_dataframe`

        max_workers : int, optional, default: 5
            The maximum number of threads that can be used to execute the given calls.
            This is used only when ``fmt`` is ``msgpack``.
        """
        params = None
        if table.exists:
            if if_exists == "error":
                raise RuntimeError(
                    f"target table '{table.database}.{table.table}' already exists"
                )
            elif if_exists == "ignore":
                return
            elif if_exists == "append":
                params = {"mode": "append"}
            elif if_exists == "overwrite":
                table.delete()
                table.create()
            else:
                raise ValueError(f"invalid value for if_exists: {if_exists}")
        else:
            table.create()

        session_name = f"session-{uuid.uuid1()}"

        bulk_import = table.client.api_client.create_bulk_import(
            session_name, table.database, table.table, params=params
        )
        s_time = time.time()
        try:
            logger.info(f"uploading data converted into a {fmt} file")
            if fmt == "msgpack":
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for i, fp in enumerate(file_likes):
                        fsize = fp.tell()
                        fp.seek(0)
                        executor.submit(
                            bulk_import.upload_part,
                            f"part-{i}",
                            fp,
                            fsize,
                        )
                        logger.debug(f"to upload {fp.name} to TD. File size: {fsize}B")
            else:
                fp = file_likes[0]
                bulk_import.upload_file("part", fmt, fp)
            bulk_import.freeze()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError(f"failed to upload file: {e}")

        logger.debug(f"uploaded data in {time.time() - s_time:.2f} sec")

        logger.info("performing a bulk import job")
        job = bulk_import.perform(wait=True)

        if 0 < bulk_import.error_records:
            logger.warning(
                f"[job id {job.id}] detected {bulk_import.error_records} error records."
            )

        if 0 < bulk_import.valid_records:
            logger.info(
                f"[job id {job.id}] imported {bulk_import.valid_records} records."
            )
        else:
            raise RuntimeError(
                f"[job id {job.id}] no records have been imported: {bulk_import.name}"
            )
        bulk_import.commit(wait=True)
        bulk_import.delete()

    def _write_msgpack_stream(self, items, stream):
        """Write MessagePack stream

        Parameters
        ----------
        items : list of dict
            Same format with dataframe.to_dict(orient="records")
            Examples:
                ``[{"time": 12345, "col1": "foo"}, {"time": 12345, "col1": "bar"}]``
        stream : File like object
            Target file like object which has `write()` function. This object will be
            updated in this function.
        """

        with gzip.GzipFile(mode="wb", fileobj=stream) as gz:
            packer = msgpack.Packer()
            for item in items:
                try:
                    mp = packer.pack(item)
                except (OverflowError, ValueError):
                    packer.reset()
                    mp = packer.pack(normalized_msgpack(item))
                gz.write(mp)

        logger.debug(
            f"created a msgpack file: {stream.name}. File size: {stream.tell()}"
        )
        return stream


class SparkWriter(Writer):
    """A writer module that loads Python data to Treasure Data.

    Parameters
    ----------
    td_spark_path : str, optional
        Path to td-spark-assembly-{td-spark-version}_spark{spark-version}.jar.
        If not given, seek a path ``TDSparkContextBuilder.default_jar_path()``
        by default.

    download_if_missing : bool, default: True
        Download td-spark if it does not exist at the time of initialization.

    spark_configs : dict, optional
        Additional Spark configurations to be set via ``SparkConf``'s ``set`` method.

    Attributes
    ----------
    td_spark_path : str
        Path to td-spark-assembly-{td-spark-version}_spark{spark-version}.jar.

    download_if_missing : bool
        Download td-spark if it does not exist at the time of initialization.

    spark_configs : dict
        Additional Spark configurations to be set via ``SparkConf``'s ``set`` method.

    td_spark : :class:`td_pyspark.TDSparkContext`
        Connection of td-spark
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

        This method internally converts a given :class:`pandas.DataFrame` into Spark
        DataFrame, and directly writes to Treasure Data's main storage
        so-called Plazma through a PySpark session.

        Parameters
        ----------
        dataframe : :class:`pandas.DataFrame`
            Data loaded to a target table.

        table : :class:`pytd.table.Table`
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
            raise ValueError(f"invalid value for if_exists: {if_exists}")

        if isinstance(table, str):
            raise TypeError(f"table '{table}' should be pytd.table.Table, not str")

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
        _replace_pd_na(dataframe)

        sdf = self.td_spark.spark.createDataFrame(dataframe)
        try:
            destination = f"{table.database}.{table.table}"
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
        """Close a PySpark session connected to Treasure Data."""
        if self.td_spark is not None:
            self.td_spark.spark.stop()
