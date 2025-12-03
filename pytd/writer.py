import abc
import gzip
import logging
import math
import os
import tempfile
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, BinaryIO, Literal

import msgpack
import numpy as np
import pandas as pd
from tdclient.util import normalized_msgpack
from tqdm import tqdm

from .spark import fetch_td_spark_context  # type: ignore[misc]

if TYPE_CHECKING:
    from .table import Table

logger = logging.getLogger(__name__)


def _is_pd_na(x: Any) -> bool:
    is_na = pd.isna(x)  # type: ignore[misc]
    return isinstance(is_na, bool) and is_na


def _is_np_nan(x: Any) -> bool:
    return isinstance(x, float) and np.isnan(x)


def _is_0d_ary(x: Any) -> bool:
    return isinstance(x, np.ndarray) and len(x.shape) == 0  # type: ignore[misc, arg-type]


def _is_0d_nan(x: Any) -> bool:
    return _is_0d_ary(x) and x.dtype.kind == "f" and np.isnan(x)


def _isnull(x: Any) -> bool:
    return x is None or _is_np_nan(x) or _is_pd_na(x)


def _isinstance_or_null(x: Any, t: type | tuple[type, ...]) -> bool:
    return _isnull(x) or isinstance(x, t)


def _replace_pd_na(dataframe: pd.DataFrame) -> None:
    """Replace np.nan and pd.NA to None to avoid Int64 conversion issue"""
    if dataframe.isnull().any().any():  # type: ignore[misc]
        # Replace both np.nan and pd.NA with None
        replace_dict = {np.nan: None, pd.NA: None}
        dataframe.replace(replace_dict, inplace=True)


def _to_list(ary: Any) -> list[Any] | None:
    # Return None if None, np.nan, pd.NA, or np.nan in 0-d array given
    if ary is None or _is_np_nan(ary) or _is_0d_nan(ary) or _is_pd_na(ary):
        return None

    # Return Python primitive value if 0-d array given
    if _is_0d_ary(ary):
        return ary.tolist()

    _ary = np.asarray(ary)
    # Replace numpy.nan to None which will be converted to NULL on TD
    kind = _ary.dtype.kind
    if kind == "f":
        _ary = np.where(np.isnan(_ary), None, _ary)  # type: ignore[arg-type]
    elif kind == "U":
        _ary = np.where(_ary == "nan", None, _ary)  # type: ignore[arg-type]
    elif kind == "O":
        _ary = np.array([None if _is_np_nan(x) or _is_pd_na(x) else x for x in _ary])
    return _ary.tolist()


def _convert_nullable_str(x: Any, t: type, lower: bool = False) -> str | None:
    v = str(x).lower() if lower else str(x)
    return v if isinstance(x, t) else None


def _cast_dtypes(
    dataframe: pd.DataFrame, inplace: bool = True, keep_list: bool = False
) -> pd.DataFrame | None:
    """Convert dtypes into one of {int, float, str, list} type.

    A character code (one of 'biufcmMOSUV') identifying the general kind of data.

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

    for column, kind in dataframe.dtypes.apply(lambda dtype: dtype.kind).items():  # type: ignore[misc]
        t = str
        if kind in ("i", "u"):
            t = "Int64" if df[column].isnull().any() else "int64"  # type: ignore[misc]
        elif kind == "f":
            t = "Float64" if df[column].isnull().any() else "float64"  # type: ignore[misc]
        elif kind in ("b", "O"):
            t = object
            if df[column].apply(_isinstance_or_null, args=((list, np.ndarray),)).all():  # type: ignore[misc]
                if keep_list:
                    df[column] = df[column].apply(_to_list)  # type: ignore[misc]
                else:
                    df[column] = df[column].apply(  # type: ignore[misc]
                        _convert_nullable_str, args=((list, np.ndarray),)
                    )
            elif df[column].apply(_isinstance_or_null, args=(bool,)).all():  # type: ignore[misc]
                # Bulk Import API internally handles boolean string as a boolean type,
                # and hence "True" ("False") will be stored as "true" ("false"). Align
                # to lower case here.
                df[column] = df[column].apply(  # type: ignore[misc]
                    _convert_nullable_str, args=(bool,), lower=True
                )
            elif df[column].apply(_isinstance_or_null, args=(str,)).all():  # type: ignore[misc]
                df[column] = df[column].apply(_convert_nullable_str, args=(str,))  # type: ignore[misc]
            else:
                t = str
        df[column] = df[column].astype(t)  # type: ignore[misc]

    if not inplace:
        return df
    return None


def _get_schema(dataframe: pd.DataFrame) -> tuple[list[str], list[str]]:
    column_names, column_types = [], []
    for c, t in zip(dataframe.columns, dataframe.dtypes, strict=False):  # type: ignore[misc]
        # Compare nullable integer type by using pandas function because `t ==
        # "Int64"` causes a following warning for some reasons:
        #     DeprecationWarning: Numeric-style type codes are deprecated and
        #     will result in an error in the future.
        dtype_str = str(t)  # type: ignore[misc]
        if (
            t == "int64"
            or (hasattr(pd, "Int64Dtype") and isinstance(t, pd.Int64Dtype))
            or dtype_str in ["Int64", "Int32", "Int16", "Int8"]
            or (hasattr(pd, "Int32Dtype") and isinstance(t, pd.Int32Dtype))
            or (hasattr(pd, "Int16Dtype") and isinstance(t, pd.Int16Dtype))
            or (hasattr(pd, "Int8Dtype") and isinstance(t, pd.Int8Dtype))
        ):
            presto_type = "bigint"
        elif (
            t == "float64"
            or (hasattr(pd, "Float64Dtype") and isinstance(t, pd.Float64Dtype))
            or dtype_str in ["Float64", "Float32"]
            or (hasattr(pd, "Float32Dtype") and isinstance(t, pd.Float32Dtype))
        ):
            presto_type = "double"
        else:
            presto_type = "varchar"
            logger.info(
                f"column '{c}' has non-numeric. The values are stored as "
                "'varchar' type on Treasure Data."
            )
        column_names.append(c)  # type: ignore[misc]
        column_types.append(presto_type)  # type: ignore[misc]
    return column_names, column_types  # type: ignore[return-value]


class Writer(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        self._closed: bool = False

    @property
    def closed(self) -> bool:
        return self._closed

    @abc.abstractmethod
    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table: "Table",
        if_exists: Literal["error", "overwrite", "append", "ignore"],
    ) -> None:
        pass

    def close(self) -> None:
        self._closed = True

    @staticmethod
    def from_string(
        writer: Literal["bulk_import", "insert_into", "spark"], **kwargs: Any
    ) -> "Writer":
        writer_lower = writer.lower()
        if writer_lower == "bulk_import":
            return BulkImportWriter()
        elif writer_lower == "insert_into":
            return InsertIntoWriter()
        elif writer_lower == "spark":
            return SparkWriter(**kwargs)
        else:
            raise ValueError("unknown way to upload data to TD is specified")


class InsertIntoWriter(Writer):
    """A writer module that loads Python data to Treasure Data by issueing
    INSERT INTO query in Presto.
    """

    def _format_value_for_trino(self, value: Any) -> str:
        """Convert a value to appropriate string format for use in Presto queries.

        Parameters
        ----------
        value : any
            The value to convert

        Returns
        -------
        str
            String representation usable in Presto queries
        """
        # Handle string values with quote escaping
        if isinstance(value, str):
            return f"""'{value.replace("'", "''")}'"""

        # Detect infinity (excluding nan)
        if (
            isinstance(value, int | float | np.number)
            and not pd.isnull(value)  # type: ignore[misc, arg-type]
            and math.isinf(value)  # type: ignore[arg-type]
        ):
            if value > 0:
                return "infinity()"
            else:
                return "-infinity()"

        # Detect nan specifically
        if isinstance(value, int | float | np.number) and math.isnan(value):  # type: ignore[arg-type]
            return "nan()"

        # Handle other null values (pd.NA, pd.NaT, None)
        if pd.isnull(value):  # type: ignore[misc, arg-type]
            return "null"

        # Handle other numeric values
        return str(value)  # type: ignore[arg-type]

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table: "Table",
        if_exists: Literal["error", "overwrite", "append", "ignore"],
    ) -> None:
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

    def _insert_into(
        self,
        table: "Table",
        list_of_tuple: list[tuple[Any, ...]],
        column_names: list[str],
        column_types: list[str],
        if_exists: Literal["error", "overwrite", "append", "ignore"],
    ) -> None:
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
            https://api-docs.treasuredata.com/en/tools/presto/sql_tips_for_hive_and_presto#treasure-data-native-data-types

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

    def _build_query(
        self,
        database: str,
        table: str,
        list_of_tuple: list[tuple[Any, ...]],
        column_names: list[str],
    ) -> str:
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
        rows: list[str] = []
        for tpl in list_of_tuple:
            list_of_value_strings = [self._format_value_for_trino(e) for e in tpl]  # type: ignore[misc]
            rows.append(f"({', '.join(list_of_value_strings)})")  # type: ignore[misc]

        return (
            f"INSERT INTO {database}.{table} "
            f"({', '.join(map(str, column_names))}) "  # type: ignore[arg-type,misc]
            f"VALUES {', '.join(rows)}"
        )


class BulkImportWriter(Writer):
    """A writer module that loads Python data to Treasure Data by using
    td-client-python's bulk importer.
    """

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table: "Table",
        if_exists: Literal["error", "overwrite", "append", "ignore"],
        fmt: Literal["csv", "msgpack"] = "csv",
        keep_list: bool = False,
        max_workers: int = 5,
        chunk_record_size: int = 10_000,
        show_progress: bool = False,
        bulk_import_name: str | None = None,
        commit_timeout: int | None = None,
        perform_timeout: int | None = None,
        perform_wait_callback: Callable[..., Any] | None = None,
    ) -> None:
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


        show_progress : boolean, default: False
            If this argument is True, shows a TQDM progress bar
            for chunking data into msgpack format and uploading before
            performing a bulk import.

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

        bulk_import_name : str, optional, default: None
            Custom name for the bulk import job. If not provided, a UUID-based
            name will be automatically generated.

        commit_timeout : int, optional, default: None
            Timeout in seconds for the bulk import commit operation. If None,
            no timeout is applied.

        perform_timeout : int, optional, default: None
            Timeout in seconds for the bulk import perform operation. If None,
            no timeout is applied.

        perform_wait_callback : callable, optional, default: None
            A callable to be called on every tick of wait interval during
            bulk import job execution.
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
            fps: list[BinaryIO] = []
            if fmt == "csv":
                fp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
                stack.callback(os.unlink, fp.name)
                stack.callback(fp.close)
                dataframe.to_csv(fp.name)  # type: ignore[misc]
                fps.append(fp)  # type: ignore[misc]
            elif fmt == "msgpack":
                _replace_pd_na(dataframe)
                num_rows = len(dataframe)
                # chunk number of records should not exceed 200 to avoid OSError
                _chunk_record_size = max(chunk_record_size, num_rows // 200)
                try:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = []
                        chunk_range = (
                            tqdm(
                                range(0, num_rows, _chunk_record_size),
                                desc="Chunking data",
                            )
                            if show_progress
                            else range(0, num_rows, _chunk_record_size)
                        )
                        for start in chunk_range:
                            records = dataframe.iloc[  # type: ignore[misc]
                                start : start + _chunk_record_size
                            ].to_dict(orient="records")  # type: ignore[misc]
                            fp = tempfile.NamedTemporaryFile(
                                suffix=".msgpack.gz", delete=False
                            )
                            futures.append(  # type: ignore[misc]
                                (
                                    start,
                                    executor.submit(
                                        self._write_msgpack_stream,
                                        records,  # type: ignore[misc]
                                        fp,  # type: ignore[misc, arg-type]
                                    ),
                                )
                            )
                            stack.callback(os.unlink, fp.name)
                            stack.callback(fp.close)
                        resolve_range = (  # type: ignore[misc]
                            tqdm(sorted(futures), desc="Resolving futures")  # type: ignore[arg-type]
                            if show_progress
                            else sorted(futures)  # type: ignore[arg-type]
                        )
                        for _start, future in resolve_range:  # type: ignore[misc]
                            fps.append(future.result())  # type: ignore[arg-type, misc]
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
            self._bulk_import(  # type: ignore[misc]
                table,
                fps,
                if_exists,
                fmt,
                max_workers=max_workers,
                show_progress=show_progress,
                bulk_import_name=bulk_import_name,
                commit_timeout=commit_timeout,
                perform_timeout=perform_timeout,
                perform_wait_callback=perform_wait_callback,
            )
            stack.close()

    def _bulk_import(
        self,
        table: "Table",
        file_likes: list[BinaryIO],
        if_exists: Literal["error", "overwrite", "append", "ignore"],
        fmt: Literal["csv", "msgpack"] = "csv",
        max_workers: int = 5,
        show_progress: bool = False,
        bulk_import_name: str | None = None,
        commit_timeout: int | None = None,
        perform_timeout: int | None = None,
        perform_wait_callback: Callable[..., Any] | None = None,
    ) -> None:
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

        show_progress : boolean, default: False
            If this argument is True, shows a TQDM progress bar
            for the upload process performed on multiple threads.

        bulk_import_name : str, optional, default: None
            Custom name for the bulk import job. If not provided, a UUID-based
            name will be automatically generated.

        commit_timeout : int, optional, default: None
            Timeout in seconds for the bulk import commit operation. If None,
            no timeout is applied.

        perform_timeout : int, optional, default: None
            Timeout in seconds for the bulk import perform operation. If None,
            no timeout is applied.

        perform_wait_callback : callable, optional, default: None
            A callable to be called on every tick of wait interval during
            bulk import job execution.
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

        if bulk_import_name is None:
            bulk_import_name = f"session-{uuid.uuid1()}"

        logger.info(f"creating bulk import session: {bulk_import_name}")

        bulk_import = table.client.api_client.create_bulk_import(
            bulk_import_name,
            table.database,
            table.table,
            params=params,  # type: ignore[arg-type]
        )
        s_time = time.time()
        try:
            logger.info(f"uploading data converted into a {fmt} file")
            if fmt == "msgpack":
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for i, fp in enumerate(file_likes):
                        fsize = fp.tell()
                        fp.seek(0)
                        futures.append(  # type: ignore[misc]
                            executor.submit(
                                bulk_import.upload_part,
                                f"part-{i}",
                                fp,
                                fsize,
                            )
                        )
                        logger.debug(f"to upload {fp.name} to TD. File size: {fsize}B")
                    if show_progress:
                        for _ in tqdm(futures, desc="Uploading parts"):  # type: ignore[misc]
                            _.result()  # type: ignore[misc]
                    else:
                        for future in futures:  # type: ignore[misc]
                            future.result()  # type: ignore[misc]
            else:
                fp = file_likes[0]
                bulk_import.upload_file("part", fmt, fp)
            bulk_import.freeze()
        except Exception as e:
            bulk_import.delete()
            raise RuntimeError(f"failed to upload file: {e}") from e

        logger.debug(f"uploaded data in {time.time() - s_time:.2f} sec")

        logger.info("performing a bulk import job")
        job = bulk_import.perform(
            wait=True, timeout=perform_timeout, wait_callback=perform_wait_callback
        )

        if bulk_import.error_records and bulk_import.error_records > 0:
            logger.warning(
                f"[job id {job.id}] detected {bulk_import.error_records} error records."
            )

        if bulk_import.valid_records and bulk_import.valid_records > 0:
            logger.info(
                f"[job id {job.id}] imported {bulk_import.valid_records} records."
            )
        else:
            raise RuntimeError(
                f"[job id {job.id}] no records have been imported: {bulk_import.name}"
            )
        bulk_import.commit(wait=True, timeout=commit_timeout)
        bulk_import.delete()

    def _write_msgpack_stream(
        self, items: list[dict[str, Any]], stream: BinaryIO
    ) -> BinaryIO:
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
        self,
        td_spark_path: str | None = None,
        download_if_missing: bool = True,
        spark_configs: dict[str, Any] | None = None,
    ) -> None:
        self.td_spark_path = td_spark_path
        self.download_if_missing = download_if_missing
        self.spark_configs = spark_configs

        self.td_spark = None
        self.fetched_apikey, self.fetched_endpoint = "", ""

    @property
    def closed(self) -> bool:
        return self.td_spark is not None and self.td_spark.spark._jsc.sc().isStopped()  # type: ignore[attr-defined, union-attr, misc]

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table: "Table",
        if_exists: Literal["error", "overwrite", "append", "ignore"],
    ) -> None:
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

        if self.td_spark is None:  # type: ignore[misc]
            self.td_spark = fetch_td_spark_context(  # type: ignore[misc,assignment]
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

        from py4j.protocol import Py4JJavaError  # type: ignore[import]

        _cast_dtypes(dataframe)
        _replace_pd_na(dataframe)

        sdf = self.td_spark.spark.createDataFrame(dataframe)  # type: ignore[misc, union-attr]
        try:
            destination = f"{table.database}.{table.table}"
            self.td_spark.write(sdf, destination, if_exists)  # type: ignore[misc, union-attr]
        except Py4JJavaError as e:  # type: ignore[misc]
            if "API_ACCESS_FAILURE" in str(e.java_exception):  # type: ignore[misc]
                raise PermissionError(
                    "failed to access to Treasure Data Plazma API."
                    "Contact customer support to enable access rights."
                ) from e
            raise RuntimeError(
                "failed to load table via td-spark: " + str(e.java_exception)  # type: ignore[misc]
            ) from e

    def close(self) -> None:
        """Close a PySpark session connected to Treasure Data."""
        if self.td_spark is not None:  # type: ignore[misc]
            self.td_spark.spark.stop()  # type: ignore[misc, union-attr]
