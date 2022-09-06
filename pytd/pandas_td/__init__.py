import datetime
import logging
import re
import time

import pandas as pd

from ..client import Client
from ..query_engine import HiveQueryEngine, PrestoQueryEngine

logger = logging.getLogger(__name__)


def connect(apikey=None, endpoint=None, **kwargs):
    """Create a connection to Treasure Data

    Parameters
    ----------
    apikey : str, optional
        Treasure Data API key. If not given, a value of environment variable
        ``TD_API_KEY`` is used by default.

    endpoint : str, optional
        Treasure Data API server. If not given, ``https://api.treasuredata.com`` is
        used by default. List of available endpoints is:
        https://docs.treasuredata.com/display/public/PD/Sites+and+Endpoints

    kwargs : dict, optional
        Optional arguments

    Returns
    -------
    :class:`pytd.Client`
    """
    return Client(apikey=apikey, endpoint=endpoint, **kwargs)


RE_ENGINE_DESC = re.compile(
    r"(?P<type>presto|hive)://(?P<apikey>[0-9]+/[a-z0-9]+)@"
    r"(?P<host>[^/]+)/(?P<database>[a-z0-9_]+)(\?.*)?"
)
RE_ENGINE_DESC_SHORT = re.compile(
    r"(?P<type>presto|hive):(?P<database>[a-z0-9_]+)(\?.*)?"
)


def create_engine(url, con=None, header=True, show_progress=5.0, clear_progress=True):
    """Create a handler for query engine based on a URL.

    The following environment variables are used for default connection:

      TD_API_KEY     API key
      TD_API_SERVER  API server (default: ``https://api.treasuredata.com``)

    Parameters
    ----------
    url : str
        Engine descriptor in the form "type://apikey@host/database?params..."
        Use shorthand notation "type:database?params..." for the default connection.
        pytd: "params" will be ignored since pytd.QueryEngine does not have any
        extra parameters.

    con : :class:`pytd.Client`, optional
        Handler returned by :meth:`pytd.pandas_td.connect`. If not given, default client
        is used.

    header : str or bool, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.

    show_progress : double or bool, default: 5.0
        Number of seconds to wait before printing progress.
        Set False to disable progress entirely.
        pytd: This argument will be ignored.

    clear_progress : bool, default: True
        If True, clear progress when query completed.
        pytd: This argument will be ignored.

    Returns
    -------
    :class:`pytd.query_engine.QueryEngine`


    Examples
    --------

    >>> import pytd.pandas_td as td
    >>> con = td.connect(apikey=apikey, endpoint="https://api.treasuredata.com")
    >>> engine = td.create_engine("presto:sample_datasets")
    """

    apikey, endpoint = None, None

    res = RE_ENGINE_DESC.search(url)
    if res is not None:
        engine_type = res.group("type")
        apikey = res.group("apikey")
        host = res.group("host")
        database = res.group("database")

        endpoint = f"https://{host}/"
    else:
        res = RE_ENGINE_DESC_SHORT.search(url)
        if res is not None:
            engine_type = res.group("type")
            database = res.group("database")
        else:
            raise ValueError("invalid engine descriptor format")

    if con is None:
        con = connect(apikey=apikey, endpoint=endpoint)
    apikey, endpoint = con.apikey, con.endpoint

    if engine_type == "presto":
        return PrestoQueryEngine(apikey, endpoint, database, header=header)
    return HiveQueryEngine(apikey, endpoint, database, header=header)


def read_td_query(
    query, engine, index_col=None, parse_dates=None, distributed_join=False, params=None
):
    """Read Treasure Data query into a DataFrame.

    Returns a DataFrame corresponding to the result set of the query string.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    While Presto in pytd has two options to issue a query, by either
    ``tdclient`` or ``prestodb``, pytd.pandas_td#read_td_query always uses the
    former to be compatible with the original pandas-td. Use
    :class:`pytd.Client` to take advantage of the latter option.

    Parameters
    ----------
    query : str
        Query string to be executed.

    engine : :class:`pytd.query_engine.QueryEngine`
        Handler returned by create_engine.

    index_col : str, optional
        Column name to use as index for the returned DataFrame object.

    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    distributed_join : bool, default: `False`
        (Presto only) If True, distributed join is enabled. If False, broadcast join is
        used.
        See https://trino.io/docs/current/admin/properties-general.html

    params : dict, optional
        Parameters to pass to execute method. pytd does not support parameter
        ``type`` ('hive', 'presto'), and query type needs to be defined by
        ``engine``.

        Available parameters:

        - ``db`` (str): use the database
        - ``result_url`` (str): result output URL
        - ``priority`` (int or str): priority

            - -2: "VERY LOW"
            - -1: "LOW"
            -  0: "NORMAL"
            -  1: "HIGH"
            -  2: "VERY HIGH"
        - ``retry_limit`` (int): max number of automatic retries
        - ``wait_interval`` (int): sleep interval until job finish
        - ``wait_callback`` (function): called every interval against job itself
        - ``engine_version`` (str): run query with Hive 2 if this parameter is
          set to ``"stable"`` in ``HiveQueryEngine``.
          https://docs.treasuredata.com/display/public/PD/Writing+Hive+Queries

    Returns
    -------
    :class:`pandas.DataFrame`
        Query result in a DataFrame
    """
    if params is None:
        params = {}
    params["force_tdclient"] = True

    if isinstance(engine, PrestoQueryEngine) and distributed_join is not None:
        header = engine.create_header(
            [
                "read_td_query",
                "set session join_distribution_type = "
                f"'{'PARTITIONED' if distributed_join else 'BROADCAST'}'\n",
            ]
        )
    else:
        header = engine.create_header("read_td_query")

    return _to_dataframe(
        engine.execute(header + query, **params), index_col, parse_dates
    )


def read_td_job(job_id, engine, index_col=None, parse_dates=None):
    """Read Treasure Data job result into a DataFrame.

    Returns a DataFrame corresponding to the result set of the job.
    This method waits for job completion if the specified job is still running.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    job_id : int
        Job ID.

    engine : :class:`pytd.query_engine.QueryEngine`
        Handler returned by create_engine.

    index_col : str, optional
        Column name to use as index for the returned DataFrame object.

    parse_dates : list or dict, optional

        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    Returns
    -------
    :class:`pandas.DataFrame`
        Job result in a dataframe
    """
    con = connect(default_engine=engine)

    # get job
    job = con.get_job(job_id)

    job.wait()

    if not job.success():
        if job.debug and job.debug["stderr"]:
            logger.error(job.debug["stderr"])
        raise RuntimeError(f"job {job.job_id} {job.status()}")

    if not job.finished():
        job.wait()

    columns = [c[0] for c in job.result_schema]
    rows = job.result()

    return _to_dataframe({"data": rows, "columns": columns}, index_col, parse_dates)


def read_td_table(
    table_name,
    engine,
    index_col=None,
    parse_dates=None,
    columns=None,
    time_range=None,
    limit=10000,
):
    """Read Treasure Data table into a DataFrame.

    The number of returned rows is limited by "limit" (default 10,000).
    Setting limit=None means all rows. Be careful when you set limit=None
    because your table might be very large and the result does not fit into memory.

    Parameters
    ----------
    table_name : str
        Name of Treasure Data table in database.

    engine : :class:`pytd.query_engine.QueryEngine`
        Handler returned by create_engine.

    index_col : str, optional
        Column name to use as index for the returned DataFrame object.

    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    columns : list, optional
        List of column names to select from table.

    time_range : tuple (start, end), optional
        Limit time range to select. "start" and "end" are one of None, integers,
        strings or datetime objects. "end" is exclusive, not included in the result.

    limit : int, default: 10,000
        Maximum number of rows to select.

    Returns
    -------
    :class:`pandas.DataFrame`
    """
    # header
    query = engine.create_header(f"read_td_table('{table_name}')")
    # SELECT
    query += f"SELECT {'*' if columns is None else ', '.join(columns)}\n"
    # FROM
    query += f"FROM {table_name}\n"
    # WHERE
    if time_range is not None:
        start, end = time_range
        query += (
            "WHERE td_time_range("
            f"time, {_convert_time(start)}, {_convert_time(end)})\n"
        )
    # LIMIT
    if limit is not None:
        query += f"LIMIT {limit}\n"
    # execute
    return _to_dataframe(engine.execute(query), index_col, parse_dates)


def _convert_time(time):
    if time is None:
        return "NULL"
    elif isinstance(time, int):
        t = pd.to_datetime(time, unit="s")
    elif isinstance(time, str):
        t = pd.to_datetime(time)
    elif isinstance(time, (datetime.date, datetime.datetime)):
        t = pd.to_datetime(time)
    else:
        raise ValueError(f"invalid time value: {time}")
    return f"'{t.replace(microsecond=0)}'"


def _to_dataframe(dic, index_col, parse_dates):
    frame = pd.DataFrame(**dic)
    if parse_dates is not None:
        frame = _parse_dates(frame, parse_dates)
    if index_col is not None:
        frame.set_index(index_col, inplace=True)
    return frame


def _parse_dates(frame, parse_dates):
    for name in parse_dates:
        if type(parse_dates) is list:
            frame[name] = pd.to_datetime(frame[name])
        else:
            if frame[name].dtype.kind == "O":
                frame[name] = pd.to_datetime(frame[name], format=parse_dates[name])
            else:
                frame[name] = pd.to_datetime(frame[name], unit=parse_dates[name])
    return frame


# alias
read_td = read_td_query


def to_td(
    frame,
    name,
    con,
    if_exists="fail",
    time_col=None,
    time_index=None,
    index=True,
    index_label=None,
    chunksize=10000,
    date_format=None,
    writer="bulk_import",
    **kwargs,
):
    """Write a DataFrame to a Treasure Data table.

    This method converts the dataframe into a series of key-value pairs
    and send them using the Treasure Data streaming API. The data is divided
    into chunks of rows (default 10,000) and uploaded separately. If upload
    failed, the client retries the process for a certain amount of time
    (max_cumul_retry_delay; default 600 secs). This method may fail and
    raise an exception when retries did not success, in which case the data
    may be partially inserted. Use the bulk import utility if you cannot
    accept partial inserts.

    Parameters
    ----------
    frame : :class:`pandas.DataFrame`
        DataFrame to be written.

    name : str
        Name of table to be written, in the form 'database.table'.

    con : :class:`pytd.Client`
        A client for a Treasure Data account returned by :meth:`pytd.pandas_td.connect`.

    if_exists : str, {'error' ('fail'), 'overwrite' ('replace'), 'append', 'ignore'}, \
                    default: 'error'
        What happens when a target table already exists. For pandas-td
        compatibility, 'error', 'overwrite', 'append', 'ignore' can
        respectively be:

        - fail: If table exists, raise an exception.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.
        - ignore: If table exists, do nothing.

    time_col : str, optional
        Column name to use as "time" column for the table. Column type must be
        integer (unixtime), datetime, or string. If None is given (default),
        then the current time is used as time values.

    time_index : int, optional
        Level of index to use as "time" column for the table. Set 0 for a single index.
        This parameter implies index=False.

    index : bool, default: True
        Write DataFrame index as a column.

    index_label : str or sequence, default: None
        Column label for index column(s). If None is given (default) and index is True,
        then the index names are used. A sequence should be given if the DataFrame uses
        MultiIndex.

    chunksize : int, default: 10,000
        Number of rows to be inserted in each chunk from the dataframe.
        pytd: This argument will be ignored.

    date_format : str, default: None
        Format string for datetime objects

    writer : str, {'bulk_import', 'insert_into', 'spark'}, or \
                :class:`pytd.writer.Writer`, default: 'bulk_import'
        A Writer to choose writing method to Treasure Data. If not given or
        string value, a temporal Writer instance will be created.

    fmt : str, {'csv', 'msgpack'}, default: 'csv'
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
    """
    if if_exists == "fail" or if_exists == "error":
        mode = "error"
    elif if_exists == "replace" or if_exists == "overwrite":
        mode = "overwrite"
    elif if_exists == "append":
        mode = "append"
    elif if_exists == "ignore":
        mode = "ignore"
    else:
        raise ValueError(f"invalid value for if_exists: {if_exists}")

    # convert
    frame = frame.copy()
    frame = _convert_time_column(frame, time_col, time_index)
    frame = _convert_index_column(frame, index, index_label)
    frame = _convert_date_format(frame, date_format)

    database, table = name.split(".")
    con.get_table(database, table).import_dataframe(frame, writer, mode, **kwargs)


def _convert_time_column(frame, time_col=None, time_index=None):
    if time_col is not None and time_index is not None:
        raise ValueError("time_col and time_index cannot be used at the same time")
    if "time" in frame.columns and time_col != "time":
        raise ValueError('"time" column already exists')
    if time_col is not None:
        # Use 'time_col' as time column
        if time_col != "time":
            frame.rename(columns={time_col: "time"}, inplace=True)
        col = frame["time"]
        # convert python string to pandas datetime
        if col.dtype.name == "object" and len(col) > 0 and isinstance(col[0], str):
            col = pd.to_datetime(col)
        # convert pandas datetime to unixtime
        if col.dtype.name == "datetime64[ns]":
            frame["time"] = col.astype("int64") // (10**9)
    elif time_index is not None:
        # Use 'time_index' as time column
        if type(time_index) is bool or not isinstance(time_index, int):
            raise TypeError("invalid type for time_index")
        if isinstance(frame.index, pd.MultiIndex):
            idx = frame.index.levels[time_index]
        else:
            if time_index == 0:
                idx = frame.index
            else:
                raise IndexError("list index out of range")
        if idx.dtype.name != "datetime64[ns]":
            raise TypeError("index type must be datetime64[ns]")
        # convert pandas datetime to unixtime
        frame["time"] = idx.astype("int64") // (10**9)
    else:
        # Use current time as time column
        frame["time"] = int(time.time())
    return frame


def _convert_index_column(frame, index=None, index_label=None):
    if index is not None and not isinstance(index, bool):
        raise TypeError("index must be boolean")
    if index:
        if isinstance(frame.index, pd.MultiIndex):
            if index_label is None:
                index_label = [
                    v if v else f"level_{i}" for i, v in enumerate(frame.index.names)
                ]
            for i, name in zip(frame.index.levels, index_label):
                frame[name] = i.astype("object")
        else:
            if index_label is None:
                index_label = frame.index.name if frame.index.name else "index"
            frame[index_label] = frame.index.astype("object")
    return frame


def _convert_date_format(frame, date_format=None):
    if date_format is not None:

        def _convert(col):
            if col.dtype.name == "datetime64[ns]":
                return col.apply(lambda x: x.strftime(date_format))
            return col

        frame = frame.apply(_convert)
    return frame
