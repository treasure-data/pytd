import re
import six
import time
import datetime
import pandas as pd

from ..client import Client
from ..writer import SparkWriter
from ..query_engine import PrestoQueryEngine, HiveQueryEngine
from ..dbapi.connection import Connection


def connect(apikey=None, endpoint=None, **kwargs):
    return Connection(Client(apikey=apikey, endpoint=endpoint, **kwargs))


RE_ENGINE_DESC = re.compile("(?P<type>presto|hive)://(?P<apikey>[0-9]+/[a-z0-9]+)@(?P<host>[^/]+)/(?P<database>[a-z0-9_]+)(\?.*)?")
RE_ENGINE_DESC_SHORT = re.compile("(?P<type>presto|hive):(?P<database>[a-z0-9_]+)(\?.*)?")


def create_engine(url, con=None, header=True, show_progress=5.0, clear_progress=True):
    """Create a handler for query engine based on a URL.

    The following environment variables are used for default connection:

      TD_API_KEY     API key
      TD_API_SERVER  API server (default: https://api.treasuredata.com)

    Parameters
    ----------
    url : string
        Engine descriptor in the form "type://apikey@host/database?params..."
        Use shorthand notation "type:database?params..." for the default connection.
        pytd: "params" will be ignored since pytd.QueryEngine does not have any extra parameters.

    con : pytd.dbapi.Connection, optional
        Handler returned by connect. If not given, default connection is used.

    header : string or boolean, default: True
        Prepend comment strings, in the form "-- comment", as a header of queries.
        Set False to disable header.

    show_progress : double or boolean, default: 5.0
        Number of seconds to wait before printing progress.
        Set False to disable progress entirely.
        pytd: This argument will be ignored.

    clear_progress : boolean, default: True
        If True, clear progress when query completed.
        pytd: This argument will be ignored.

    Returns
    -------
    QueryEngine
    """

    apikey, endpoint = None, None

    res = RE_ENGINE_DESC.search(url)
    if res is not None:
        engine_type = res.group('type')
        apikey = res.group('apikey')
        host = res.group('host')
        database = res.group('database')

        endpoint = "https://{0}/".format(host)
    else:
        res = RE_ENGINE_DESC_SHORT.search(url)
        if res is not None:
            engine_type = res.group('type')
            database = res.group('database')
        else:
            raise ValueError('invalid engine descriptor format')

    if con is None:
        con = connect(apikey=apikey, endpoint=endpoint)
    apikey, endpoint = con.apikey, con.endpoint

    if engine_type == 'presto':
        return PrestoQueryEngine(apikey, endpoint, database, header=header)
    return HiveQueryEngine(apikey, endpoint, database, header=header)


def read_td_query(query, engine, index_col=None, parse_dates=None, distributed_join=False, params=None):
    """Read Treasure Data query into a DataFrame.

    Returns a DataFrame corresponding to the result set of the query string.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    query : string
        Query string to be executed.

    engine : QueryEngine
        Handler returned by create_engine.

    index_col : string, optional
        Column name to use as index for the returned DataFrame object.

    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    distributed_join : boolean, default: False
        (Presto only) If True, distributed join is enabled. If False, broadcast join is used.
        See https://prestodb.io/docs/current/release/release-0.77.html

    params : dict, optional
        Parameters to pass to execute method.
        Available parameters:
        - result_url (str): result output URL
        - priority (int or str): priority (e.g. "NORMAL", "HIGH", etc.)
        - retry_limit (int): retry limit
        pytd: This argument will be ignored.

    Returns
    -------
    DataFrame
    """
    if isinstance(engine, PrestoQueryEngine) and distributed_join is not None:
        header = engine.create_header(['read_td_query', "set session distributed_join = '{0}'\n".format('true' if distributed_join else 'false')])
    else:
        header = engine.create_header('read_td_query')

    return _to_dataframe(engine.execute(header + query), index_col, parse_dates)


def read_td_job(job_id, engine, index_col=None, parse_dates=None):
    """Read Treasure Data job result into a DataFrame.

    Returns a DataFrame corresponding to the result set of the job.
    This method waits for job completion if the specified job is still running.
    Optionally provide an index_col parameter to use one of the columns as
    the index, otherwise default integer index will be used.

    Parameters
    ----------
    job_id : integer
        Job ID.

    engine : QueryEngine
        Handler returned by create_engine.

    index_col : string, optional
        Column name to use as index for the returned DataFrame object.

    parse_dates : list or dict, optional
        - List of column names to parse as dates
        - Dict of {column_name: format string} where format string is strftime
          compatible in case of parsing string times or is one of (D, s, ns, ms, us)
          in case of parsing integer timestamps

    Returns
    -------
    DataFrame
    """
    # get job
    job = engine.client.job(job_id)
    return _to_dataframe(engine.get_job_result(job, wait=True), index_col, parse_dates)


def read_td_table(table_name, engine, index_col=None, parse_dates=None, columns=None, time_range=None, limit=10000):
    """Read Treasure Data table into a DataFrame.

    The number of returned rows is limited by "limit" (default 10,000).
    Setting limit=None means all rows. Be careful when you set limit=None
    because your table might be very large and the result does not fit into memory.

    Parameters
    ----------
    table_name : string
        Name of Treasure Data table in database.

    engine : QueryEngine
        Handler returned by create_engine.

    index_col : string, optional
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
    DataFrame
    """
    # header
    query = engine.create_header("read_td_table('{0}')".format(table_name))
    # SELECT
    query += "SELECT {0}\n".format('*' if columns is None else ', '.join(columns))
    # FROM
    query += "FROM {0}\n".format(table_name)
    # WHERE
    if time_range is not None:
        start, end = time_range
        query += "WHERE td_time_range(time, {0}, {1})\n".format(_convert_time(start), _convert_time(end))
    # LIMIT
    if limit is not None:
        query += "LIMIT {0}\n".format(limit)
    # execute
    return _to_dataframe(engine.execute(query), index_col, parse_dates)


def _convert_time(time):
    if time is None:
        return "NULL"
    elif isinstance(time, six.integer_types):
        t = pd.to_datetime(time, unit='s')
    elif isinstance(time, six.string_types):
        t = pd.to_datetime(time)
    elif isinstance(time, (datetime.date, datetime.datetime)):
        t = pd.to_datetime(time)
    else:
        raise ValueError('invalid time value: {0}'.format(time))
    return "'{0}'".format(t.replace(microsecond=0))


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
            if frame[name].dtype.kind == 'O':
                frame[name] = pd.to_datetime(frame[name], format=parse_dates[name])
            else:
                frame[name] = pd.to_datetime(frame[name], unit=parse_dates[name])
    return frame


# alias
read_td = read_td_query


def to_td(frame, name, con, if_exists='fail', time_col=None, time_index=None, index=True, index_label=None, chunksize=10000, date_format=None):
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
    frame : DataFrame
        DataFrame to be written.

    name : string
        Name of table to be written, in the form 'database.table'.

    con : pytd.dbapi.Connection
        Connection to a Treasure Data account.

    if_exists : {'fail', 'replace', 'append'}, default: 'fail'
        - fail: If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.

    time_col : string, optional
        Column name to use as "time" column for the table. Column type must be
        integer (unixtime), datetime, or string. If None is given (default),
        then the current time is used as time values.

    time_index : int, optional
        Level of index to use as "time" column for the table. Set 0 for a single index.
        This parameter implies index=False.

    index : boolean, default: True
        Write DataFrame index as a column.

    index_label : string or sequence, default: None
        Column label for index column(s). If None is given (default) and index is True,
        then the index names are used. A sequence should be given if the DataFrame uses
        MultiIndex.

    chunksize : int, default: 10,000
        Number of rows to be inserted in each chunk from the dataframe.
        pytd: This argument will be ignored.

    date_format : string, default: None
        Format string for datetime objects
    """
    if if_exists == 'fail':
        mode = 'error'
    elif if_exists == 'replace':
        mode = 'overwrite'
    elif if_exists == 'append':
        mode = 'append'
    else:
        raise ValueError('invalid value for if_exists: %s' % if_exists)

    # convert
    frame = frame.copy()
    frame = _convert_time_column(frame, time_col, time_index)
    frame = _convert_index_column(frame, index, index_label)
    frame = _convert_date_format(frame, date_format)

    writer = SparkWriter(con.apikey, con.endpoint)
    writer.write_dataframe(frame, con.database, name, mode)


def _convert_time_column(frame, time_col=None, time_index=None):
    if time_col is not None and time_index is not None:
        raise ValueError('time_col and time_index cannot be used at the same time')
    if 'time' in frame.columns and time_col != 'time':
        raise ValueError('"time" column already exists')
    if time_col is not None:
        # Use 'time_col' as time column
        if time_col != 'time':
            frame.rename(columns={time_col: 'time'}, inplace=True)
        col = frame['time']
        # convert python string to pandas datetime
        if col.dtype.name == 'object' and len(col) > 0 and isinstance(col[0], six.string_types):
            col = pd.to_datetime(col)
        # convert pandas datetime to unixtime
        if col.dtype.name == 'datetime64[ns]':
            frame['time'] = col.astype('int64') // (10 ** 9)
    elif time_index is not None:
        # Use 'time_index' as time column
        if type(time_index) is bool or not isinstance(time_index, six.integer_types):
            raise TypeError('invalid type for time_index')
        if isinstance(frame.index, pd.MultiIndex):
            idx = frame.index.levels[time_index]
        else:
            if time_index == 0:
                idx = frame.index
            else:
                raise IndexError('list index out of range')
        if idx.dtype.name != 'datetime64[ns]':
            raise TypeError('index type must be datetime64[ns]')
        # convert pandas datetime to unixtime
        frame['time'] = idx.astype('int64') // (10 ** 9)
    else:
        # Use current time as time column
        frame['time'] = int(time.time())
    return frame


def _convert_index_column(frame, index=None, index_label=None):
    if index is not None and not isinstance(index, bool):
        raise TypeError('index must be boolean')
    if index:
        if isinstance(frame.index, pd.MultiIndex):
            if index_label is None:
                index_label = [v if v else "level_%d" % i for i, v in enumerate(frame.index.names)]
            for i, name in zip(frame.index.levels, index_label):
                frame[name] = i.astype('object')
        else:
            if index_label is None:
                index_label = frame.index.name if frame.index.name else 'index'
            frame[index_label] = frame.index.astype('object')
    return frame


def _convert_date_format(frame, date_format=None):
    if date_format is not None:
        def _convert(col):
            if col.dtype.name == 'datetime64[ns]':
                return col.apply(lambda x: x.strftime(date_format))
            return col
        frame = frame.apply(_convert)
    return frame