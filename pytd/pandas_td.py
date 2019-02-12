import six
import datetime
import pandas as pd

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

from .client import Client


def create_engine(url, **kwargs):
    url = urlparse(url)
    engine = url.scheme if url.scheme else 'presto'
    database = url.path[1:] if url.path.startswith('/') else url.path
    return Client(database=database, default_engine=engine)


def read_td_query(query, engine, **kwargs):
    query = "-- read_td_query\n" + query
    return pd.DataFrame(**engine.query(query))


# alias
connect = Client
read_td = read_td_query


def read_td_table(table_name, engine, columns=None, time_range=None, limit=10000, **kwargs):
    # SELECT
    query = "-- read_td_table\n"
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

    return read_td_query(query, engine)


def to_td(frame, name, con, if_exists='fail', **kwargs):
    if if_exists == 'fail':
        mode = 'error'
    elif if_exists == 'replace':
        mode = 'overwrite'
    elif if_exists == 'append':
        mode = 'append'
    else:
        raise ValueError('invalid value for if_exists: %s' % if_exists)

    con.load_table_from_dataframe(frame, name, mode)


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
