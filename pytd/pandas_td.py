import six
import datetime
import pandas as pd

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

import pytd


def create_engine(url, **kwargs):
    url = urlparse(url)
    database = url.path[1:] if url.path.startswith('/') else url.path
    return pytd.connect(database=database)


def read_td_query(query, engine, **kwargs):
    columns, rows = pytd.query(query, engine)
    return pd.DataFrame(rows, columns=columns)


# alias
connect = pytd.connect
read_td = read_td_query


def read_td_table(table_name, engine,
                  index_col=None,  # unused
                  parse_dates=None,
                  columns=None,
                  time_range=None,
                  limit=10000):
    # SELECT
    query = "SELECT {0}\n".format('*' if columns is None else ', '.join(columns))
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


def to_td(frame,
          name,
          con,
          if_exists='fail',
          time_col=None,  # unused
          time_index=None,
          index=True,
          index_label=None,
          chunksize=10000,
          date_format=None):

    if if_exists == 'fail':
        mode = 'error'
    elif if_exists == 'replace':
        mode = 'overwrite'
    elif if_exists == 'append':
        mode = 'append'
    else:
        raise ValueError('invalid value for if_exists: %s' % if_exists)

    pytd.write(frame, name, con, mode)


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
