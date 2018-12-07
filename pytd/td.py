import os
import prestodb


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    return [column_names] + rows


def query_iterrows(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    index = 0
    column_names = None
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if index == 0:
            column_names = [desc[0] for desc in cur.description]
        yield index, dict(zip(column_names, row))
        index += 1


class Connection(object):

    def __init__(self, apikey=None, database='sample_datasets'):
        if apikey is None:
            if 'TD_API_KEY' not in os.environ:
                raise ValueError("either argument 'apikey' or environment variable 'TD_API_KEY' should be set")
            apikey = os.environ['TD_API_KEY']

        self.td_presto = prestodb.dbapi.connect(
            host='api-presto.treasuredata.com',
            port=443,
            http_scheme='https',
            user=apikey,
            catalog='td-presto',
            schema=database
        )

    def cursor(self):
        return self.td_presto.cursor()
