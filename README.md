pytd
===

[Treasure Data](https://www.treasuredata.com/) Driver for Python

## Installation

```sh
pip install -e git+git@github.com:takuti/pytd@master#egg=treasure-data
```

If you don't want to introduce Kerberos system dependency, add `--process-dependency-links` option to the command.

## Usage

Set `TD_API_KEY` as an environment variable beforehand and create a client instance:

```py
import pytd

client = pytd.Client(database='sample_datasets')
# or, hard-code your API key:
# >>> pytd.Client(apikey='1/XXX', database='sample_datasets')
```

Issue query and retrieve the result:

```py

client.query('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc')
# {'data': [['CRRC', 9268], ['MPET', 9268], ['HELE', 9268], ..., ['ADPVV', 2]], 'columns': ['symbol', 'cnt']}
```

Once you install the package with PySpark dependencies, any data represented as `pandas.DataFrame` can directly be written to TD via [td-spark](https://support.treasuredata.com/hc/en-us/articles/360001487167-Apache-Spark-Driver-td-spark-FAQs):

```sh
pip install -e git+git@github.com:takuti/pytd@master#egg=treasure-data[spark]
```

```py
import pandas as pd

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 10]})
client.load_table_from_dataframe(df, 'takuti.foo', if_exists='overwrite')
```

### DB-API

`pytd` implements [Python Database API Specification v2.0](https://www.python.org/dev/peps/pep-0249/) with the help of [prestodb/presto-python-client](https://github.com/prestodb/presto-python-client).

Connect to the API first:

```py
import pytd

conn = pytd.dbapi.connect(database='sample_datasets')
```

`Cursor` defined by the specification allows us to flexibly fetch query results from a custom function:

```py
def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return {'data': rows, 'columns': columns}

query('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc', conn)
```

Below is an example of generator-based iterative retrieval, just like [pandas.DataFrame.iterrows](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.iterrows.html):

```py
def iterrows(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    index = 0
    columns = None
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if columns is None:
            columns = [desc[0] for desc in cur.description]
        yield index, dict(zip(columns, row))
        index += 1

for index, row in iterrows('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc', conn):
    print(index, row)
# 0 {'cnt': 9268, 'symbol': 'ASBC'}
# 1 {'cnt': 9268, 'symbol': 'MGEE'}
# 2 {'cnt': 9268, 'symbol': 'DIOD'}
# 3 {'cnt': 9268, 'symbol': 'NTRS'}
# 4 {'cnt': 9268, 'symbol': 'AGYS'}
# ...
```

### pandas-td compatibility

If you are familiar with [pandas-td](https://github.com/treasure-data/pandas-td), `pytd` provides some compatible functions:

```py
import pytd.pandas_td as td

# Initialize query engine
engine = td.create_engine('presto:sample_datasets')

# Read Treasure Data query into a DataFrame
df = td.read_td('select * from www_access', engine)

# Read Treasure Data table into a DataFrame
df = td.read_td_table('nasdaq', engine, limit=10000)

# Write a DataFrame to a Treasure Data table
con = td.connect()
td.to_td(df, 'takuti.test_table', con, if_exists='replace')
```

However, it should be noted that only a small portion of the original pandas-td capability is supported in this package. We highly recommend to replace those code with new `pytd` functions as soon as possible since the limited compatibility is not actively maintained.
