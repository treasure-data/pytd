pytd
===

[![Build Status](https://travis-ci.org/treasure-data/pytd.svg?branch=master)](https://travis-ci.org/treasure-data/pytd) [![Build status](https://ci.appveyor.com/api/projects/status/h1os6uvl598o7cau?svg=true)](https://ci.appveyor.com/project/takuti/pytd) [![PyPI version](https://badge.fury.io/py/pytd.svg)](https://badge.fury.io/py/pytd)

**pytd** provides user-friendly interfaces to Treasure Data's [REST APIs](https://github.com/treasure-data/td-client-python), [Presto query engine](https://support.treasuredata.com/hc/en-us/articles/360001457427-Presto-Query-Engine-Introduction), and [Plazma primary storage](https://www.slideshare.net/treasure-data/td-techplazma).

The seamless connection allows your Python code to efficiently read/write a large volume of data from/to Treasure Data. Eventually, pytd makes your day-to-day data analytics work more productive.

## Installation

```sh
pip install pytd
```

## Usage

- [Sample usage on Google Colaboratory](https://colab.research.google.com/drive/1ps_ChU-H2FvkeNlj1e1fcOebCt4ryN11)

Set your [API key](https://support.treasuredata.com/hc/en-us/articles/360000763288-Get-API-Keys) and [endpoint](https://support.treasuredata.com/hc/en-us/articles/360001474288-Sites-and-Endpoints) to the environment variables, `TD_API_KEY` and `TD_API_SERVER`, respectively, and create a client instance:

```py
import pytd

client = pytd.Client(database='sample_datasets')
# or, hard-code your API key, endpoint, and/or query engine:
# >>> pytd.Client(apikey='1/XXX', endpoint='https://api.treasuredata.com/', database='sample_datasets', default_engine='presto')
```

### Query in Treasure Data

Issue Presto query and retrieve the result:

```py
client.query('select symbol, count(1) as cnt from nasdaq group by 1 order by 1')
# {'columns': ['symbol', 'cnt'], 'data': [['AAIT', 590], ['AAL', 82], ['AAME', 9252], ..., ['ZUMZ', 2364]]}
```

In case of Hive:

```py
client.query('select hivemall_version()', engine='hive')
# {'columns': ['_c0'], 'data': [['0.6.0-SNAPSHOT-201901-r01']]} (as of Feb, 2019)
```

It is also possible to explicitly initialize `pytd.Client` for Hive:

```py
client_hive = pytd.Client(database='sample_datasets', default_engine='hive')
client_hive.query('select hivemall_version()')
```

### Write data to Treasure Data

Data represented as `pandas.DataFrame` can be written to Treasure Data as follows:

```py
import pandas as pd

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 10]})
client.load_table_from_dataframe(df, 'takuti.foo', writer='bulk_import', if_exists='overwrite')
```

For the `writer` option, pytd supports three different ways to ingest data to Treasure Data:

1. **Bulk Import API**: `bulk_import` (default)
    - Convert data into a CSV file and upload in the batch fashion.
2. **Presto INSERT INTO query**: `insert_into`
    - Insert every single row in `DataFrame` by issuing an INSERT INTO query through the Presto query engine.
    - Recommended only for a small volume of data.
3. **[td-spark](https://support.treasuredata.com/hc/en-us/articles/360001487167-Apache-Spark-Driver-td-spark-FAQs)**: `spark`
    - Local customized Spark instance directly writes `DataFrame` to Treasure Data's primary storage system.

#### Enabling Spark Writer

Since td-spark gives special access to the main storage system via [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), follow the instructions below:

1. Contact support@treasuredata.com to activate the permission to your Treasure Data account.
2. Install pytd with `[spark]` option if you use the third option:
    ```sh
    pip install pytd[spark]
    ```


If you want to use existing td-spark JAR file, creating `SparkWriter` with `td_spark_path` option would be helpful.

```py
from pytd.writer import SparkWriter

writer = SparkWriter(apikey='1/XXX', endpoint='https://api.treasuredata.com/', td_spark_path='/path/to/td-spark-assembly.jar')
client.load_table_from_dataframe(df, 'mydb.bar', writer=writer, if_exists='overwrite')
```

### DB-API

pytd implements [Python Database API Specification v2.0](https://www.python.org/dev/peps/pep-0249/) with the help of [prestodb/presto-python-client](https://github.com/prestodb/presto-python-client).

Connect to the API first:

```py
from pytd.dbapi import connect

conn = connect(pytd.Client(database='sample_datasets'))
# or, connect with Hive:
# >>> conn = connect(pytd.Client(database='sample_datasets', default_engine='hive'))
```

`Cursor` defined by the specification allows us to flexibly fetch query results from a custom function:

```py
def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return {'data': rows, 'columns': columns}

query('select symbol, count(1) as cnt from nasdaq group by 1 order by 1', conn)
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

for index, row in iterrows('select symbol, count(1) as cnt from nasdaq group by 1 order by 1', conn):
    print(index, row)
# 0 {'cnt': 590, 'symbol': 'AAIT'}
# 1 {'cnt': 82, 'symbol': 'AAL'}
# 2 {'cnt': 9252, 'symbol': 'AAME'}
# 3 {'cnt': 253, 'symbol': 'AAOI'}
# 4 {'cnt': 5980, 'symbol': 'AAON'}
# ...
```

## How to replace pandas-td

**pytd** offers [pandas-td](https://github.com/treasure-data/pandas-td)-compatible functions that provide the same functionalities more efficiently. If you are still using pandas-td, we recommend you to switch to **pytd** as follows.

First, install the package from PyPI:

```sh
pip install pytd
# or, `pip install pytd[spark]` if you wish to use `to_td`
```

Next, make the following modifications on the import statements.

*Before:*

```python
import pandas_td as td
```

```python
In [1]: %%load_ext pandas_td.ipython
```

*After:*

```python
import pytd.pandas_td as td
```

```python
In [1]: %%load_ext pytd.pandas_td.ipython
```

Consequently, all `pandas_td` code should keep running correctly with `pytd`. Report an issue from [here](https://github.com/treasure-data/pytd/issues/new) if you noticed any incompatible behaviors.

### Use existing td-spark-assembly.jar file

If you want to use existing td-spark JAR file, creating `SparkWriter` with `td_spark_path` option would be helpful. You can pass a writer to `connect()` function.

```py
import pytd
import pytd.pandas_td as td
import pandas as pd
apikey = '1/XXX'
endpoint = 'https://api.treasuredata.com/'

writer = pytd.writer.SparkWriter(apikey=apikey, endpoint=endpoint, td_spark_path='/path/to/td-spark-assembly.jar')
con = td.connect(apikey=apikey, endpoint=endpoint, writer=writer)

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 10]})
td.to_td(df, 'mydb.buzz', con, if_exists='replace', index=False)
```

## For developers

We use [black](https://black.readthedocs.io/en/stable/) and [isort](https://github.com/timothycrosley/isort) as a formatter, and [flake8](http://flake8.pycqa.org/en/latest/) as a linter. Our CI checks format with them.

Note that black requires Python 3.6+ while pytd supports 3.5+, so you must need to have Python 3.6+ for development.

We highly recommend you to introduce [pre-commit](https://pre-commit.com/) to ensure your commit follows required format.

You can install pre-commit as follows:

```sh
pip install pre-commit
pre-commit install
```

Now, black, isort, and flake8 will check each time you commit changes. You can skip these check with `git commit --no-verify`.

If you want to check code format manually, you can install them as follows:

```sh
pip install black isort flake8
```

Then, you can run those tool manually;

```sh
black pytd
flake8 pytd
isort
```

You can run formatter, linter, and test by using nox as the following:

```sh
pip install nox # You should install at the first time
nox
```
