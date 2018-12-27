pytd
===

[Treasure Data](https://www.treasuredata.com/) Driver for Python

## Installation

```sh
pip install -e git+git@github.com:takuti/pytd@master#egg=treasure-data
```

If you don't want to introduce Kerberos system dependency, try:

```sh
pip install -e git+git@github.com:takuti/pytd@master#egg=treasure-data --process-dependency-links
```

## Usage

Set `TD_API_KEY` as an environment variable beforehand.

```py
import pytd

conn = pytd.connect(database='sample_datasets')
# or, hard-code your API key:
# >>> pytd.connect(apikey='1/XXX', database='sample_datasets')

pytd.query('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc', conn)
# [['symbol', 'cnt'], ['CRRC', 9268], ['MPET', 9268], ['HELE', 9268], ..., ['ADPVV', 2]]
```

Query result can also be retrieved from a generator, just like [pandas.DataFrame.iterrows](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.iterrows.html):

```py
for index, row in pytd.query_iterrows('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc', conn):
    print(index, row['symbol'], row['cnt'])
```

Your data represented as `pandas.DataFrame` can be directly written to TD in the form of table:

```py
import pandas as pd

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 100]})
pytd.write(df, 'takuti.foo', conn, if_exists='overwrite')
```

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
td.to_td(df, 'takuti.test_table', con, if_exists='replace', index=False)
```

However, it should be noted that only a small portion of the original pandas-td capability is supported in this package. We highly recommend to replace those code with new `pytd` functions as soon as possible since the limited compatibility is not actively maintained.
