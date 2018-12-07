pytd
===

[Treasure Data](https://www.treasuredata.com/) Driver for Python

## Installation

```sh
pip install .
```

If you don't want to introduce Kerberos system dependency, try:

```sh
pip install . --process-dependency-links
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
