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

cur = conn.cursor()
cur.execute('select symbol, count(1) as cnt from nasdaq group by 1 order by 2 desc')

rows = cur.fetchall()
column_names = [desc[0] for desc in cur.description]
```
