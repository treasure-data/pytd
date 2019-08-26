Usage Guides
============

Working with DB-API
-------------------

pytd implements `Python Database API Specification
v2.0 <https://www.python.org/dev/peps/pep-0249/>`__ with the help of
`prestodb/presto-python-client <https://github.com/prestodb/presto-python-client>`__.

Connect to the API first:

.. code:: py

   from pytd.dbapi import connect

   conn = connect(pytd.Client(database='sample_datasets'))
   # or, connect with Hive:
   # >>> conn = connect(pytd.Client(database='sample_datasets', default_engine='hive'))

``Cursor`` defined by the specification allows us to flexibly fetch
query results from a custom function:

.. code:: py

   def query(sql, connection):
       cur = connection.cursor()
       cur.execute(sql)
       rows = cur.fetchall()
       columns = [desc[0] for desc in cur.description]
       return {'data': rows, 'columns': columns}

   query('select symbol, count(1) as cnt from nasdaq group by 1 order by 1', conn)

Below is an example of generator-based iterative retrieval, just like
`pandas.DataFrame.iterrows <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.iterrows.html>`__:

.. code:: py

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

Use Existing td-spark-assembly.jar File
---------------------------------------

If you want to use existing td-spark JAR file, creating ``SparkWriter``
with ``td_spark_path`` option would be helpful. You can pass a writer to
``connect()`` function.

.. code:: py

   import pytd
   import pytd.pandas_td as td
   import pandas as pd
   apikey = '1/XXX'
   endpoint = 'https://api.treasuredata.com/'

   writer = pytd.writer.SparkWriter(apikey=apikey, endpoint=endpoint, td_spark_path='/path/to/td-spark-assembly.jar')
   con = td.connect(apikey=apikey, endpoint=endpoint, writer=writer)

   df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 10]})
   td.to_td(df, 'mydb.buzz', con, if_exists='replace', index=False)
