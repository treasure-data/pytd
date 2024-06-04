pytd
====

|Build status| |PyPI version| |docs status|

**pytd** provides user-friendly interfaces to Treasure Data’s `REST
APIs <https://github.com/treasure-data/td-client-python>`__, `Presto
query
engine <https://docs.treasuredata.com/display/public/PD/About+Presto+Distributed+Query+Engine>`__,
and `Plazma primary
storage <https://www.slideshare.net/treasure-data/td-techplazma>`__.

The seamless connection allows your Python code to efficiently
read/write a large volume of data from/to Treasure Data. Eventually,
pytd makes your day-to-day data analytics work more productive.

Installation
------------

.. code:: sh

   pip install pytd

Usage
-----

-  `Documentation <https://pytd-doc.readthedocs.io/>`__
-  `Sample usage on Google
   Colaboratory <https://colab.research.google.com/drive/1ps_ChU-H2FvkeNlj1e1fcOebCt4ryN11>`__

Set your `API
key <https://docs.treasuredata.com/display/public/PD/Getting+Your+API+Keys>`__
and
`endpoint <https://docs.treasuredata.com/display/public/PD/Sites+and+Endpoints>`__
to the environment variables, ``TD_API_KEY`` and ``TD_API_SERVER``,
respectively, and create a client instance:

.. code:: py

   import pytd

   client = pytd.Client(database='sample_datasets')
   # or, hard-code your API key, endpoint, and/or query engine:
   # >>> pytd.Client(apikey='1/XXX', endpoint='https://api.treasuredata.com/', database='sample_datasets', default_engine='presto')

Query in Treasure Data
~~~~~~~~~~~~~~~~~~~~~~

Issue Presto query and retrieve the result:

.. code:: py

   client.query('select symbol, count(1) as cnt from nasdaq group by 1 order by 1')
   # {'columns': ['symbol', 'cnt'], 'data': [['AAIT', 590], ['AAL', 82], ['AAME', 9252], ..., ['ZUMZ', 2364]]}

In case of Hive:

.. code:: py

   client.query('select hivemall_version()', engine='hive')
   # {'columns': ['_c0'], 'data': [['0.6.0-SNAPSHOT-201901-r01']]} (as of Feb, 2019)

It is also possible to explicitly initialize ``pytd.Client`` for Hive:

.. code:: py

   client_hive = pytd.Client(database='sample_datasets', default_engine='hive')
   client_hive.query('select hivemall_version()')

Here is an example of generator-based iterative retrieval using DB-API.
For details, please refer to `Documentation  <https://pytd-doc.readthedocs.io/en/latest/usage.html#working-with-db-api>`__

.. code:: py

   from pytd.dbapi import connect

   conn = connect(pytd.Client(database='sample_datasets'))
   # or, connect with Hive:
   # >>> conn = connect(pytd.Client(database='sample_datasets', default_engine='hive'))

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

When you face unexpected timeout error with Presto, you can try iterative way to retrieve data.

Write data to Treasure Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Data represented as ``pandas.DataFrame`` can be written to Treasure Data
as follows:

.. code:: py

   import pandas as pd

   df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 10]})
   client.load_table_from_dataframe(df, 'takuti.foo', writer='bulk_import', if_exists='overwrite')

For the ``writer`` option, pytd supports three different ways to ingest
data to Treasure Data:

1. **Bulk Import API**: ``bulk_import`` (default)

   -  Convert data into a CSV file and upload in the batch fashion.

2. **Presto INSERT INTO query**: ``insert_into``

   -  Insert every single row in ``DataFrame`` by issuing an INSERT INTO
      query through the Presto query engine.
   -  Recommended only for a small volume of data.

3. `td-spark <https://treasure-data.github.io/td-spark/>`__:
   ``spark`` (No longer available)

   -  Local customized Spark instance directly writes ``DataFrame`` to
      Treasure Data’s primary storage system.

Characteristics of each of these methods can be summarized as follows:

+-----------------------------------+------------------+------------------+---------------------------------+
|                                   | ``bulk_import``  | ``insert_into``  | ``spark (No longer available)`` |
+===================================+==================+==================+=================================+
| Scalable against data volume      |        ✓         |                  |                 ✓               |
+-----------------------------------+------------------+------------------+---------------------------------+
| Write performance for larger data |                  |                  |                 ✓               |
+-----------------------------------+------------------+------------------+---------------------------------+
| Memory efficient                  |        ✓         |        ✓         |                                 |
+-----------------------------------+------------------+------------------+---------------------------------+
| Disk efficient                    |                  |        ✓         |                                 |
+-----------------------------------+------------------+------------------+---------------------------------+
| Minimal package dependency        |        ✓         |        ✓         |                                 |
+-----------------------------------+------------------+------------------+---------------------------------+

Enabling Spark Writer
^^^^^^^^^^^^^^^^^^^^^

Since td-spark gives special access to the main storage system via
`PySpark <https://spark.apache.org/docs/latest/api/python/index.html>`__,
follow the instructions below:

1. Contact support@treasuredata.com to activate the permission to your
   Treasure Data account. Note that the underlying component, Plazma Public
   API, limits its free tier at 100GB Read and 100TB Write.
2. Install pytd with ``[spark]`` option if you use the third option:
   ``pip install pytd[spark]``

If you want to use existing td-spark JAR file, creating ``SparkWriter``
with ``td_spark_path`` option would be helpful.

.. code:: py

   from pytd.writer import SparkWriter

   writer = SparkWriter(td_spark_path='/path/to/td-spark-assembly.jar')
   client.load_table_from_dataframe(df, 'mydb.bar', writer=writer, if_exists='overwrite')

Comparison between pytd, td-client-python, and pandas-td
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Treasure Data offers three different Python clients on GitHub, and the following list summarizes their characteristics.

1. `td-client-python <https://github.com/treasure-data/td-client-python>`__

   - Basic REST API wrapper.
   - Similar functionalities to td-client-{`ruby <https://github.com/treasure-data/td-client-ruby>`__, `java <https://github.com/treasure-data/td-client-java>`__, `node <https://github.com/treasure-data/td-client-node>`__, `go <https://github.com/treasure-data/td-client-go>`__}.
   - The capability is limited by `what Treasure Data REST API can do <https://docs.treasuredata.com/display/public/PD/REST+APIs+in+Treasure+Data>`__.

2. **pytd**

   - Access to Plazma via td-spark as introduced above.
   - Efficient connection to Presto based on `presto-python-client <https://github.com/prestodb/presto-python-client>`__.
   - Multiple data ingestion methods and a variety of utility functions.

3. `pandas-td <https://github.com/treasure-data/pandas-td>`__ *(deprecated)*

   - Old tool optimized for `pandas <https://pandas.pydata.org>`__ and `Jupyter Notebook <https://jupyter.org>`__.
   - **pytd** offers its compatible function set (see below for the detail).

An optimal choice of package depends on your specific use case, but common guidelines can be listed as follows:

- Use td-client-python if you want to execute *basic CRUD operations* from Python applications.
- Use **pytd** for (1) *analytical purpose* relying on pandas and Jupyter Notebook, and (2) achieving *more efficient data access* at ease.
- Do not use pandas-td. If you are using pandas-td, replace the code with pytd based on the following guidance as soon as possible.

How to replace pandas-td
^^^^^^^^^^^^^^^^^^^^^^^^

**pytd** offers
`pandas-td <https://github.com/treasure-data/pandas-td>`__-compatible
functions that provide the same functionalities more efficiently. If you
are still using pandas-td, we recommend you to switch to **pytd** as
follows.

First, install the package from PyPI:

.. code:: sh

   pip install pytd
   # or, `pip install pytd[spark]` if you wish to use `to_td`

Next, make the following modifications on the import statements.

*Before:*

.. code:: python

   import pandas_td as td

.. code:: python

   In [1]: %%load_ext pandas_td.ipython

*After:*

.. code:: python

   import pytd.pandas_td as td

.. code:: python

   In [1]: %%load_ext pytd.pandas_td.ipython

Consequently, all ``pandas_td`` code should keep running correctly with
``pytd``. Report an issue from
`here <https://github.com/treasure-data/pytd/issues/new>`__ if you
noticed any incompatible behaviors.

.. |Build status| image:: https://github.com/treasure-data/pytd/workflows/Build/badge.svg
   :target: https://github.com/treasure-data/pytd/actions/
.. |PyPI version| image:: https://badge.fury.io/py/pytd.svg
   :target: https://badge.fury.io/py/pytd
.. |docs status| image:: https://readthedocs.org/projects/pytd-doc/badge/?version=latest
   :target: https://pytd-doc.readthedocs.io/en/latest/?badge=latest
