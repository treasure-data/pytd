.. pytd documentation master file, created by
   sphinx-quickstart on Thu Aug 22 16:29:34 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. include:: ../README.rst

.. note:: There is a known difference to ``pandas_td.to_td`` function for type conversion.
   Since :class:`pytd.writer.BulkImportWriter`, default writer pytd, uses CSV as an intermediate file before
   uploading a table, column type may change via ``pandas.read_csv``. To respect column type as much as possible,
   you need to pass `fmt="msgpack"` argument to ``to_td`` function.

   For more detail, see ``fmt`` option of :func:`pytd.pandas_td.to_td`.

More Examples
-------------

.. toctree::
   :maxdepth: 2

   usage

API Reference
-------------

.. toctree::
   :maxdepth: 2

   reference
   pandas_td
   pyspark
   dbapi

Changelog
---------

.. toctree::
  :maxdepth: 2

  changelog

Development
-----------

.. toctree::
   :maxdepth: 2

   contributing
