Changelog
=========

v1.4.1 (2021-02-10)
-------------------

- Presto queries issued by ``pandas_td.read_td_query`` use ``join_distribution_type`` session property instead of the deprecated ``distributed_join`` property. See our `documentation <https://docs.treasuredata.com/display/public/PD/Presto+0.205+to+317+Migration+2020#Presto0.205to317Migration2020-DeprecatedFeatures>`__ for more information about the change. (`#100 <https://github.com/treasure-data/pytd/pull/100>`__)

v1.4.0 (2021-01-11)
-------------------

- Deprecate (Py)Spark 2.x and Python 3.5 support, and migrate to Spark 3.x and Python 3.8, respectively. ``SparkWriter`` requires running with Spark 3.x from now on. (`#94 <https://github.com/treasure-data/pytd/pull/94>`__)


v1.3.0 (2020-05-11)
-------------------

- Enable passing extra keyword arguments (e.g., ``fmt="msgpack"``) to ``pandas_td.to_td``. (`#80 <https://github.com/treasure-data/pytd/pull/80>`__)
- Support ``engine_version`` option in query APIs. (`#81 <https://github.com/treasure-data/pytd/pull/81>`__)
- Add ``force_tdclient`` option to Presto query interfaces for deterministically using ``tdclient`` rather than ``prestodb``. (`#85 <https://github.com/treasure-data/pytd/pull/85>`__)
- Add a precondition check to ``Writer#write_dataframe`` for making sure the type of ``table`` argument. (`#86 <https://github.com/treasure-data/pytd/pull/86>`__)
- Documentation updates. (`#82 <https://github.com/treasure-data/pytd/pull/82>`__, `#89 <https://github.com/treasure-data/pytd/pull/89>`__)


v1.2.0 (2020-03-18)
-------------------

- Support nullable column containing ``pandas.NA``, which was `newly introduced in pandas 1.0.0 <https://pandas.pydata.org/pandas-docs/version/1.0.0/whatsnew/v1.0.0.html#experimental-na-scalar-to-denote-missing-values>`__. The ``Writer`` module internally converts ``pandas.NA`` into ``None`` before ingesting ``pandas.DataFrame`` to Treasure Data. Note that ``Writer#write_dataframe`` may behave differently between before and after upgrading pandas to 1.0.0 because of the experimental, backward-incompatible updates on the dependent package. (`#72 <https://github.com/treasure-data/pytd/pull/72>`__)


v1.1.0 (2020-03-07)
-------------------

- Support list-type column in ``BulkImportWriter#write_dataframe``. A list-type column of ``pandas.DataFrame`` will be stored into Treasure Data table as an array-type column. (`#60 <https://github.com/treasure-data/pytd/pull/60>`__)
- Store a resulting object from ``Client#query`` to ``Client.query_executed``. The object could be a Treasure Data job id if query is executed via ``tdclient``. (`#63 <https://github.com/treasure-data/pytd/pull/63>`__)
- Support null value in ``str``, ``bool``, and ``"Int64"``-type column of ``pandas.DataFrame``. (`#68 <https://github.com/treasure-data/pytd/pull/68>`__, `#71 <https://github.com/treasure-data/pytd/pull/71>`__)
- Update minimum required pandas version to 0.24.0 (`#69 <https://github.com/treasure-data/pytd/pull/69>`__)

v1.0.0 (2019-11-11)
-------------------

-  Update documentation site. (`#49 <https://github.com/treasure-data/pytd/pull/49>`__, `#57 <https://github.com/treasure-data/pytd/pull/57>`__)
-  Add Treasure Data API endpoint HTTPS scheme validation. (`#51 <https://github.com/treasure-data/pytd/pull/51>`__)
-  Support bulk importing with the MessagePack format. (`#53 <https://github.com/treasure-data/pytd/pull/53>`__)
-  Improve stability of ``BulkImportWriter`` session ID. (`#55 <https://github.com/treasure-data/pytd/pull/55>`__)
-  Require td-client-python version 1.1.0 or later. (`#56 <https://github.com/treasure-data/pytd/pull/56>`__)
-  Add ``Client#exists(database, table)`` and ``Client#create_database_if_not_exists(database)`` method. (`#58 <https://github.com/treasure-data/pytd/pull/58/>`__)

v0.8.0 (2019-09-17)
-------------------

-  Clean up docstrings and launch documentation site.
   (`#43 <https://github.com/treasure-data/pytd/pull/43>`__, `#44 <https://github.com/treasure-data/pytd/pull/44>`__)
-  Disable ``type``, one of the Treasure Data-specific query parameters, because it is conflicted with the ``engine`` option.
   (`#45 <https://github.com/treasure-data/pytd/pull/45>`__)
-  Add `td-pyspark <https://pypi.org/project/td-pyspark/>`__ dependency for easily accessing to the `td-spark <https://treasure-data.github.io/td-spark/>`__ functionalities.
   (`#46 <https://github.com/treasure-data/pytd/pull/46>`__, `#47 <https://github.com/treasure-data/pytd/pull/47>`__)

v0.7.0 (2019-08-23)
-------------------

-  Support ``if_exists="append"`` option in ``BulkImportWriter``.
   (`#38 <https://github.com/treasure-data/pytd/pull/38>`__)
-  ``PrestoQueryEngine`` and ``HiveQueryEngine`` accept Treasure
   Data-specific query parameters such as ``priority``.
   (`#41 <https://github.com/treasure-data/pytd/pull/41>`__)
