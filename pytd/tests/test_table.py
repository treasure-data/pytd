import unittest
from unittest.mock import MagicMock

import pandas as pd

from pytd.table import Table


class TableTestCase(unittest.TestCase):
    def setUp(self):
        mock_client = MagicMock()
        self.table = Table(mock_client, database="sample_datasets", table="www_access")
        self.table.td_spark = MagicMock()

    def test_if_exists_error(self):
        with self.assertRaises(RuntimeError):
            self.table.insert_into(
                [[1, 2], [3, 4]], ["c1", "c2"], ["bigint", "bigint"], "error"
            )

    def test_if_exists_ignore(self):
        self.table.insert_into(
            [[1, 2], [3, 4]], ["c1", "c2"], ["bigint", "bigint"], "ignore"
        )
        self.assertFalse(self.table.client.query.called)

    def test_if_exists_append(self):
        self.table.insert_into(
            [[1, 2], [3, 4]], ["c1", "c2"], ["bigint", "bigint"], "append"
        )
        # 1) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_if_exists_overwrite(self):
        self.table.insert_into(
            [[1, 2], [3, 4]], ["c1", "c2"], ["bigint", "bigint"], "overwrite"
        )
        # 1) Create an alternative table with schema
        # 2) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 2)

    def test_if_exists_invalid(self):
        with self.assertRaises(ValueError):
            self.table.insert_into(
                [[1, 2], [3, 4]], ["c1", "c2"], ["bigint", "bigint"], "bar"
            )

    def test_spark_close(self):
        self.table.spark_import(pd.DataFrame([[1, 2], [3, 4]]), "overwrite")
        self.assertTrue(self.table.td_spark.createDataFrame.called)

        self.table.close()
        self.assertTrue(self.table.client is None)
        self.assertTrue(self.table.td_spark.stop.called)
