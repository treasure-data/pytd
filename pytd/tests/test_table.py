from pytd.table import Table
import pandas as pd

import unittest
from unittest.mock import MagicMock


class TableTestCase(unittest.TestCase):

    def setUp(self):
        mock_client = MagicMock()
        self.table = Table(mock_client, database='sample_datasets', table='www_access')
        self.table.td_spark = MagicMock()

    def test_if_exists_error(self):
        with self.assertRaises(RuntimeError):
            self.table.insert_into(pd.DataFrame([[1, 2], [3, 4]]), 'error')

    def test_if_exists_ignore(self):
        self.table.insert_into(pd.DataFrame([[1, 2], [3, 4]]), 'ignore')
        self.assertFalse(self.table.client.query.called)

    def test_if_exists_append(self):
        self.table.insert_into(pd.DataFrame([[1, 2], [3, 4]]), 'append')
        # 1) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_if_exists_overwrite(self):
        self.table.insert_into(pd.DataFrame([[1, 2], [3, 4]]), 'overwrite')
        # 1) Create an alternative table with schema
        # 2) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 2)

    def test_if_exists_invalid(self):
        with self.assertRaises(ValueError):
            self.table.insert_into(pd.DataFrame([[1, 2], [3, 4]]), 'bar')

    def test_spark_close(self):
        self.table.spark_import(pd.DataFrame([[1, 2], [3, 4]]), 'overwrite')
        self.assertTrue(self.table.td_spark.createDataFrame.called)

        self.table.close()
        self.assertTrue(self.table.client is None)
        self.assertTrue(self.table.td_spark.stop.called)
