import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from pytd.writer import BulkImportWriter, InsertIntoWriter, SparkWriter


class InsertIntoWriterTestCase(unittest.TestCase):
    def setUp(self):
        mock_api_client = MagicMock()
        mock_presto = MagicMock()

        self.writer = InsertIntoWriter(mock_api_client, mock_presto)

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "ignore"
        )
        self.assertFalse(self.writer.presto.execute.called)

    def test_write_dataframe_append(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "append"
        )
        # 1) INSERT INTO
        self.assertEqual(self.writer.presto.execute.call_count, 1)

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "overwrite"
        )
        # 1) Delete existing table
        # 2) Create an alternative table
        # 3) INSERT INTO
        self.assertEqual(self.writer.presto.execute.call_count, 3)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.api_client is None)


class BulkImportWriterTestCase(unittest.TestCase):
    def setUp(self):
        mock_bulk_import = MagicMock()
        mock_bulk_import.error_records = 1
        mock_bulk_import.valid_records = 2

        mock_api_client = MagicMock()
        mock_api_client.create_bulk_import.return_value = mock_bulk_import

        self.writer = BulkImportWriter(mock_api_client)

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "ignore"
        )
        self.assertFalse(self.writer.api_client.create_bulk_import.called)

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", "overwrite"
        )
        self.assertTrue(self.writer.api_client.delete_table.called)
        self.assertTrue(self.writer.api_client.create_log_table.called)
        self.assertTrue(self.writer.api_client.create_bulk_import.called)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.api_client is None)


class SparkWriterTestCase(unittest.TestCase):
    @patch.object(SparkWriter, "_fetch_td_spark", return_value=MagicMock())
    def setUp(self, _fetch_td_spark):
        self.writer = SparkWriter("1/XXX", "ENDPOINT")
        self.assertTrue(_fetch_td_spark.called)
        self.writer.td_spark = MagicMock()

    def test_write_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.writer.write_dataframe(df, "foo", "bar", "error")
        self.assertTrue(self.writer.td_spark.createDataFrame.called)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), "foo", "bar", if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.td_spark.stop.called)
