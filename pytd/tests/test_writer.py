import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from pytd.writer import BulkImportWriter, InsertIntoWriter, SparkWriter, _cast_dtypes


class WriterTestCase(unittest.TestCase):
    def setUp(self):
        self.dft = pd.DataFrame(
            {
                "A": np.random.rand(3),
                "B": 1,
                "C": "foo",
                "D": pd.Timestamp("20010102"),
                "E": pd.Series([1.0] * 3).astype("float32"),
                "F": False,
                "G": pd.Series([1] * 3, dtype="int8"),
                "H": [[0, 1, 2], [1, 2, 3], [2, 3, 4]],
            }
        )

    def test_cast_dtypes(self):
        dft = _cast_dtypes(self.dft, inplace=False)
        dtypes = set(dft.dtypes)
        self.assertEqual(
            dtypes, set([np.dtype("int"), np.dtype("float"), np.dtype("O")])
        )
        self.assertEqual(dft["F"][0], "false")

    def test_cast_dtypes_inplace(self):
        _cast_dtypes(self.dft)
        dtypes = set(self.dft.dtypes)
        self.assertEqual(
            dtypes, set([np.dtype("int"), np.dtype("float"), np.dtype("O")])
        )
        self.assertEqual(self.dft["F"][0], "false")


class InsertIntoWriterTestCase(unittest.TestCase):
    def setUp(self):
        self.writer = InsertIntoWriter()
        self.table = MagicMock()
        self.table.exist.return_value = True

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "ignore"
        )
        self.assertFalse(self.table.client.query.called)

    def test_write_dataframe_append(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "append"
        )
        # 1) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
        )
        # 1) Delete existing table
        self.assertEqual(self.table.delete.call_count, 1)
        # 2) Create an alternative table
        self.assertEqual(self.table.create.call_count, 1)
        # 3) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_query_builder(self):
        df = pd.DataFrame({"a": [1, 2], "b": [None, 3]})
        _cast_dtypes(df)
        q = self.writer._build_query(
            "foo", "bar", list(df.itertuples(index=False, name=None)), ["a", "b"]
        )
        # column 'b' is handled as float64 because of null
        q_expected = "INSERT INTO foo.bar (a, b) VALUES (1, null), (2, 3.0)"
        self.assertEqual(q, q_expected)

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )


class BulkImportWriterTestCase(unittest.TestCase):
    def setUp(self):
        self.writer = BulkImportWriter()

        mock_bulk_import = MagicMock()
        mock_bulk_import.error_records = 1
        mock_bulk_import.valid_records = 2

        mock_api_client = MagicMock()
        mock_api_client.create_bulk_import.return_value = mock_bulk_import

        self.table = MagicMock()
        self.table.client.api_client = mock_api_client
        self.table.exist.return_value = True

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "ignore"
        )
        self.assertFalse(self.table.client.api_client.create_bulk_import.called)

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
        )
        self.assertTrue(self.table.delete.called)
        self.assertTrue(self.table.create.called)
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )


class SparkWriterTestCase(unittest.TestCase):
    @patch.object(SparkWriter, "_fetch_td_spark", return_value=MagicMock())
    def setUp(self, _fetch_td_spark):
        self.writer = SparkWriter()

        td_spark = MagicMock()
        td_spark._jsc.sc().isStopped.return_value = False
        self.writer.td_spark = td_spark
        self.writer.fetched_apikey = "1/XXX"
        self.writer.fetched_endpoint = "ENDPOINT"

        self.table = MagicMock()
        self.table.client.apikey = "1/XXX"
        self.table.client.endpoint = "ENDPOINT"

    def test_write_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.writer.write_dataframe(df, self.table, "error")
        self.assertTrue(self.writer.td_spark.createDataFrame.called)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.td_spark.stop.called)

        self.writer.td_spark._jsc.sc().isStopped.return_value = True
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )
