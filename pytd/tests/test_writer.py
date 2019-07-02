import unittest

import pandas as pd

from pytd.writer import SparkWriter

try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch


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
