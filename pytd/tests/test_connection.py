from pytd.connection import Connection
from pytd.error import NotSupportedError
import pandas as pd

import unittest
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch


def mock_setup_td_spark(self):
    self.td_spark = MagicMock()


class ConnectionTestCase(unittest.TestCase):

    @patch.object(Connection, '_connect_td_presto', return_value=MagicMock())
    def setUp(self, _connect_td_presto):
        self.conn = Connection(apikey='APIKEY', database='sample_datasets')
        self._connect_td_presto = _connect_td_presto

    def test_close(self):
        self._connect_td_presto.assert_called_with('APIKEY', 'sample_datasets')
        self.assertTrue(self.conn.td_spark is None)
        self.conn.close()
        self.assertTrue(self.conn.td_presto.close.called)

    def test_commit(self):
        self.assertRaises(NotSupportedError, self.conn.commit)

    def test_rollback(self):
        self.assertRaises(NotSupportedError, self.conn.rollback)

    def test_cursor(self):
        self.conn.td_presto.cursor = MagicMock()
        self.conn.cursor()
        self.assertTrue(self.conn.td_presto.cursor.called)

    def test_write_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        with patch.object(Connection, '_setup_td_spark', new=mock_setup_td_spark):
            self.assertTrue(self.conn.td_spark is None)
            self.conn.write_dataframe(df, 'foo', 'error')
            self.conn.td_spark.createDataFrame.assert_called_with(df)


def test_connection_context():
    with patch.object(Connection, '_connect_td_presto', return_value=MagicMock()):
        with Connection(apikey='APIKEY', database='sample_datasets') as conn:
            conn.close = MagicMock()
            conn.close.assert_not_called()
        conn.close.assert_called_with()
