from pytd.client import Client
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


class ClientTestCase(unittest.TestCase):

    @patch.object(Client, '_connect_td_presto', return_value=MagicMock())
    def setUp(self, _connect_td_presto):
        self.client = Client(apikey='APIKEY', database='sample_datasets')

        cursor = MagicMock(return_value=MagicMock())
        cursor.description = [('col1', 'int'), ('col2', 'string')]
        cursor.fetchall.return_value = [[1, 'a'], [2, 'b']]

        self.client.get_cursor = MagicMock(return_value=cursor)

        self._connect_td_presto = _connect_td_presto

    def test_close(self):
        self._connect_td_presto.assert_called_with('APIKEY', 'sample_datasets')
        self.assertTrue(self.client.td_spark is None)
        self.client.close()
        self.assertTrue(self.client.td_presto.close.called)

    def test_query(self):
        d = self.client.query('select * from tbl')
        self.assertListEqual(d['columns'], ['col1', 'col2'])
        self.assertListEqual(d['data'], [[1, 'a'], [2, 'b']])

    def test_load_table_from_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        with patch.object(Client, '_setup_td_spark', new=mock_setup_td_spark):
            self.assertTrue(self.client.td_spark is None)
            self.client.load_table_from_dataframe(df, 'foo', 'error')
            self.client.td_spark.createDataFrame.assert_called_with(df)

    def test_load_table_from_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.client.load_table_from_dataframe(pd.DataFrame([[1, 2], [3, 4]]), 'foo', if_exists='bar')


def test_client_context():
    with patch.object(Client, '_connect_td_presto', return_value=MagicMock()):
        with Client(apikey='APIKEY', database='sample_datasets') as client:
            client.close = MagicMock()
            client.close.assert_not_called()
        client.close.assert_called_with()
