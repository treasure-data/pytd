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
        self._connect_td_presto = _connect_td_presto

    def test_close(self):
        self._connect_td_presto.assert_called_with('APIKEY', 'sample_datasets')
        self.assertTrue(self.client.td_spark is None)
        self.client.close()
        self.assertTrue(self.client.td_presto.close.called)

    def test_write_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        with patch.object(Client, '_setup_td_spark', new=mock_setup_td_spark):
            self.assertTrue(self.client.td_spark is None)
            self.client.write_dataframe(df, 'foo', 'error')
            self.client.td_spark.createDataFrame.assert_called_with(df)


def test_client_context():
    with patch.object(Client, '_connect_td_presto', return_value=MagicMock()):
        with Client(apikey='APIKEY', database='sample_datasets') as client:
            client.close = MagicMock()
            client.close.assert_not_called()
        client.close.assert_called_with()
