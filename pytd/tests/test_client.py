from pytd.client import Client
import pandas as pd

import unittest
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch


class ClientTestCase(unittest.TestCase):

    @patch.object(Client, '_get_engine', return_value=MagicMock())
    def setUp(self, connect_query_engine):
        self.client = Client(apikey='APIKEY', endpoint='ENDPOINT', database='sample_datasets')

        self.assertTrue(connect_query_engine.called)
        self.client.engine = MagicMock()

        res = {'columns': ['col1', 'col2'], 'data': [[1, 'a'], [2, 'b']]}
        self.client.engine.execute = MagicMock(return_value=res)

    def test_close(self):
        self.assertTrue(self.client.writer is None)
        self.client.writer = MagicMock()
        self.client.close()
        self.assertTrue(self.client.engine.close.called)
        self.assertTrue(self.client.writer.close.called)

    def test_query(self):
        d = self.client.query('select * from tbl')
        self.assertListEqual(d['columns'], ['col1', 'col2'])
        self.assertListEqual(d['data'], [[1, 'a'], [2, 'b']])

    def test_load_table_from_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.assertTrue(self.client.writer is None)
        self.client.writer = MagicMock()
        self.client.load_table_from_dataframe(df, 'foo', 'error')
        self.assertTrue(self.client.writer.write_dataframe.called)


def test_client_context():
    with patch.object(Client, '_get_engine', return_value=MagicMock()):
        with Client(apikey='APIKEY', endpoint='ENDPOINT', database='sample_datasets') as client:
            client.close = MagicMock()
            client.close.assert_not_called()
        client.close.assert_called_with()
