from pytd.connection import Connection
from pytd.error import NotSupportedError

import unittest
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch


class ConnectionTestCase(unittest.TestCase):

    @patch('pytd.client.Client')
    def setUp(self, mock_client_class):
        self.mock_client = mock_client_class.return_value
        self.conn = Connection(apikey='APIKEY', database='sample_datasets')

    def test_close(self):
        self.conn.close()
        self.mock_client.close.assert_called_with()

    def test_commit(self):
        self.assertRaises(NotSupportedError, self.conn.commit)

    def test_rollback(self):
        self.assertRaises(NotSupportedError, self.conn.rollback)

    def test_cursor(self):
        self.conn.cursor()
        self.mock_client.td_presto.cursor.assert_called_with()


def test_connection_context():
    with Connection(apikey='APIKEY', database='sample_datasets') as conn:
        conn.close = MagicMock()
        conn.close.assert_not_called()
    conn.close.assert_called_with()
