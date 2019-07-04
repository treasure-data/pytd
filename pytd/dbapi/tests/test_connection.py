import unittest
from unittest.mock import MagicMock

from pytd.dbapi import Connection, NotSupportedError


class ConnectionTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.conn = Connection(self.mock_client)

    def test_close(self):
        self.conn.close()
        self.mock_client.close.assert_called_with()

    def test_commit(self):
        self.assertRaises(NotSupportedError, self.conn.commit)

    def test_rollback(self):
        self.assertRaises(NotSupportedError, self.conn.rollback)

    def test_cursor(self):
        self.conn.cursor()
        self.mock_client.engine.cursor.assert_called_with()


def test_connection_context():
    with Connection(MagicMock()) as conn:
        conn.close = MagicMock()
        conn.close.assert_not_called()
    conn.close.assert_called_with()
