import pytd
import pandas as pd

import unittest
try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


class TdTestCase(unittest.TestCase):

    def setUp(self):
        cursor = MagicMock(return_value=MagicMock())
        cursor.description = [('col1', 'int'), ('col2', 'string')]
        cursor.fetchall.return_value = [[1, 'a'], [2, 'b']]

        self.conn = MagicMock()
        self.conn.database = 'sample_datasets'
        self.conn.td_spark = None
        self.conn.cursor.return_value = cursor

    def test_query(self):
        d = pytd.query('select * from tbl', self.conn)

        self.assertListEqual(d['columns'], ['col1', 'col2'])
        self.assertListEqual(d['data'], [[1, 'a'], [2, 'b']])

    def test_write(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        pytd.write(df, 'foo', self.conn, if_exists='error')
        self.conn.write_dataframe.assert_called_with(df, 'foo', 'error')

    def test_write_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            pytd.write(pd.DataFrame([[1, 2], [3, 4]]), 'foo', self.conn, if_exists='bar')
