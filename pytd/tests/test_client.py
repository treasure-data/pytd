import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from pytd.client import Client
from pytd.table import Table


class ClientTest(unittest.TestCase):
    @patch.object(Client, "_fetch_query_engine", return_value=MagicMock())
    def setUp(self, fetch_query_engine):
        self.client = Client(
            apikey="APIKEY", endpoint="ENDPOINT", database="sample_datasets"
        )
        self.assertEqual(self.client.apikey, "APIKEY")
        self.assertEqual(self.client.endpoint, "ENDPOINT")
        self.assertEqual(self.client.database, "sample_datasets")

        self.assertTrue(fetch_query_engine.called)
        self.client.default_engine = MagicMock()
        self.client.api_client = MagicMock()

        res = {"columns": ["col1", "col2"], "data": [[1, "a"], [2, "b"]]}
        self.client.default_engine.execute = MagicMock(return_value=res)

    def test_close(self):
        self.client.close()
        self.assertTrue(self.client.default_engine.close.called)
        self.assertTrue(self.client.api_client.close.called)

    def test_query(self):
        d = self.client.query("select * from tbl")
        self.assertListEqual(d["columns"], ["col1", "col2"])
        self.assertListEqual(d["data"], [[1, "a"], [2, "b"]])

    def test_get_table(self):
        tbl = self.client.get_table("a", "b")
        self.assertTrue(isinstance(tbl, Table))
        self.assertEqual(tbl.database, "a")
        self.assertEqual(tbl.table, "b")

    def test_load_table_from_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        mock_table = MagicMock()
        self.client.load_table_from_dataframe(
            df, mock_table, writer="bulk_import", if_exists="error"
        )
        self.assertTrue(mock_table.import_dataframe.called)


def test_client_context():
    with patch.object(Client, "_fetch_query_engine", return_value=MagicMock()):
        with Client(
            apikey="APIKEY", endpoint="ENDPOINT", database="sample_datasets"
        ) as client:
            client.close = MagicMock()
            client.close.assert_not_called()
        client.close.assert_called_with()
