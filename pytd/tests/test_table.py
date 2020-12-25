import unittest
from unittest.mock import MagicMock

import pandas as pd

from pytd.table import Table


class TableTestCase(unittest.TestCase):
    def setUp(self):
        mock_client = MagicMock()
        self.database_name = "sample_datasets"
        self.table_name = "www_access"
        self.table = Table(
            mock_client, database=self.database_name, table=self.table_name
        )

    def test_properties(self):
        self.assertEqual(self.table.database, self.database_name)
        self.assertEqual(self.table.table, self.table_name)
        self.assertTrue(self.table.exists)

    def test_create_empty(self):
        self.table.create()
        self.assertTrue(self.table.client.api_client.create_log_table.called)

    def test_create(self):
        self.table.create(column_names=["a"], column_types=["varchar"])
        self.table.client.query.assert_called_with(
            f"CREATE TABLE {self.database_name}.{self.table_name} (a varchar)",
            engine="presto",
        )

    def test_delete(self):
        self.table.delete()
        self.table.client.api_client.delete_table.assert_called_with(
            self.database_name, self.table_name
        )

    def test_import_dataframe(self):
        df = pd.DataFrame({"columns": ["col1", "col2"], "data": [[1, "a"], [2, "b"]]})
        writer = MagicMock()
        self.table.import_dataframe(df, writer)
        self.assertTrue(writer.write_dataframe.called)
