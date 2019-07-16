import unittest
from unittest.mock import MagicMock

from pytd.table import Table


class TableTestCase(unittest.TestCase):
    def setUp(self):
        mock_client = MagicMock()
        self.table = Table(mock_client, database="sample_datasets", table="www_access")
