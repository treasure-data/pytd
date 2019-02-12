from pytd.query_engine import PrestoQueryEngine, HiveQueryEngine

import unittest
try:
    from unittest.mock import MagicMock
    from unittest.mock import patch
except ImportError:
    from mock import MagicMock
    from mock import patch


class PrestoQueryEngineTestCase(unittest.TestCase):

    @patch.object(PrestoQueryEngine, '_connect', return_value=MagicMock())
    def setUp(self, connect):
        self.presto = PrestoQueryEngine('1/XXX', 'sample_datasets')
        self.assertTrue(connect.called)

    def test_cursor(self):
        self.presto.cursor()
        self.assertTrue(self.presto.engine.cursor.called)

    def test_close(self):
        self.presto.close()
        self.assertTrue(self.presto.engine.close.called)


class HiveQueryEngineTestCase(unittest.TestCase):

    @patch.object(HiveQueryEngine, '_connect', return_value=MagicMock())
    def setUp(self, connect):
        self.hive = HiveQueryEngine('1/XXX', 'sample_datasets')
        self.assertTrue(connect.called)

    def test_cursor(self):
        self.hive.cursor()
        self.assertTrue(self.hive.engine.cursor.called)

    def test_close(self):
        self.hive.close()
        self.assertTrue(self.hive.engine.close.called)
