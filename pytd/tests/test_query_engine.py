from pytd.query_engine import PrestoQueryEngine, HiveQueryEngine
from pytd.version import __version__
import prestodb
import tdclient

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
        self.presto = PrestoQueryEngine('1/XXX', 'https://api.treasuredata.com/', 'sample_datasets', True)
        self.assertTrue(connect.called)

    def test_create_header(self):
        ua = self.presto.get_user_agent()

        header = self.presto.create_header()
        self.assertEquals(header, "-- client: {0}\n".format(ua))

        header = self.presto.create_header('foo')
        self.assertEquals(header, "-- client: {0}\n-- foo\n".format(ua))

        header = self.presto.create_header(['foo', 'bar'])
        self.assertEquals(header, "-- client: {0}\n-- foo\n-- bar\n".format(ua))

    def test_get_user_agent(self):
        ua = self.presto.get_user_agent()
        self.assertEquals(ua, 'pytd/%s (Presto; prestodb/%s)' % (__version__, prestodb.__version__))

    def test_cursor(self):
        self.presto.cursor()
        self.assertTrue(self.presto.engine.cursor.called)

    def test_close(self):
        self.presto.close()
        self.assertTrue(self.presto.engine.close.called)


class HiveQueryEngineTestCase(unittest.TestCase):

    @patch.object(HiveQueryEngine, '_connect', return_value=MagicMock())
    def setUp(self, connect):
        self.hive = HiveQueryEngine('1/XXX', 'https://api.treasuredata.com/', 'sample_datasets', True)
        self.assertTrue(connect.called)

    def test_create_header(self):
        ua = self.hive.get_user_agent()

        header = self.hive.create_header()
        self.assertEquals(header, "-- client: {0}\n".format(ua))

        header = self.hive.create_header('foo')
        self.assertEquals(header, "-- client: {0}\n-- foo\n".format(ua))

        header = self.hive.create_header(['foo', 'bar'])
        self.assertEquals(header, "-- client: {0}\n-- foo\n-- bar\n".format(ua))

    def test_get_user_agent(self):
        ua = self.hive.get_user_agent()
        self.assertEquals(ua, 'pytd/%s (Hive; tdclient/%s)' % (__version__, tdclient.__version__))

    def test_cursor(self):
        self.hive.cursor()
        self.assertTrue(self.hive.engine.cursor.called)

    def test_close(self):
        self.hive.close()
        self.assertTrue(self.hive.engine.close.called)
