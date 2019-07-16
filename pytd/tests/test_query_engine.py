import unittest
from unittest.mock import MagicMock, patch

import prestodb
import tdclient

from pytd.query_engine import HiveQueryEngine, PrestoQueryEngine
from pytd.version import __version__


class PrestoQueryEngineTestCase(unittest.TestCase):
    @patch.object(PrestoQueryEngine, "_connect", return_value=MagicMock())
    def setUp(self, connect):
        self.presto = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", True
        )
        self.assertTrue(connect.called)

    def test_user_agent(self):
        ua = self.presto.user_agent
        self.assertEqual(
            ua, "pytd/{0} (prestodb/{1})".format(__version__, prestodb.__version__)
        )

    def test_create_header(self):
        presto_no_header = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", False
        )
        self.assertEqual(presto_no_header.create_header("foo"), "")

        ua = self.presto.user_agent

        header = self.presto.create_header()
        self.assertEqual(header, "-- client: {0}\n".format(ua))

        header = self.presto.create_header("foo")
        self.assertEqual(header, "-- client: {0}\n-- foo\n".format(ua))

        header = self.presto.create_header(["foo", "bar"])
        self.assertEqual(header, "-- client: {0}\n-- foo\n-- bar\n".format(ua))

    def test_cursor(self):
        self.presto.cursor()
        self.assertTrue(self.presto.engine.cursor.called)

    def test_close(self):
        self.presto.close()
        self.assertTrue(self.presto.engine.close.called)


class HiveQueryEngineTestCase(unittest.TestCase):
    @patch.object(HiveQueryEngine, "_connect", return_value=MagicMock())
    def setUp(self, connect):
        self.hive = HiveQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", True
        )
        self.assertTrue(connect.called)

    def test_user_agent(self):
        hive_no_header = HiveQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", False
        )
        self.assertEqual(hive_no_header.create_header("foo"), "")

        ua = self.hive.user_agent
        self.assertEqual(
            ua, "pytd/{0} (tdclient/{1})".format(__version__, tdclient.__version__)
        )

    def test_create_header(self):
        ua = self.hive.user_agent

        header = self.hive.create_header()
        self.assertEqual(header, "-- client: {0}\n".format(ua))

        header = self.hive.create_header("foo")
        self.assertEqual(header, "-- client: {0}\n-- foo\n".format(ua))

        header = self.hive.create_header(["foo", "bar"])
        self.assertEqual(header, "-- client: {0}\n-- foo\n-- bar\n".format(ua))

    def test_cursor(self):
        self.hive.cursor()
        self.assertTrue(self.hive.engine.cursor.called)

    def test_close(self):
        self.hive.close()
        self.assertTrue(self.hive.engine.close.called)
