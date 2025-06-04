import unittest
from unittest.mock import MagicMock, patch

import tdclient
import trino

from pytd import __version__
from pytd.query_engine import HiveQueryEngine, PrestoQueryEngine


class QueryEngineEndpointSchemeTestCase(unittest.TestCase):
    def test_presto_endpoint(self):
        presto = PrestoQueryEngine(
            "1/XXX", "api.treasuredata.com", "sample_datasets", True
        )
        self.assertEqual(presto.endpoint, "https://api.treasuredata.com")

        presto = PrestoQueryEngine(
            "1/XXX", "http://api.treasuredata.com", "sample_datasets", True
        )
        self.assertEqual(presto.endpoint, "http://api.treasuredata.com")

        presto = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com", "sample_datasets", True
        )
        self.assertEqual(presto.endpoint, "https://api.treasuredata.com")

    def test_hive_endpoint(self):
        hive = HiveQueryEngine("1/XXX", "api.treasuredata.com", "sample_datasets", True)
        self.assertEqual(hive.endpoint, "https://api.treasuredata.com")

        hive = HiveQueryEngine(
            "1/XXX", "http://api.treasuredata.com", "sample_datasets", True
        )
        self.assertEqual(hive.endpoint, "http://api.treasuredata.com")

        hive = HiveQueryEngine(
            "1/XXX", "https://api.treasuredata.com", "sample_datasets", True
        )
        self.assertEqual(hive.endpoint, "https://api.treasuredata.com")


class PrestoQueryEngineTestCase(unittest.TestCase):
    @patch.object(
        PrestoQueryEngine, "_connect", return_value=(MagicMock(), MagicMock())
    )
    def setUp(self, connect):
        self.presto = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", True
        )
        self.assertTrue(connect.called)
        self.assertEqual(self.presto.executed, None)

    def test_user_agent(self):
        ua = self.presto.user_agent
        self.assertEqual(
            ua,
            (
                f"pytd/{__version__} "
                f"(trino/{trino.__version__}; "
                f"tdclient/{tdclient.__version__})"
            ),
        )

    def test_presto_api_host(self):
        host = self.presto.presto_api_host
        self.assertEqual(host, "api-presto.treasuredata.com")

    def test_create_header(self):
        presto_no_header = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", False
        )
        self.assertEqual(presto_no_header.create_header("foo"), "")

        ua = self.presto.user_agent

        header = self.presto.create_header()
        self.assertEqual(header, f"-- client: {ua}\n")

        header = self.presto.create_header("foo")
        self.assertEqual(header, f"-- client: {ua}\n-- foo\n")

        header = self.presto.create_header(["foo", "bar"])
        self.assertEqual(header, f"-- client: {ua}\n-- foo\n-- bar\n")

    def test_cursor(self):
        self.presto.cursor()
        self.assertTrue(self.presto.trino_connection.cursor.called)

    def test_cursor_tdclient(self):
        self.presto.cursor(force_tdclient=True)
        self.assertTrue(self.presto.tdclient_connection.cursor.called)

    def test_cursor_with_params(self):
        self.presto.cursor(priority="LOW")
        self.assertTrue(self.presto.tdclient_connection.cursor.called)

    def test_cursor_with_unknown_params(self):
        with self.assertRaises(RuntimeError):
            self.presto.cursor(foo="LOW")

    def test_close(self):
        self.presto.close()
        self.assertTrue(self.presto.trino_connection.close.called)
        self.assertTrue(self.presto.tdclient_connection.close.called)


class HiveQueryEngineTestCase(unittest.TestCase):
    @patch.object(HiveQueryEngine, "_connect", return_value=MagicMock())
    def setUp(self, connect):
        self.hive = HiveQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", True
        )
        self.assertTrue(connect.called)
        self.assertEqual(self.hive.executed, None)

    def test_user_agent(self):
        hive_no_header = HiveQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", False
        )
        self.assertEqual(hive_no_header.create_header("foo"), "")

        ua = self.hive.user_agent
        self.assertEqual(ua, f"pytd/{__version__} (tdclient/{tdclient.__version__})")

    def test_create_header(self):
        ua = self.hive.user_agent

        header = self.hive.create_header()
        self.assertEqual(header, f"-- client: {ua}\n")

        header = self.hive.create_header("foo")
        self.assertEqual(header, f"-- client: {ua}\n-- foo\n")

        header = self.hive.create_header(["foo", "bar"])
        self.assertEqual(header, f"-- client: {ua}\n-- foo\n-- bar\n")

    def test_cursor(self):
        self.hive.cursor()
        self.assertTrue(self.hive.engine.cursor.called)

        # the `force_tdclient` flag has no effect for HiveQueryEngine
        self.hive.cursor(force_tdclient=False)
        self.assertTrue(self.hive.engine.cursor.called)

    def test_close(self):
        self.hive.close()
        self.assertTrue(self.hive.engine.close.called)
