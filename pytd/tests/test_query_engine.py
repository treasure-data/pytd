import unittest
from unittest.mock import MagicMock, patch

import tdclient
import trino

from pytd import __version__
from pytd.query_engine import CustomTrinoCursor, HiveQueryEngine, PrestoQueryEngine


class DummyCustomTrinoCursor(CustomTrinoCursor):
    """Test version of CustomTrinoCursor that accepts mock objects."""

    def __init__(self, connection, request, user_agent, legacy_primitive_types=False):
        # For testing: skip trino's connection validation
        self._connection = connection
        self._request = request
        self._legacy_primitive_types = legacy_primitive_types
        self._query = None
        self._iterator = None
        self._custom_user_agent = user_agent


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
    @patch.object(PrestoQueryEngine, "_connect")
    def setUp(self, connect_mock):
        # Create mock connections
        mock_trino_connection = MagicMock()
        mock_tdclient_connection = MagicMock()

        # Set up mock trino connection to return a cursor with _request attribute
        mock_cursor = MagicMock()
        mock_cursor._request = MagicMock()
        mock_trino_connection.cursor.return_value = mock_cursor

        connect_mock.return_value = (mock_trino_connection, mock_tdclient_connection)

        self.presto = PrestoQueryEngine(
            "1/XXX", "https://api.treasuredata.com/", "sample_datasets", True
        )
        self.assertTrue(connect_mock.called)
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
        # Patch CustomTrinoCursor creation to use our testable version
        with patch("pytd.query_engine.CustomTrinoCursor", DummyCustomTrinoCursor):
            cursor = self.presto.cursor()
            # Verify it's our custom cursor (testable version)
            self.assertIsInstance(cursor, DummyCustomTrinoCursor)
            # Verify custom user agent is set
            self.assertEqual(cursor._custom_user_agent, self.presto.user_agent)

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
