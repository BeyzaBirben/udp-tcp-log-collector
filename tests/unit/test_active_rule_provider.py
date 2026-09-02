import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.api.config import RuleStoreSettings
from log_collector.correlation import MongoActiveRuleProvider
from log_collector.storage import MongoDBSettings


class MongoActiveRuleProviderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.database_settings = MongoDBSettings(
            uri="mongodb://database:27017/",
            database="log_collection",
            collection="raw_logs",
            server_selection_timeout_ms=7000,
            max_pool_size=30,
        )
        self.store_settings = RuleStoreSettings(collection="security_rules")

        self.cursor = MagicMock()
        self.cursor.sort.return_value = self.cursor
        self.cursor.to_list = AsyncMock(return_value=[])

        self.collection = MagicMock(spec=AsyncCollection)
        self.collection.find.return_value = self.cursor

        self.database = MagicMock()
        self.database.get_collection.return_value = self.collection

        self.admin = MagicMock()
        self.admin.command = AsyncMock(return_value={"ok": 1})

        self.client = MagicMock(spec=AsyncMongoClient)
        self.client.admin = self.admin
        self.client.get_database.return_value = self.database
        self.client.close = AsyncMock()

        self.client_factory = MagicMock(return_value=self.client)
        patcher = patch(
            "log_collector.correlation.AsyncMongoClient",
            new=self.client_factory,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_provider(self) -> MongoActiveRuleProvider:
        return MongoActiveRuleProvider(
            self.database_settings,
            self.store_settings,
        )

    async def test_connects_to_rule_collection_for_reads(self) -> None:
        provider = self.make_provider()

        await provider.connect()

        self.client_factory.assert_called_once_with(
            "mongodb://database:27017/",
            serverSelectionTimeoutMS=7000,
            maxPoolSize=30,
            retryReads=True,
            tz_aware=True,
        )
        self.admin.command.assert_awaited_once_with("ping")
        self.client.get_database.assert_called_once_with("log_collection")
        self.database.get_collection.assert_called_once_with("security_rules")
        self.assertFalse(self.collection.create_index.called)

    async def test_connect_is_idempotent(self) -> None:
        provider = self.make_provider()

        await asyncio.gather(provider.connect(), provider.connect())

        self.client_factory.assert_called_once()

    async def test_closes_client_when_connection_fails(self) -> None:
        provider = self.make_provider()
        self.admin.command.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await provider.connect()

        self.client.close.assert_awaited_once()

    async def test_loads_only_active_rules_in_stable_order(self) -> None:
        documents = [{"_id": "rule-1"}, {"_id": "rule-2"}]
        self.cursor.to_list.return_value = documents
        provider = self.make_provider()

        await provider.connect()
        result = await provider.load_active_rules()

        self.assertEqual(result, documents)
        self.collection.find.assert_called_once_with({"enabled": True})
        self.cursor.sort.assert_called_once_with(
            [("updatedAt", DESCENDING), ("_id", ASCENDING)]
        )
        self.cursor.to_list.assert_awaited_once_with(length=None)

    async def test_operations_require_connection_and_close_is_idempotent(self) -> None:
        provider = self.make_provider()

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await provider.load_active_rules()

        await provider.connect()
        await provider.close()
        await provider.close()

        self.client.close.assert_awaited_once()
