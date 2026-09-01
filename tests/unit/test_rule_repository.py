import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import UUID

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.api.config import RuleStoreSettings
from log_collector.api.rule_models import RuleCreateRequest, RuleUpdateRequest
from log_collector.api.rule_repository import MongoRuleRepository
from log_collector.storage import MongoDBSettings


RULE_ID = UUID("45302f50-35e0-4da8-b767-aaa647004637")
LOCAL_TIME = datetime(
    2026,
    8,
    24,
    13,
    30,
    tzinfo=timezone(timedelta(hours=3)),
)
UTC_TIME = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


def make_rule(model: type[RuleCreateRequest] | type[RuleUpdateRequest]):
    return model.model_validate(
        {
            "name": "SSH Login Failed",
            "enabled": True,
            "sourceIp": "192.168.1.15",
            "conditions": [
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": "login failed",
                }
            ],
            "actions": ["Alarm", "Email"],
            "emailTo": ["soc@example.com"],
        }
    )


class MongoRuleRepositoryTests(unittest.IsolatedAsyncioTestCase):
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

        self.insert_result = MagicMock()
        self.insert_result.acknowledged = True
        self.delete_result = MagicMock()
        self.delete_result.acknowledged = True
        self.delete_result.deleted_count = 1

        self.collection = MagicMock(spec=AsyncCollection)
        self.collection.create_index = AsyncMock()
        self.collection.find.return_value = self.cursor
        self.collection.find_one = AsyncMock(return_value=None)
        self.collection.insert_one = AsyncMock(return_value=self.insert_result)
        self.collection.find_one_and_update = AsyncMock(return_value=None)
        self.collection.delete_one = AsyncMock(return_value=self.delete_result)

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
            "log_collector.api.rule_repository.AsyncMongoClient",
            new=self.client_factory,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    async def connected_repository(self) -> MongoRuleRepository:
        repository = MongoRuleRepository(
            self.database_settings,
            self.store_settings,
            clock=lambda: LOCAL_TIME,
            id_factory=lambda: RULE_ID,
        )
        await repository.connect()

        return repository

    async def test_connects_with_durable_writes_and_rule_indexes(self) -> None:
        repository = await self.connected_repository()

        self.client_factory.assert_called_once_with(
            "mongodb://database:27017/",
            serverSelectionTimeoutMS=7000,
            maxPoolSize=30,
            retryReads=True,
            retryWrites=True,
            tz_aware=True,
        )
        self.admin.command.assert_awaited_once_with("ping")
        collection_call = self.database.get_collection.call_args
        self.assertEqual(collection_call.args, ("security_rules",))
        self.assertEqual(
            collection_call.kwargs["write_concern"].document,
            {"w": "majority", "j": True},
        )
        self.collection.create_index.assert_has_awaits(
            [
                call(
                    [("name", ASCENDING), ("_id", ASCENDING)],
                    name="rule_name_ascending",
                ),
                call(
                    [("enabled", ASCENDING), ("updatedAt", DESCENDING)],
                    name="enabled_updated_at",
                ),
            ]
        )

        await repository.close()

        self.client.close.assert_awaited_once()

    async def test_connect_is_idempotent(self) -> None:
        repository = MongoRuleRepository(
            self.database_settings,
            self.store_settings,
        )

        await asyncio.gather(repository.connect(), repository.connect())

        self.client_factory.assert_called_once()

    async def test_closes_client_when_index_creation_fails(self) -> None:
        repository = MongoRuleRepository(
            self.database_settings,
            self.store_settings,
        )
        self.collection.create_index.side_effect = RuntimeError("index failed")

        with self.assertRaisesRegex(RuntimeError, "index failed"):
            await repository.connect()

        self.client.close.assert_awaited_once()

    async def test_lists_rules_in_stable_name_order(self) -> None:
        documents = [{"_id": "rule-1"}, {"_id": "rule-2"}]
        self.cursor.to_list.return_value = documents
        repository = await self.connected_repository()

        result = await repository.list_rules()

        self.assertEqual(result, documents)
        self.collection.find.assert_called_once_with({})
        self.cursor.sort.assert_called_once_with(
            [("name", ASCENDING), ("_id", ASCENDING)]
        )
        self.cursor.to_list.assert_awaited_once_with(length=None)

    async def test_gets_rule_by_identifier(self) -> None:
        self.collection.find_one.return_value = {"_id": str(RULE_ID)}
        repository = await self.connected_repository()

        result = await repository.get_rule(str(RULE_ID))

        self.assertEqual(result, {"_id": str(RULE_ID)})
        self.collection.find_one.assert_awaited_once_with({"_id": str(RULE_ID)})

    async def test_creates_rule_with_identifier_and_utc_timestamps(self) -> None:
        repository = await self.connected_repository()
        rule = make_rule(RuleCreateRequest)

        document = await repository.create_rule(rule)

        self.assertEqual(document["_id"], str(RULE_ID))
        self.assertEqual(document["name"], "SSH Login Failed")
        self.assertEqual(document["conditions"][0]["field"], "rawLog")
        self.assertEqual(document["createdAt"], UTC_TIME)
        self.assertEqual(document["updatedAt"], UTC_TIME)
        self.collection.insert_one.assert_awaited_once_with(document)

    async def test_replaces_mutable_fields_and_preserves_creation_time(
        self,
    ) -> None:
        repository = await self.connected_repository()
        rule = make_rule(RuleUpdateRequest)
        returned_document = {
            "_id": str(RULE_ID),
            "createdAt": datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
            "updatedAt": UTC_TIME,
            **rule.model_dump(),
        }
        self.collection.find_one_and_update.return_value = returned_document

        result = await repository.update_rule(str(RULE_ID), rule)

        self.assertEqual(result, returned_document)
        update_call = self.collection.find_one_and_update.await_args
        self.assertEqual(update_call.args[0], {"_id": str(RULE_ID)})
        self.assertEqual(update_call.args[1]["$set"]["updatedAt"], UTC_TIME)
        self.assertNotIn("createdAt", update_call.args[1]["$set"])
        self.assertEqual(
            update_call.kwargs["return_document"],
            ReturnDocument.AFTER,
        )

    async def test_deletes_existing_rule(self) -> None:
        repository = await self.connected_repository()

        deleted = await repository.delete_rule(str(RULE_ID))

        self.assertTrue(deleted)
        self.collection.delete_one.assert_awaited_once_with({"_id": str(RULE_ID)})

    async def test_reports_missing_rule_during_update_and_delete(self) -> None:
        repository = await self.connected_repository()
        rule = make_rule(RuleUpdateRequest)
        self.delete_result.deleted_count = 0

        updated = await repository.update_rule(str(RULE_ID), rule)
        deleted = await repository.delete_rule(str(RULE_ID))

        self.assertIsNone(updated)
        self.assertFalse(deleted)

    async def test_rejects_unacknowledged_writes(self) -> None:
        repository = await self.connected_repository()
        rule = make_rule(RuleCreateRequest)
        self.insert_result.acknowledged = False

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await repository.create_rule(rule)

        self.delete_result.acknowledged = False

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await repository.delete_rule(str(RULE_ID))

    async def test_operations_require_connection(self) -> None:
        repository = MongoRuleRepository(
            self.database_settings,
            self.store_settings,
        )

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await repository.list_rules()

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await repository.ping()
