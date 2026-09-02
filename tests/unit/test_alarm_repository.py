import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import UUID

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.api.alarm_repository import (
    AlarmPage,
    AlarmQuery,
    MongoAlarmRepository,
    NewAlarm,
)
from log_collector.api.config import AlarmStoreSettings
from log_collector.storage import MongoDBSettings


NOW = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)
ALARM_ID = UUID("d893cf4c-8f96-46d0-945b-8ae4eb73e583")
SECOND_ALARM_ID = UUID("c2d6b991-3dcb-4b86-ac3c-e06e2df11ee2")


def new_alarm() -> NewAlarm:
    return NewAlarm(
        rule_id="45302f50-35e0-4da8-b767-aaa647004637",
        rule_name="SSH Login Failed",
        source_ip="192.168.1.15",
        log_id="8c831afa-2695-4cec-a19c-06caa73180a7",
        raw_log="sshd: login failed",
    )


class MongoAlarmRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.database_settings = MongoDBSettings(
            uri="mongodb://database:27017/",
            database="log_collection",
            collection="raw_logs",
            server_selection_timeout_ms=7000,
            max_pool_size=30,
        )
        self.store_settings = AlarmStoreSettings(collection="security_alarms")

        self.cursor = MagicMock()
        self.cursor.to_list = AsyncMock(return_value=[])

        self.collection = MagicMock(spec=AsyncCollection)
        self.collection.create_index = AsyncMock()
        self.collection.insert_one = AsyncMock()
        self.collection.update_one = AsyncMock()
        self.collection.aggregate = AsyncMock(return_value=self.cursor)
        self.collection.find_one = AsyncMock(return_value=None)

        self.insert_result = MagicMock()
        self.insert_result.acknowledged = True
        self.collection.insert_one.return_value = self.insert_result

        self.update_result = MagicMock()
        self.update_result.acknowledged = True
        self.update_result.matched_count = 1
        self.collection.update_one.return_value = self.update_result

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
            "log_collector.api.alarm_repository.AsyncMongoClient",
            new=self.client_factory,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        identifiers = iter((ALARM_ID, SECOND_ALARM_ID))
        self.repository = MongoAlarmRepository(
            self.database_settings,
            self.store_settings,
            clock=lambda: NOW,
            id_factory=lambda: next(identifiers),
        )

    async def test_connects_and_creates_query_indexes(self) -> None:
        await self.repository.connect()

        self.client_factory.assert_called_once_with(
            "mongodb://database:27017/",
            serverSelectionTimeoutMS=7000,
            maxPoolSize=30,
            retryReads=True,
            retryWrites=True,
            tz_aware=True,
        )
        self.admin.command.assert_awaited_once_with("ping")
        self.client.get_database.assert_called_once_with("log_collection")
        collection_call = self.database.get_collection.call_args
        self.assertEqual(collection_call.args, ("security_alarms",))
        self.assertEqual(
            collection_call.kwargs["write_concern"].document,
            {"w": "majority", "j": True},
        )
        self.assertEqual(
            self.collection.create_index.await_args_list,
            [
                call(
                    [("createdAt", -1), ("_id", -1)],
                    name="alarm_created_at_descending",
                ),
                call(
                    [("ruleId", 1), ("createdAt", -1), ("_id", -1)],
                    name="alarm_rule_created_at",
                ),
                call(
                    [("sourceIp", 1), ("createdAt", -1), ("_id", -1)],
                    name="alarm_source_created_at",
                ),
            ],
        )

    async def test_connect_is_idempotent(self) -> None:
        await asyncio.gather(
            self.repository.connect(),
            self.repository.connect(),
        )

        self.client_factory.assert_called_once()

    async def test_closes_client_when_connection_fails(self) -> None:
        self.admin.command.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await self.repository.connect()

        self.client.close.assert_awaited_once()

    async def test_creates_a_new_alarm_for_each_match(self) -> None:
        await self.repository.connect()

        first = await self.repository.create_alarm(new_alarm())
        second = await self.repository.create_alarm(new_alarm())

        self.assertEqual(first["_id"], str(ALARM_ID))
        self.assertEqual(second["_id"], str(SECOND_ALARM_ID))
        self.assertEqual(first["ruleName"], "SSH Login Failed")
        self.assertEqual(first["sourceIp"], "192.168.1.15")
        self.assertEqual(first["rawLog"], "sshd: login failed")
        self.assertEqual(first["createdAt"], NOW)
        self.assertFalse(first["emailSent"])
        self.assertEqual(self.collection.insert_one.await_count, 2)

    async def test_rejects_an_unacknowledged_alarm_write(self) -> None:
        await self.repository.connect()
        self.insert_result.acknowledged = False

        with self.assertRaisesRegex(RuntimeError, "acknowledge"):
            await self.repository.create_alarm(new_alarm())

    async def test_lists_with_server_side_filters_and_pagination(self) -> None:
        document = {"_id": str(ALARM_ID)}
        self.cursor.to_list.return_value = [
            {"items": [document], "metadata": [{"total": 12}]}
        ]
        await self.repository.connect()
        query = AlarmQuery(
            start=datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
            end=datetime(2026, 8, 24, 11, 0, tzinfo=timezone.utc),
            rule_id="45302f50-35e0-4da8-b767-aaa647004637",
            source_ip="192.168.1.15",
        )

        result = await self.repository.list_alarms(query, page=3, page_size=5)

        self.assertEqual(result, AlarmPage([document], 12))
        pipeline = self.collection.aggregate.await_args.args[0]
        self.assertEqual(
            pipeline[0],
            {
                "$match": {
                    "createdAt": {
                        "$gte": query.start,
                        "$lte": query.end,
                    },
                    "ruleId": query.rule_id,
                    "sourceIp": query.source_ip,
                }
            },
        )
        self.assertEqual(
            pipeline[1]["$facet"]["items"],
            [
                {"$sort": {"createdAt": -1, "_id": -1}},
                {"$skip": 10},
                {"$limit": 5},
            ],
        )
        self.assertEqual(
            pipeline[1]["$facet"]["metadata"],
            [{"$count": "total"}],
        )

    async def test_lists_without_filters_and_handles_an_empty_page(self) -> None:
        await self.repository.connect()

        result = await self.repository.list_alarms(
            AlarmQuery(),
            page=1,
            page_size=50,
        )

        self.assertEqual(result, AlarmPage([], 0))
        pipeline = self.collection.aggregate.await_args.args[0]
        self.assertEqual(pipeline[0], {"$match": {}})

    async def test_gets_one_alarm_by_identifier(self) -> None:
        document = {"_id": str(ALARM_ID)}
        self.collection.find_one.return_value = document
        await self.repository.connect()

        result = await self.repository.get_alarm(str(ALARM_ID))

        self.assertEqual(result, document)
        self.collection.find_one.assert_awaited_once_with(
            {"_id": str(ALARM_ID)}
        )

    async def test_marks_alarm_email_as_sent(self) -> None:
        await self.repository.connect()

        updated = await self.repository.mark_email_sent(str(ALARM_ID))

        self.assertTrue(updated)
        self.collection.update_one.assert_awaited_once_with(
            {"_id": str(ALARM_ID)},
            {"$set": {"emailSent": True}},
        )

    async def test_reports_missing_and_unacknowledged_email_status_updates(
        self,
    ) -> None:
        await self.repository.connect()
        self.update_result.matched_count = 0

        self.assertFalse(
            await self.repository.mark_email_sent(str(ALARM_ID))
        )

        self.update_result.acknowledged = False

        with self.assertRaisesRegex(RuntimeError, "acknowledge"):
            await self.repository.mark_email_sent(str(ALARM_ID))

    async def test_operations_require_connection_and_close_is_idempotent(
        self,
    ) -> None:
        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await self.repository.create_alarm(new_alarm())

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await self.repository.list_alarms(
                AlarmQuery(),
                page=1,
                page_size=50,
            )

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await self.repository.mark_email_sent(str(ALARM_ID))

        await self.repository.connect()
        await self.repository.close()
        await self.repository.close()

        self.client.close.assert_awaited_once()

    async def test_requires_a_timezone_aware_clock(self) -> None:
        repository = MongoAlarmRepository(
            self.database_settings,
            self.store_settings,
            clock=lambda: datetime(2026, 8, 24, 10, 30),
            id_factory=lambda: ALARM_ID,
        )
        await repository.connect()

        with self.assertRaisesRegex(RuntimeError, "timezone-aware"):
            await repository.create_alarm(new_alarm())
