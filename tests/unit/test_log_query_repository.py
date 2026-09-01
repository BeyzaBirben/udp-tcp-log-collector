import re
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.api.repository import LogPage, LogQuery, MongoLogQueryRepository
from log_collector.storage import MongoDBSettings


class MongoLogQueryRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.settings = MongoDBSettings(
            uri="mongodb://database:27017/",
            database="log_collection",
            collection="raw_logs",
            server_selection_timeout_ms=7000,
            max_pool_size=30,
        )
        self.cursor = MagicMock()
        self.cursor.to_list = AsyncMock(return_value=[])

        self.collection = MagicMock(spec=AsyncCollection)
        self.collection.aggregate = AsyncMock(return_value=self.cursor)
        self.collection.find_one = AsyncMock(return_value=None)

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
            "log_collector.api.repository.AsyncMongoClient",
            new=self.client_factory,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    async def connected_repository(self) -> MongoLogQueryRepository:
        repository = MongoLogQueryRepository(self.settings)
        await repository.connect()

        return repository

    async def test_connects_as_read_only_query_repository(self) -> None:
        repository = await self.connected_repository()

        self.client_factory.assert_called_once_with(
            "mongodb://database:27017/",
            serverSelectionTimeoutMS=7000,
            maxPoolSize=30,
            retryReads=True,
            tz_aware=True,
        )
        self.admin.command.assert_awaited_once_with("ping")
        self.client.get_database.assert_called_once_with("log_collection")
        self.database.get_collection.assert_called_once_with("raw_logs")
        self.assertFalse(self.collection.create_index.called)

        await repository.close()

        self.client.close.assert_awaited_once()

    async def test_lists_logs_with_server_side_filter_count_and_pagination(
        self,
    ) -> None:
        document = {"_id": "log-1"}
        self.cursor.to_list.return_value = [
            {"items": [document], "metadata": [{"total": 17}]}
        ]
        repository = await self.connected_repository()
        query = LogQuery(
            start=datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
            end=datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
            source_ip="192.168.1.15",
            text="failed.*[root]",
        )

        result = await repository.list_logs(query, page=3, page_size=5)

        self.assertEqual(result, LogPage([document], 17))
        pipeline = self.collection.aggregate.await_args.args[0]
        self.assertEqual(
            pipeline[0],
            {
                "$match": {
                    "receivedAt": {
                        "$gte": query.start,
                        "$lte": query.end,
                    },
                    "sourceIp": "192.168.1.15",
                    "rawLog": {
                        "$regex": re.escape("failed.*[root]"),
                        "$options": "i",
                    },
                }
            },
        )
        facet = pipeline[1]["$facet"]
        self.assertEqual(
            facet["items"],
            [
                {"$sort": {"receivedAt": -1, "_id": -1}},
                {"$skip": 10},
                {"$limit": 5},
            ],
        )
        self.assertEqual(facet["metadata"], [{"$count": "total"}])
        self.cursor.to_list.assert_awaited_once_with(length=1)

    async def test_returns_empty_page_when_aggregation_has_no_result(self) -> None:
        repository = await self.connected_repository()
        query = LogQuery(
            start=datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
            end=datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
        )

        result = await repository.list_logs(query, page=1, page_size=50)

        self.assertEqual(result, LogPage([], 0))

    async def test_gets_one_log_by_identifier(self) -> None:
        self.collection.find_one.return_value = {"_id": "log-1"}
        repository = await self.connected_repository()

        result = await repository.get_log("log-1")

        self.assertEqual(result, {"_id": "log-1"})
        self.collection.find_one.assert_awaited_once_with({"_id": "log-1"})

    async def test_aggregates_source_summaries_in_database(self) -> None:
        sources = [
            {
                "sourceIp": "192.168.1.15",
                "logCount": 5,
                "firstSeen": datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
                "lastSeen": datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
            }
        ]
        self.cursor.to_list.return_value = sources
        repository = await self.connected_repository()

        result = await repository.list_sources()

        self.assertEqual(result, sources)
        pipeline = self.collection.aggregate.await_args.args[0]
        self.assertEqual(
            pipeline[0]["$group"],
            {
                "_id": "$sourceIp",
                "logCount": {"$sum": 1},
                "firstSeen": {"$min": "$receivedAt"},
                "lastSeen": {"$max": "$receivedAt"},
            },
        )
        self.assertEqual(pipeline[-1], {"$sort": {"sourceIp": 1}})
        self.cursor.to_list.assert_awaited_once_with(length=None)

    async def test_operations_require_connection(self) -> None:
        repository = MongoLogQueryRepository(self.settings)
        query = LogQuery(
            start=datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
            end=datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
        )

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await repository.list_logs(query, page=1, page_size=50)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await repository.ping()
