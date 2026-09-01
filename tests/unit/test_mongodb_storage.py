import asyncio
import unittest
from datetime import timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

from bson.binary import Binary
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, UpdateOne
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.broker import BrokerMessage
from log_collector.models import LogRecord
from log_collector.storage import (
    MongoDBSettings,
    MongoLogRepository,
    PersistenceResult,
)


class MongoDBSettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = MongoDBSettings.from_environment({})

        self.assertEqual(settings.uri, "mongodb://localhost:27017/")
        self.assertEqual(settings.database, "log_collection")
        self.assertEqual(settings.collection, "raw_logs")
        self.assertEqual(settings.server_selection_timeout_ms, 5000)
        self.assertEqual(settings.max_pool_size, 20)

    def test_loads_custom_configuration(self) -> None:
        settings = MongoDBSettings.from_environment(
            {
                "MONGODB_URI": " mongodb://database:27017/ ",
                "MONGODB_DATABASE": " security_logs ",
                "MONGODB_COLLECTION": " incoming_logs ",
                "MONGODB_SERVER_SELECTION_TIMEOUT_MS": "7500",
                "MONGODB_MAX_POOL_SIZE": "40",
            }
        )

        self.assertEqual(settings.uri, "mongodb://database:27017/")
        self.assertEqual(settings.database, "security_logs")
        self.assertEqual(settings.collection, "incoming_logs")
        self.assertEqual(settings.server_selection_timeout_ms, 7500)
        self.assertEqual(settings.max_pool_size, 40)

    def test_accepts_srv_connection_strings(self) -> None:
        settings = MongoDBSettings.from_environment(
            {"MONGODB_URI": "mongodb+srv://cluster.example.com/"}
        )

        self.assertEqual(settings.uri, "mongodb+srv://cluster.example.com/")

    def test_rejects_empty_configuration_values(self) -> None:
        for variable_name in (
            "MONGODB_URI",
            "MONGODB_DATABASE",
            "MONGODB_COLLECTION",
        ):
            with self.subTest(variable_name=variable_name):
                with self.assertRaisesRegex(ValueError, variable_name):
                    MongoDBSettings.from_environment({variable_name: "  "})

    def test_rejects_invalid_connection_strings(self) -> None:
        with self.assertRaisesRegex(ValueError, "MONGODB_URI"):
            MongoDBSettings.from_environment({"MONGODB_URI": "http://database"})

    def test_rejects_invalid_numeric_configuration(self) -> None:
        for variable_name in (
            "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
            "MONGODB_MAX_POOL_SIZE",
        ):
            for value in ("0", "-1", "1.5", "invalid"):
                with self.subTest(variable_name=variable_name, value=value):
                    with self.assertRaisesRegex(ValueError, variable_name):
                        MongoDBSettings.from_environment({variable_name: value})

    def test_rejects_boolean_numeric_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "MONGODB_SERVER_SELECTION"):
            MongoDBSettings(server_selection_timeout_ms=True)

        with self.assertRaisesRegex(ValueError, "MONGODB_MAX_POOL_SIZE"):
            MongoDBSettings(max_pool_size=True)

    def test_persistence_result_validates_counts(self) -> None:
        result = PersistenceResult(3, 2, 1)

        self.assertEqual(result.attempted_count, 3)
        self.assertEqual(result.inserted_count, 2)
        self.assertEqual(result.duplicate_count, 1)

        for counts in ((-1, 0, 0), (1, -1, 2), (1, 0, -1), (1, 1, 1)):
            with self.subTest(counts=counts):
                with self.assertRaises(ValueError):
                    PersistenceResult(*counts)


class MongoLogRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.settings = MongoDBSettings.from_environment(
            {
                "MONGODB_URI": "mongodb://database:27017/",
                "MONGODB_DATABASE": "log_collection",
                "MONGODB_COLLECTION": "raw_logs",
                "MONGODB_SERVER_SELECTION_TIMEOUT_MS": "7500",
                "MONGODB_MAX_POOL_SIZE": "30",
            }
        )

        self.result = MagicMock()
        self.result.acknowledged = True
        self.result.upserted_count = 1
        self.result.matched_count = 0

        self.collection = MagicMock(spec=AsyncCollection)
        self.collection.create_index = AsyncMock()
        self.collection.bulk_write = AsyncMock(return_value=self.result)

        self.database = MagicMock()
        self.database.get_collection = MagicMock(return_value=self.collection)

        self.admin = MagicMock()
        self.admin.command = AsyncMock(return_value={"ok": 1})

        self.client = MagicMock(spec=AsyncMongoClient)
        self.client.admin = self.admin
        self.client.get_database = MagicMock(return_value=self.database)
        self.client.close = AsyncMock()

        self.client_factory = MagicMock(return_value=self.client)
        patcher = patch(
            "log_collector.storage.AsyncMongoClient",
            new=self.client_factory,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_message(
        self,
        raw_log: bytes = b"mongodb-test",
        protocol: str = "UDP",
    ) -> BrokerMessage:
        record = LogRecord.capture(
            protocol=protocol,
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=raw_log,
        )

        return BrokerMessage.from_record(record)

    async def test_connects_with_acknowledged_durable_writes(self) -> None:
        repository = MongoLogRepository(self.settings)

        await repository.connect()

        self.client_factory.assert_called_once_with(
            "mongodb://database:27017/",
            serverSelectionTimeoutMS=7500,
            maxPoolSize=30,
            retryWrites=True,
            tz_aware=True,
        )
        self.admin.command.assert_awaited_once_with("ping")
        self.client.get_database.assert_called_once_with("log_collection")
        collection_call = self.database.get_collection.call_args

        self.assertEqual(collection_call.args, ("raw_logs",))
        self.assertEqual(
            collection_call.kwargs["write_concern"].document,
            {"w": "majority", "j": True},
        )

    async def test_creates_unique_message_and_timestamp_indexes(self) -> None:
        repository = MongoLogRepository(self.settings)

        await repository.connect()

        self.collection.create_index.assert_has_awaits(
            [
                call("messageId", unique=True, name="message_id_unique"),
                call(
                    [("receivedAt", DESCENDING)],
                    name="received_at_descending",
                ),
                call(
                    [("sourceIp", ASCENDING), ("receivedAt", DESCENDING)],
                    name="source_ip_received_at",
                ),
            ]
        )

    async def test_connect_is_idempotent(self) -> None:
        repository = MongoLogRepository(self.settings)

        await asyncio.gather(repository.connect(), repository.connect())

        self.client_factory.assert_called_once()

    async def test_closes_client_when_ping_fails(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.admin.command.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await repository.connect()

        self.client.close.assert_awaited_once()

    async def test_closes_client_when_index_creation_fails(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.collection.create_index.side_effect = RuntimeError("index failed")

        with self.assertRaisesRegex(RuntimeError, "index failed"):
            await repository.connect()

        self.client.close.assert_awaited_once()

    async def test_persists_raw_log_without_losing_binary_content(self) -> None:
        repository = MongoLogRepository(self.settings)
        message = self.make_message(b"raw-\x00\xff", protocol="TCP")

        await repository.connect()
        result = await repository.save_batch([message])

        bulk_call = self.collection.bulk_write.await_args
        operations = bulk_call.args[0]

        self.assertEqual(len(operations), 1)
        self.assertIsInstance(operations[0], UpdateOne)
        self.assertEqual(operations[0]._filter, {"_id": message.message_id})
        self.assertTrue(operations[0]._upsert)
        self.assertFalse(bulk_call.kwargs["ordered"])

        document = operations[0]._doc["$setOnInsert"]

        self.assertNotIn("_id", document)
        self.assertEqual(document["messageId"], message.message_id)
        self.assertEqual(document["protocol"], "TCP")
        self.assertEqual(document["sourceIp"], "127.0.0.1")
        self.assertEqual(document["sourcePort"], 55140)
        self.assertEqual(document["rawLog"], "raw-\x00\ufffd")
        self.assertEqual(document["rawLogBase64"], message.as_dict()["rawLogBase64"])
        self.assertEqual(document["rawLogSize"], len(message.raw_log))
        self.assertEqual(document["schemaVersion"], 1)
        self.assertIsInstance(document["rawLogBytes"], Binary)
        self.assertEqual(bytes(document["rawLogBytes"]), message.raw_log)
        self.assertEqual(document["receivedAt"].tzinfo, timezone.utc)
        self.assertEqual(result, PersistenceResult(1, 1, 0))

    async def test_persists_multiple_messages_in_one_bulk_write(self) -> None:
        repository = MongoLogRepository(self.settings)
        messages = [
            self.make_message(b"first"),
            self.make_message(b"second"),
            self.make_message(b"third"),
        ]
        self.result.upserted_count = 3

        await repository.connect()
        result = await repository.save_batch(messages)

        operations = self.collection.bulk_write.await_args.args[0]

        self.assertEqual(len(operations), 3)
        self.assertEqual(result, PersistenceResult(3, 3, 0))

    async def test_treats_existing_messages_as_idempotent_duplicates(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.result.upserted_count = 0
        self.result.matched_count = 1

        await repository.connect()
        result = await repository.save_batch([self.make_message()])

        self.assertEqual(result, PersistenceResult(1, 0, 1))

    async def test_reports_inserted_and_duplicate_messages_together(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.result.upserted_count = 2
        self.result.matched_count = 1

        await repository.connect()
        result = await repository.save_batch(
            [self.make_message(), self.make_message(), self.make_message()]
        )

        self.assertEqual(result, PersistenceResult(3, 2, 1))

    async def test_repeated_message_uses_same_document_identifier(self) -> None:
        repository = MongoLogRepository(self.settings)
        message = self.make_message()
        self.result.upserted_count = 1
        self.result.matched_count = 1

        await repository.connect()
        result = await repository.save_batch([message, message])

        operations = self.collection.bulk_write.await_args.args[0]

        self.assertEqual(operations[0]._filter, operations[1]._filter)
        self.assertEqual(result, PersistenceResult(2, 1, 1))

    async def test_empty_batches_do_not_call_mongodb(self) -> None:
        repository = MongoLogRepository(self.settings)

        await repository.connect()
        result = await repository.save_batch([])

        self.assertEqual(result, PersistenceResult(0, 0, 0))
        self.collection.bulk_write.assert_not_awaited()

    async def test_rejects_unacknowledged_database_writes(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.result.acknowledged = False

        await repository.connect()

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await repository.save_batch([self.make_message()])

    async def test_rejects_incomplete_database_results(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.result.upserted_count = 0
        self.result.matched_count = 0

        await repository.connect()

        with self.assertRaisesRegex(RuntimeError, "confirm every message"):
            await repository.save_batch([self.make_message()])

    async def test_propagates_database_write_failures(self) -> None:
        repository = MongoLogRepository(self.settings)
        self.collection.bulk_write.side_effect = RuntimeError("write failed")

        await repository.connect()

        with self.assertRaisesRegex(RuntimeError, "write failed"):
            await repository.save_batch([self.make_message()])

    async def test_requires_an_active_database_connection(self) -> None:
        repository = MongoLogRepository(self.settings)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await repository.save_batch([self.make_message()])

    async def test_close_is_idempotent(self) -> None:
        repository = MongoLogRepository(self.settings)

        await repository.connect()
        await repository.close()
        await repository.close()

        self.client.close.assert_awaited_once()

    async def test_can_reconnect_after_close(self) -> None:
        repository = MongoLogRepository(self.settings)

        await repository.connect()
        await repository.close()
        await repository.connect()

        self.assertEqual(self.client_factory.call_count, 2)
