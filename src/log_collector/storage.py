import asyncio
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timezone

from bson.binary import Binary
from pymongo import DESCENDING, AsyncMongoClient, UpdateOne
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.write_concern import WriteConcern

from log_collector.broker.message import BrokerMessage


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MongoDBSettings:
    uri: str = "mongodb://localhost:27017/"
    database: str = "log_collection"
    collection: str = "raw_logs"
    server_selection_timeout_ms: int = 5000
    max_pool_size: int = 20

    def __post_init__(self) -> None:
        required_values = {
            "MONGODB_URI": self.uri,
            "MONGODB_DATABASE": self.database,
            "MONGODB_COLLECTION": self.collection,
        }

        for variable_name, value in required_values.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{variable_name} must not be empty")

        if not self.uri.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("MONGODB_URI must be a valid MongoDB connection string")

        if (
            type(self.server_selection_timeout_ms) is not int
            or self.server_selection_timeout_ms <= 0
        ):
            raise ValueError("MONGODB_SERVER_SELECTION_TIMEOUT_MS must be positive")

        if type(self.max_pool_size) is not int or self.max_pool_size <= 0:
            raise ValueError("MONGODB_MAX_POOL_SIZE must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "MongoDBSettings":
        environment = os.environ if values is None else values

        return cls(
            uri=environment.get(
                "MONGODB_URI", "mongodb://localhost:27017/"
            ).strip(),
            database=environment.get("MONGODB_DATABASE", "log_collection").strip(),
            collection=environment.get("MONGODB_COLLECTION", "raw_logs").strip(),
            server_selection_timeout_ms=_positive_integer(
                environment,
                "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
                5000,
            ),
            max_pool_size=_positive_integer(
                environment,
                "MONGODB_MAX_POOL_SIZE",
                20,
            ),
        )


@dataclass(frozen=True, slots=True)
class PersistenceResult:
    attempted_count: int
    inserted_count: int
    duplicate_count: int

    def __post_init__(self) -> None:
        counts = (
            self.attempted_count,
            self.inserted_count,
            self.duplicate_count,
        )

        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("Persistence result counts must be non-negative integers")

        if self.inserted_count + self.duplicate_count != self.attempted_count:
            raise ValueError("Persistence result counts do not match attempted records")


class MongoLogRepository:
    def __init__(self, settings: MongoDBSettings) -> None:
        self._settings = settings
        self._client: AsyncMongoClient | None = None
        self._collection: AsyncCollection | None = None
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._client is not None and self._collection is not None:
                return

            client = AsyncMongoClient(
                self._settings.uri,
                serverSelectionTimeoutMS=self._settings.server_selection_timeout_ms,
                maxPoolSize=self._settings.max_pool_size,
                retryWrites=True,
                tz_aware=True,
            )

            try:
                await client.admin.command("ping")
                database = client.get_database(self._settings.database)
                collection = database.get_collection(
                    self._settings.collection,
                    write_concern=WriteConcern(w="majority", j=True),
                )
                await collection.create_index(
                    "messageId",
                    unique=True,
                    name="message_id_unique",
                )
                await collection.create_index(
                    [("receivedAt", DESCENDING)],
                    name="received_at_descending",
                )
            except Exception:
                await client.close()
                raise

            self._client = client
            self._collection = collection

            logger.info(
                "MongoDB repository connected to %s.%s",
                self._settings.database,
                self._settings.collection,
            )

    async def save_batch(
        self,
        messages: Sequence[BrokerMessage],
    ) -> PersistenceResult:
        collection = self._require_collection()

        if not messages:
            return PersistenceResult(0, 0, 0)

        operations = [self._upsert_operation(message) for message in messages]
        result = await collection.bulk_write(operations, ordered=False)

        if not result.acknowledged:
            raise RuntimeError("MongoDB did not acknowledge the batch write")

        inserted_count = result.upserted_count
        duplicate_count = result.matched_count

        if inserted_count + duplicate_count != len(operations):
            raise RuntimeError("MongoDB did not confirm every message in the batch")

        return PersistenceResult(
            attempted_count=len(operations),
            inserted_count=inserted_count,
            duplicate_count=duplicate_count,
        )

    async def close(self) -> None:
        async with self._connection_lock:
            client = self._client
            self._client = None
            self._collection = None

            if client is None:
                return

            await client.close()

            logger.info("MongoDB repository disconnected")

    @staticmethod
    def _document(message: BrokerMessage) -> dict[str, object]:
        document: dict[str, object] = dict(message.as_dict())
        document["_id"] = message.message_id
        document["receivedAt"] = message.received_at.astimezone(timezone.utc)
        document["rawLogBytes"] = Binary(message.raw_log)

        return document

    @classmethod
    def _upsert_operation(cls, message: BrokerMessage) -> UpdateOne:
        document = cls._document(message)
        document.pop("_id")

        return UpdateOne(
            {"_id": message.message_id},
            {"$setOnInsert": document},
            upsert=True,
        )

    def _require_collection(self) -> AsyncCollection:
        if self._client is None or self._collection is None:
            raise RuntimeError("MongoDB repository is not connected")

        return self._collection


def _positive_integer(
    environment: Mapping[str, str],
    variable_name: str,
    default: int,
) -> int:
    raw_value = environment.get(variable_name, str(default))

    try:
        value = int(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be a positive integer") from error

    if value <= 0:
        raise ValueError(f"{variable_name} must be a positive integer")

    return value