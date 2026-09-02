import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.write_concern import WriteConcern

from log_collector.api.config import AlarmStoreSettings
from log_collector.storage import MongoDBSettings


@dataclass(frozen=True, slots=True)
class NewAlarm:
    rule_id: str
    rule_name: str
    source_ip: str
    log_id: str
    raw_log: str
    email_sent: bool = False


@dataclass(frozen=True, slots=True)
class AlarmQuery:
    start: datetime | None = None
    end: datetime | None = None
    rule_id: str | None = None
    source_ip: str | None = None


@dataclass(frozen=True, slots=True)
class AlarmPage:
    documents: list[dict[str, Any]]
    total: int


class AlarmRepository(Protocol):
    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def ping(self) -> None:
        pass

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, Any]:
        pass

    async def list_alarms(
        self,
        query: AlarmQuery,
        page: int,
        page_size: int,
    ) -> AlarmPage:
        pass

    async def get_alarm(self, alarm_id: str) -> dict[str, Any] | None:
        pass


class MongoAlarmRepository:
    def __init__(
        self,
        database_settings: MongoDBSettings,
        store_settings: AlarmStoreSettings,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], UUID] | None = None,
    ) -> None:
        self._database_settings = database_settings
        self._store_settings = store_settings
        self._clock = _utc_now if clock is None else clock
        self._id_factory = uuid4 if id_factory is None else id_factory
        self._client: AsyncMongoClient | None = None
        self._collection: AsyncCollection | None = None
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._client is not None and self._collection is not None:
                return

            client = AsyncMongoClient(
                self._database_settings.uri,
                serverSelectionTimeoutMS=(
                    self._database_settings.server_selection_timeout_ms
                ),
                maxPoolSize=self._database_settings.max_pool_size,
                retryReads=True,
                retryWrites=True,
                tz_aware=True,
            )

            try:
                await client.admin.command("ping")
                collection = client.get_database(
                    self._database_settings.database
                ).get_collection(
                    self._store_settings.collection,
                    write_concern=WriteConcern(w="majority", j=True),
                )
                await collection.create_index(
                    [("createdAt", DESCENDING), ("_id", DESCENDING)],
                    name="alarm_created_at_descending",
                )
                await collection.create_index(
                    [
                        ("ruleId", ASCENDING),
                        ("createdAt", DESCENDING),
                        ("_id", DESCENDING),
                    ],
                    name="alarm_rule_created_at",
                )
                await collection.create_index(
                    [
                        ("sourceIp", ASCENDING),
                        ("createdAt", DESCENDING),
                        ("_id", DESCENDING),
                    ],
                    name="alarm_source_created_at",
                )
            except Exception:
                await client.close()
                raise

            self._client = client
            self._collection = collection

    async def close(self) -> None:
        async with self._connection_lock:
            client = self._client
            self._client = None
            self._collection = None

            if client is not None:
                await client.close()

    async def ping(self) -> None:
        client = self._client

        if client is None:
            raise RuntimeError("MongoDB alarm repository is not connected")

        await client.admin.command("ping")

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, Any]:
        document = {
            "_id": str(self._id_factory()),
            "ruleId": alarm.rule_id,
            "ruleName": alarm.rule_name,
            "sourceIp": alarm.source_ip,
            "logId": alarm.log_id,
            "rawLog": alarm.raw_log,
            "createdAt": self._current_time(),
            "emailSent": alarm.email_sent,
        }
        result = await self._require_collection().insert_one(document)

        if not result.acknowledged:
            raise RuntimeError("MongoDB did not acknowledge the alarm write")

        return document

    async def list_alarms(
        self,
        query: AlarmQuery,
        page: int,
        page_size: int,
    ) -> AlarmPage:
        skip = (page - 1) * page_size
        pipeline = [
            {"$match": self._query_filter(query)},
            {
                "$facet": {
                    "items": [
                        {
                            "$sort": {
                                "createdAt": DESCENDING,
                                "_id": DESCENDING,
                            }
                        },
                        {"$skip": skip},
                        {"$limit": page_size},
                    ],
                    "metadata": [{"$count": "total"}],
                }
            },
        ]
        cursor = await self._require_collection().aggregate(pipeline)
        results = await cursor.to_list(length=1)

        if not results:
            return AlarmPage([], 0)

        result = results[0]
        metadata = result["metadata"]
        total = metadata[0]["total"] if metadata else 0

        return AlarmPage(documents=result["items"], total=total)

    async def get_alarm(self, alarm_id: str) -> dict[str, Any] | None:
        return await self._require_collection().find_one({"_id": alarm_id})

    @staticmethod
    def _query_filter(query: AlarmQuery) -> dict[str, Any]:
        filters: dict[str, Any] = {}
        created_at: dict[str, datetime] = {}

        if query.start is not None:
            created_at["$gte"] = query.start

        if query.end is not None:
            created_at["$lte"] = query.end

        if created_at:
            filters["createdAt"] = created_at

        if query.rule_id is not None:
            filters["ruleId"] = query.rule_id

        if query.source_ip is not None:
            filters["sourceIp"] = query.source_ip

        return filters

    def _current_time(self) -> datetime:
        value = self._clock()

        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError(
                "Alarm repository clock must return a timezone-aware value"
            )

        return value.astimezone(timezone.utc)

    def _require_collection(self) -> AsyncCollection:
        if self._client is None or self._collection is None:
            raise RuntimeError("MongoDB alarm repository is not connected")

        return self._collection


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
