import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.write_concern import WriteConcern

from log_collector.api.config import RuleStoreSettings
from log_collector.api.rule_models import RuleCreateRequest, RuleUpdateRequest
from log_collector.storage import MongoDBSettings


class RuleRepository(Protocol):
    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def ping(self) -> None:
        pass

    async def list_rules(self) -> list[dict[str, Any]]:
        pass

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        pass

    async def create_rule(self, rule: RuleCreateRequest) -> dict[str, Any]:
        pass

    async def update_rule(
        self,
        rule_id: str,
        rule: RuleUpdateRequest,
    ) -> dict[str, Any] | None:
        pass

    async def delete_rule(self, rule_id: str) -> bool:
        pass


class MongoRuleRepository:
    def __init__(
        self,
        database_settings: MongoDBSettings,
        store_settings: RuleStoreSettings,
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
                    [("name", ASCENDING), ("_id", ASCENDING)],
                    name="rule_name_ascending",
                )
                await collection.create_index(
                    [("enabled", ASCENDING), ("updatedAt", DESCENDING)],
                    name="enabled_updated_at",
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
            raise RuntimeError("MongoDB rule repository is not connected")

        await client.admin.command("ping")

    async def list_rules(self) -> list[dict[str, Any]]:
        cursor = self._require_collection().find({}).sort(
            [("name", ASCENDING), ("_id", ASCENDING)]
        )

        return await cursor.to_list(length=None)

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        return await self._require_collection().find_one({"_id": rule_id})

    async def create_rule(self, rule: RuleCreateRequest) -> dict[str, Any]:
        rule_id = str(self._id_factory())
        now = self._current_time()
        document = self._document(rule_id, rule, now)
        result = await self._require_collection().insert_one(document)

        if not result.acknowledged:
            raise RuntimeError("MongoDB did not acknowledge the rule write")

        return document

    async def update_rule(
        self,
        rule_id: str,
        rule: RuleUpdateRequest,
    ) -> dict[str, Any] | None:
        updated_fields = rule.model_dump()
        updated_fields["updatedAt"] = self._current_time()

        return await self._require_collection().find_one_and_update(
            {"_id": rule_id},
            {"$set": updated_fields},
            return_document=ReturnDocument.AFTER,
        )

    async def delete_rule(self, rule_id: str) -> bool:
        result = await self._require_collection().delete_one({"_id": rule_id})

        if not result.acknowledged:
            raise RuntimeError("MongoDB did not acknowledge the rule deletion")

        return result.deleted_count == 1

    @staticmethod
    def _document(
        rule_id: str,
        rule: RuleCreateRequest,
        now: datetime,
    ) -> dict[str, Any]:
        document = rule.model_dump()
        document["_id"] = rule_id
        document["createdAt"] = now
        document["updatedAt"] = now

        return document

    def _current_time(self) -> datetime:
        value = self._clock()

        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError(
                "Rule repository clock must return a timezone-aware value"
            )

        return value.astimezone(timezone.utc)

    def _require_collection(self) -> AsyncCollection:
        if self._client is None or self._collection is None:
            raise RuntimeError("MongoDB rule repository is not connected")

        return self._collection


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
