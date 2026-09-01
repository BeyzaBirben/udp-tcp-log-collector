import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from pymongo import DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.storage import MongoDBSettings


@dataclass(frozen=True, slots=True)
class LogQuery:
    start: datetime
    end: datetime
    source_ip: str | None = None
    text: str | None = None


@dataclass(frozen=True, slots=True)
class LogPage:
    documents: list[dict[str, Any]]
    total: int


class LogQueryRepository(Protocol):
    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def ping(self) -> None:
        pass

    async def list_logs(
        self,
        query: LogQuery,
        page: int,
        page_size: int,
    ) -> LogPage:
        pass

    async def get_log(self, log_id: str) -> dict[str, Any] | None:
        pass

    async def list_sources(self) -> list[dict[str, Any]]:
        pass


class MongoLogQueryRepository:
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
                retryReads=True,
                tz_aware=True,
            )

            try:
                await client.admin.command("ping")
                collection = client.get_database(
                    self._settings.database
                ).get_collection(self._settings.collection)
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
            raise RuntimeError("MongoDB query repository is not connected")

        await client.admin.command("ping")

    async def list_logs(
        self,
        query: LogQuery,
        page: int,
        page_size: int,
    ) -> LogPage:
        collection = self._require_collection()
        skip = (page - 1) * page_size
        pipeline = [
            {"$match": self._query_filter(query)},
            {
                "$facet": {
                    "items": [
                        {
                            "$sort": {
                                "receivedAt": DESCENDING,
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
        cursor = await collection.aggregate(pipeline)
        results = await cursor.to_list(length=1)

        if not results:
            return LogPage([], 0)

        result = results[0]
        metadata = result["metadata"]
        total = metadata[0]["total"] if metadata else 0

        return LogPage(documents=result["items"], total=total)

    async def get_log(self, log_id: str) -> dict[str, Any] | None:
        return await self._require_collection().find_one({"_id": log_id})

    async def list_sources(self) -> list[dict[str, Any]]:
        pipeline = [
            {
                "$group": {
                    "_id": "$sourceIp",
                    "logCount": {"$sum": 1},
                    "firstSeen": {"$min": "$receivedAt"},
                    "lastSeen": {"$max": "$receivedAt"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "sourceIp": "$_id",
                    "logCount": 1,
                    "firstSeen": 1,
                    "lastSeen": 1,
                }
            },
            {"$sort": {"sourceIp": 1}},
        ]
        cursor = await self._require_collection().aggregate(pipeline)

        return await cursor.to_list(length=None)

    @staticmethod
    def _query_filter(query: LogQuery) -> dict[str, Any]:
        filters: dict[str, Any] = {
            "receivedAt": {
                "$gte": query.start,
                "$lte": query.end,
            }
        }

        if query.source_ip is not None:
            filters["sourceIp"] = query.source_ip

        if query.text is not None:
            filters["rawLog"] = {
                "$regex": re.escape(query.text),
                "$options": "i",
            }

        return filters

    def _require_collection(self) -> AsyncCollection:
        if self._client is None or self._collection is None:
            raise RuntimeError("MongoDB query repository is not connected")

        return self._collection
