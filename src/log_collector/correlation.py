import asyncio
import logging
import math
import os
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from log_collector.api.config import RuleStoreSettings
from log_collector.api.rule_models import (
    RuleAction,
    RuleField,
    RuleOperator,
    rule_response_from_document,
)
from log_collector.broker import BrokerMessage
from log_collector.storage import MongoDBSettings


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CorrelationSettings:
    enabled: bool = True
    rule_refresh_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("CORRELATION_ENABLED must be a boolean")

        if (
            not math.isfinite(self.rule_refresh_seconds)
            or self.rule_refresh_seconds <= 0
        ):
            raise ValueError("CORRELATION_RULE_REFRESH_SECONDS must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "CorrelationSettings":
        environment = os.environ if values is None else values

        return cls(
            enabled=_boolean(environment, "CORRELATION_ENABLED", True),
            rule_refresh_seconds=_positive_float(
                environment,
                "CORRELATION_RULE_REFRESH_SECONDS",
                1.0,
            ),
        )


@dataclass(frozen=True, slots=True)
class CompiledCondition:
    field: RuleField
    operator: RuleOperator
    value: str
    pattern: re.Pattern[str] | None = None

    @classmethod
    def create(
        cls,
        field: RuleField,
        operator: RuleOperator,
        value: str,
    ) -> "CompiledCondition":
        pattern = re.compile(value) if operator == "regex" else None

        return cls(
            field=field,
            operator=operator,
            value=value,
            pattern=pattern,
        )

    def matches(self, message: BrokerMessage, raw_log: str) -> bool:
        actual_value = raw_log if self.field == "rawLog" else message.source_ip

        if self.operator == "contains":
            return self.value in actual_value

        if self.operator == "equals":
            return actual_value == self.value

        if self.operator == "startsWith":
            return actual_value.startswith(self.value)

        if self.operator == "regex" and self.pattern is not None:
            return self.pattern.search(actual_value) is not None

        raise RuntimeError(f"Unsupported correlation operator: {self.operator}")


@dataclass(frozen=True, slots=True)
class CompiledRule:
    rule_id: str
    name: str
    source_ip: str
    conditions: tuple[CompiledCondition, ...]
    actions: tuple[RuleAction, ...]
    email_to: tuple[str, ...]

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "CompiledRule":
        rule = rule_response_from_document(document)

        if not rule.enabled:
            raise ValueError("Correlation rule must be enabled")

        return cls(
            rule_id=rule.id,
            name=rule.name,
            source_ip=rule.sourceIp,
            conditions=tuple(
                CompiledCondition.create(
                    condition.field,
                    condition.operator,
                    condition.value,
                )
                for condition in rule.conditions
            ),
            actions=tuple(rule.actions),
            email_to=tuple(rule.emailTo),
        )

    def matches(self, message: BrokerMessage) -> bool:
        if message.source_ip != self.source_ip:
            return False

        raw_log = message.raw_log.decode("utf-8", errors="replace")

        return all(
            condition.matches(message, raw_log) for condition in self.conditions
        )


@dataclass(frozen=True, slots=True)
class CorrelationMatch:
    rule: CompiledRule
    message: BrokerMessage


class ActiveRuleProvider(Protocol):
    async def connect(self) -> None:
        pass

    async def load_active_rules(self) -> list[dict[str, Any]]:
        pass

    async def close(self) -> None:
        pass


class CorrelationMatchHandler(Protocol):
    async def connect(self) -> None:
        pass

    async def handle(self, match: CorrelationMatch) -> None:
        pass

    async def close(self) -> None:
        pass


class CorrelationProcessor(Protocol):
    async def connect(self) -> None:
        pass

    async def process(self, messages: Sequence[BrokerMessage]) -> int:
        pass

    async def close(self) -> None:
        pass


class MongoActiveRuleProvider:
    def __init__(
        self,
        database_settings: MongoDBSettings,
        store_settings: RuleStoreSettings,
    ) -> None:
        self._database_settings = database_settings
        self._store_settings = store_settings
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
                tz_aware=True,
            )

            try:
                await client.admin.command("ping")
                collection = client.get_database(
                    self._database_settings.database
                ).get_collection(self._store_settings.collection)
            except Exception:
                await client.close()
                raise

            self._client = client
            self._collection = collection

    async def load_active_rules(self) -> list[dict[str, Any]]:
        cursor = self._require_collection().find({"enabled": True}).sort(
            [("updatedAt", DESCENDING), ("_id", ASCENDING)]
        )

        return await cursor.to_list(length=None)

    async def close(self) -> None:
        async with self._connection_lock:
            client = self._client
            self._client = None
            self._collection = None

            if client is not None:
                await client.close()

    def _require_collection(self) -> AsyncCollection:
        if self._client is None or self._collection is None:
            raise RuntimeError("MongoDB active rule provider is not connected")

        return self._collection


class LoggingCorrelationMatchHandler:
    async def connect(self) -> None:
        pass

    async def handle(self, match: CorrelationMatch) -> None:
        logger.info(
            "Correlation rule matched rule_id=%s rule_name=%s message_id=%s",
            match.rule.rule_id,
            match.rule.name,
            match.message.message_id,
        )

    async def close(self) -> None:
        pass


class StreamCorrelationEngine:
    def __init__(
        self,
        provider: ActiveRuleProvider,
        handler: CorrelationMatchHandler,
        settings: CorrelationSettings,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._provider = provider
        self._handler = handler
        self._settings = settings
        self._clock = time.monotonic if clock is None else clock
        self._rules: tuple[CompiledRule, ...] = ()
        self._next_refresh_at = 0.0
        self._connected = False
        self._refresh_lock = asyncio.Lock()

    @property
    def cached_rule_count(self) -> int:
        return len(self._rules)

    async def connect(self) -> None:
        if self._connected:
            return

        self._connected = True

        try:
            await self._handler.connect()
        except Exception:
            logger.exception("Failed to connect correlation match handler")

        await self._refresh_rules(force=True)

    async def process(self, messages: Sequence[BrokerMessage]) -> int:
        if not self._connected:
            raise RuntimeError("Stream correlation engine is not connected")

        await self._refresh_rules()
        match_count = 0

        for message in messages:
            for rule in self._rules:
                try:
                    if not rule.matches(message):
                        continue

                    match_count += 1
                    await self._handler.handle(
                        CorrelationMatch(rule=rule, message=message)
                    )
                except Exception:
                    logger.exception(
                        "Correlation rule failed rule_id=%s message_id=%s",
                        rule.rule_id,
                        message.message_id,
                    )

        return match_count

    async def close(self) -> None:
        if not self._connected:
            return

        self._connected = False
        self._rules = ()
        self._next_refresh_at = 0.0
        handler_error: Exception | None = None

        try:
            await self._handler.close()
        except Exception as error:
            handler_error = error

        try:
            await self._provider.close()
        except Exception:
            if handler_error is not None:
                logger.exception(
                    "Failed to close rule provider after handler close failure"
                )
                raise handler_error

            raise

        if handler_error is not None:
            raise handler_error

    async def _refresh_rules(self, force: bool = False) -> None:
        now = self._clock()

        if not force and now < self._next_refresh_at:
            return

        async with self._refresh_lock:
            now = self._clock()

            if not force and now < self._next_refresh_at:
                return

            self._next_refresh_at = now + self._settings.rule_refresh_seconds

            try:
                await self._provider.connect()
                documents = await self._provider.load_active_rules()
            except Exception:
                logger.exception("Failed to refresh active correlation rules")
                return

            compiled_rules: list[CompiledRule] = []

            for document in documents:
                try:
                    compiled_rules.append(CompiledRule.from_document(document))
                except Exception:
                    logger.exception(
                        "Ignoring invalid correlation rule rule_id=%s",
                        document.get("_id", "unknown"),
                    )

            self._rules = tuple(compiled_rules)

            logger.info("Loaded %s active correlation rule(s)", len(self._rules))


def _boolean(
    environment: Mapping[str, str],
    variable_name: str,
    default: bool,
) -> bool:
    raw_value = environment.get(variable_name, str(default)).strip().lower()

    if raw_value in {"1", "true", "yes", "on"}:
        return True

    if raw_value in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"{variable_name} must be a boolean")


def _positive_float(
    environment: Mapping[str, str],
    variable_name: str,
    default: float,
) -> float:
    raw_value = environment.get(variable_name, str(default))

    try:
        value = float(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be a positive number") from error

    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{variable_name} must be a positive number")

    return value
