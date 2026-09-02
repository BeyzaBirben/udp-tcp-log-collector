import math
import unittest
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from log_collector.broker import BrokerMessage
from log_collector.correlation import (
    CorrelationMatch,
    CorrelationSettings,
    StreamCorrelationEngine,
)
from log_collector.models import LogRecord


NOW = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


class ManualClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeActiveRuleProvider:
    def __init__(self, documents: list[dict[str, Any]] | None = None) -> None:
        self.documents = [] if documents is None else documents
        self.connect_count = 0
        self.load_count = 0
        self.close_count = 0
        self.connect_error: Exception | None = None
        self.load_error: Exception | None = None

    async def connect(self) -> None:
        self.connect_count += 1

        if self.connect_error is not None:
            raise self.connect_error

    async def load_active_rules(self) -> list[dict[str, Any]]:
        self.load_count += 1

        if self.load_error is not None:
            raise self.load_error

        return self.documents

    async def close(self) -> None:
        self.close_count += 1


class RecordingMatchHandler:
    def __init__(self) -> None:
        self.attempted_rule_ids: list[str] = []
        self.matches: list[CorrelationMatch] = []
        self.failing_rule_ids: set[str] = set()
        self.connect_count = 0
        self.close_count = 0
        self.connect_error: Exception | None = None
        self.close_error: Exception | None = None

    async def connect(self) -> None:
        self.connect_count += 1

        if self.connect_error is not None:
            raise self.connect_error

    async def handle(self, match: CorrelationMatch) -> None:
        self.attempted_rule_ids.append(match.rule.rule_id)

        if match.rule.rule_id in self.failing_rule_ids:
            raise RuntimeError("match handler failed")

        self.matches.append(match)

    async def close(self) -> None:
        self.close_count += 1

        if self.close_error is not None:
            raise self.close_error


def condition(
    field: str = "rawLog",
    operator: str = "contains",
    value: str = "login failed",
) -> dict[str, str]:
    return {
        "field": field,
        "operator": operator,
        "value": value,
    }


def rule_document(
    *,
    rule_id: str | None = None,
    name: str = "SSH Login Failed",
    source_ip: str = "192.168.1.15",
    conditions: list[dict[str, str]] | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    return {
        "_id": str(uuid4()) if rule_id is None else rule_id,
        "name": name,
        "enabled": enabled,
        "sourceIp": source_ip,
        "conditions": (
            [condition()] if conditions is None else conditions
        ),
        "actions": ["Alarm"],
        "emailTo": [],
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def make_message(
    raw_log: bytes = b"login failed",
    source_ip: str = "192.168.1.15",
) -> BrokerMessage:
    return BrokerMessage.from_record(
        LogRecord(
            received_at=NOW,
            protocol="TCP",
            source_ip=source_ip,
            source_port=55140,
            raw_log=raw_log,
        )
    )


class CorrelationSettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = CorrelationSettings.from_environment({})

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.rule_refresh_seconds, 1.0)

    def test_loads_custom_configuration(self) -> None:
        settings = CorrelationSettings.from_environment(
            {
                "CORRELATION_ENABLED": "false",
                "CORRELATION_RULE_REFRESH_SECONDS": "2.5",
            }
        )

        self.assertFalse(settings.enabled)
        self.assertEqual(settings.rule_refresh_seconds, 2.5)

    def test_accepts_common_boolean_representations(self) -> None:
        for value in ("1", "true", "yes", "on"):
            with self.subTest(value=value):
                self.assertTrue(
                    CorrelationSettings.from_environment(
                        {"CORRELATION_ENABLED": value}
                    ).enabled
                )

        for value in ("0", "false", "no", "off"):
            with self.subTest(value=value):
                self.assertFalse(
                    CorrelationSettings.from_environment(
                        {"CORRELATION_ENABLED": value}
                    ).enabled
                )

    def test_rejects_invalid_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "CORRELATION_ENABLED"):
            CorrelationSettings.from_environment(
                {"CORRELATION_ENABLED": "invalid"}
            )

        for value in ("0", "-1", "nan", "inf", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "CORRELATION_RULE_REFRESH_SECONDS",
                ):
                    CorrelationSettings.from_environment(
                        {"CORRELATION_RULE_REFRESH_SECONDS": value}
                    )

        with self.assertRaisesRegex(ValueError, "CORRELATION_ENABLED"):
            CorrelationSettings(enabled=1)

        with self.assertRaisesRegex(ValueError, "CORRELATION_RULE_REFRESH"):
            CorrelationSettings(rule_refresh_seconds=math.nan)


class StreamCorrelationEngineTests(unittest.IsolatedAsyncioTestCase):
    def make_engine(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[
        StreamCorrelationEngine,
        FakeActiveRuleProvider,
        RecordingMatchHandler,
        ManualClock,
    ]:
        provider = FakeActiveRuleProvider(documents)
        handler = RecordingMatchHandler()
        clock = ManualClock()
        engine = StreamCorrelationEngine(
            provider=provider,
            handler=handler,
            settings=CorrelationSettings(rule_refresh_seconds=1.0),
            clock=clock,
        )

        return engine, provider, handler, clock

    async def test_supports_all_operators_and_both_fields(self) -> None:
        documents = [
            rule_document(
                name="contains",
                conditions=[condition(operator="contains", value="login")],
            ),
            rule_document(
                name="equals",
                conditions=[condition(operator="equals", value="login failed")],
            ),
            rule_document(
                name="startsWith",
                conditions=[condition(operator="startsWith", value="login")],
            ),
            rule_document(
                name="regex",
                conditions=[condition(operator="regex", value="failed$")],
            ),
            rule_document(
                name="sourceIp",
                conditions=[
                    condition(
                        field="sourceIp",
                        operator="equals",
                        value="192.168.1.15",
                    )
                ],
            ),
        ]
        engine, _, handler, _ = self.make_engine(documents)

        await engine.connect()
        count = await engine.process([make_message()])

        self.assertEqual(count, 5)
        self.assertEqual(
            {match.rule.name for match in handler.matches},
            {"contains", "equals", "startsWith", "regex", "sourceIp"},
        )

    async def test_combines_conditions_with_and(self) -> None:
        documents = [
            rule_document(
                name="all-match",
                conditions=[
                    condition(value="login"),
                    condition(operator="startsWith", value="login"),
                ],
            ),
            rule_document(
                name="one-fails",
                conditions=[
                    condition(value="login"),
                    condition(value="accepted"),
                ],
            ),
        ]
        engine, _, handler, _ = self.make_engine(documents)

        await engine.connect()
        count = await engine.process([make_message()])

        self.assertEqual(count, 1)
        self.assertEqual(handler.matches[0].rule.name, "all-match")

    async def test_rule_does_not_match_another_source(self) -> None:
        engine, _, handler, _ = self.make_engine([rule_document()])

        await engine.connect()
        count = await engine.process(
            [make_message(source_ip="192.168.1.99")]
        )

        self.assertEqual(count, 0)
        self.assertEqual(handler.matches, [])

    async def test_refreshes_rules_without_restart(self) -> None:
        old_rule = rule_document(
            name="old-rule",
            conditions=[condition(value="old value")],
        )
        new_rule = rule_document(
            name="new-rule",
            conditions=[condition(value="new value")],
        )
        engine, provider, handler, clock = self.make_engine([old_rule])

        await engine.connect()
        await engine.process([make_message(b"old value")])

        provider.documents = [new_rule]
        await engine.process([make_message(b"new value")])

        self.assertEqual(provider.load_count, 1)
        self.assertEqual([match.rule.name for match in handler.matches], ["old-rule"])

        clock.advance(1.1)
        await engine.process([make_message(b"new value")])

        self.assertEqual(provider.load_count, 2)
        self.assertEqual(
            [match.rule.name for match in handler.matches],
            ["old-rule", "new-rule"],
        )

    async def test_removes_disabled_or_deleted_rules_without_restart(self) -> None:
        active_rule = rule_document(name="active-rule")
        engine, provider, handler, clock = self.make_engine([active_rule])

        await engine.connect()
        first_count = await engine.process([make_message()])

        provider.documents = []
        clock.advance(1.1)
        second_count = await engine.process([make_message()])

        self.assertEqual((first_count, second_count), (1, 0))
        self.assertEqual(engine.cached_rule_count, 0)
        self.assertEqual(
            [match.rule.name for match in handler.matches],
            ["active-rule"],
        )

    async def test_ignores_disabled_and_invalid_rules(self) -> None:
        documents = [
            rule_document(name="disabled", enabled=False),
            rule_document(
                name="invalid-regex",
                conditions=[condition(operator="regex", value="[invalid")],
            ),
            rule_document(name="valid"),
        ]
        engine, _, handler, _ = self.make_engine(documents)

        with self.assertLogs("log_collector.correlation", level="ERROR"):
            await engine.connect()

        count = await engine.process([make_message()])

        self.assertEqual(engine.cached_rule_count, 1)
        self.assertEqual(count, 1)
        self.assertEqual(handler.matches[0].rule.name, "valid")

    async def test_retains_cached_rules_when_refresh_fails(self) -> None:
        engine, provider, handler, clock = self.make_engine([rule_document()])

        await engine.connect()
        provider.load_error = RuntimeError("database unavailable")
        clock.advance(1.1)

        with self.assertLogs("log_collector.correlation", level="ERROR"):
            count = await engine.process([make_message()])

        self.assertEqual(count, 1)
        self.assertEqual(engine.cached_rule_count, 1)
        self.assertEqual(len(handler.matches), 1)

    async def test_recovers_from_initial_rule_provider_failure(self) -> None:
        engine, provider, handler, clock = self.make_engine([rule_document()])
        provider.connect_error = RuntimeError("database unavailable")

        with self.assertLogs("log_collector.correlation", level="ERROR"):
            await engine.connect()

        self.assertEqual(engine.cached_rule_count, 0)

        provider.connect_error = None
        clock.advance(1.1)
        count = await engine.process([make_message()])

        self.assertEqual(count, 1)
        self.assertEqual(len(handler.matches), 1)

    async def test_handler_connection_failure_does_not_block_rules(self) -> None:
        engine, _, handler, _ = self.make_engine([rule_document()])
        handler.connect_error = RuntimeError("alarm database unavailable")

        with self.assertLogs("log_collector.correlation", level="ERROR"):
            await engine.connect()

        handler.connect_error = None
        count = await engine.process([make_message()])

        self.assertEqual(count, 1)
        self.assertEqual(len(handler.matches), 1)

    async def test_handler_failure_does_not_stop_other_rules(self) -> None:
        first_rule = rule_document(name="first")
        second_rule = rule_document(name="second")
        engine, _, handler, _ = self.make_engine([first_rule, second_rule])
        handler.failing_rule_ids.add(first_rule["_id"])

        await engine.connect()

        with self.assertLogs("log_collector.correlation", level="ERROR"):
            count = await engine.process([make_message()])

        self.assertEqual(count, 2)
        self.assertEqual(
            handler.attempted_rule_ids,
            [first_rule["_id"], second_rule["_id"]],
        )
        self.assertEqual(
            [match.rule.rule_id for match in handler.matches],
            [second_rule["_id"]],
        )

    async def test_evaluation_is_stateless_between_messages(self) -> None:
        engine, _, handler, _ = self.make_engine([rule_document()])
        message = make_message()

        await engine.connect()
        first_count = await engine.process([message])
        second_count = await engine.process([message])

        self.assertEqual((first_count, second_count), (1, 1))
        self.assertEqual(len(handler.matches), 2)

    async def test_requires_connection_and_closes_provider(self) -> None:
        engine, provider, handler, _ = self.make_engine([rule_document()])

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await engine.process([make_message()])

        await engine.connect()
        await engine.close()
        await engine.close()

        self.assertEqual(provider.close_count, 1)
        self.assertEqual(handler.connect_count, 1)
        self.assertEqual(handler.close_count, 1)
        self.assertEqual(engine.cached_rule_count, 0)

    async def test_closes_provider_when_handler_close_fails(self) -> None:
        engine, provider, handler, _ = self.make_engine([rule_document()])

        await engine.connect()
        handler.close_error = RuntimeError("handler close failed")

        with self.assertRaisesRegex(RuntimeError, "handler close failed"):
            await engine.close()

        self.assertEqual(provider.close_count, 1)
