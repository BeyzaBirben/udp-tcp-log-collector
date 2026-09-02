import unittest
from datetime import datetime, timezone
from typing import Any

from log_collector.alarm import AlarmCorrelationMatchHandler
from log_collector.api.alarm_repository import NewAlarm
from log_collector.broker import BrokerMessage
from log_collector.correlation import (
    CompiledCondition,
    CompiledRule,
    CorrelationMatch,
)
from log_collector.models import LogRecord


NOW = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


class RecordingAlarmWriter:
    def __init__(self) -> None:
        self.connect_count = 0
        self.close_count = 0
        self.alarms: list[NewAlarm] = []
        self.error: Exception | None = None

    async def connect(self) -> None:
        self.connect_count += 1

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, Any]:
        if self.error is not None:
            raise self.error

        self.alarms.append(alarm)

        return {"_id": f"alarm-{len(self.alarms)}"}

    async def close(self) -> None:
        self.close_count += 1


def make_match(raw_log: bytes = b"sshd: login failed|\x00\xff") -> CorrelationMatch:
    rule = CompiledRule(
        rule_id="45302f50-35e0-4da8-b767-aaa647004637",
        name="SSH Login Failed",
        source_ip="192.168.1.15",
        conditions=(
            CompiledCondition.create("rawLog", "contains", "login failed"),
        ),
        actions=("Alarm", "Email"),
        email_to=("soc@example.com",),
    )
    message = BrokerMessage.from_record(
        LogRecord(
            received_at=NOW,
            protocol="TCP",
            source_ip="192.168.1.15",
            source_port=55140,
            raw_log=raw_log,
        ),
        message_id="8c831afa-2695-4cec-a19c-06caa73180a7",
    )

    return CorrelationMatch(rule=rule, message=message)


class AlarmCorrelationMatchHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_persists_all_required_alarm_fields(self) -> None:
        writer = RecordingAlarmWriter()
        handler = AlarmCorrelationMatchHandler(writer)
        match = make_match()

        await handler.connect()
        await handler.handle(match)

        self.assertEqual(writer.connect_count, 2)
        self.assertEqual(len(writer.alarms), 1)
        alarm = writer.alarms[0]
        self.assertEqual(alarm.rule_id, match.rule.rule_id)
        self.assertEqual(alarm.rule_name, match.rule.name)
        self.assertEqual(alarm.source_ip, match.message.source_ip)
        self.assertEqual(alarm.log_id, match.message.message_id)
        self.assertEqual(alarm.raw_log, "sshd: login failed|\x00�")
        self.assertFalse(alarm.email_sent)

    async def test_each_processing_attempt_creates_a_separate_alarm(self) -> None:
        writer = RecordingAlarmWriter()
        handler = AlarmCorrelationMatchHandler(writer)
        match = make_match(b"login failed")

        await handler.handle(match)
        await handler.handle(match)

        self.assertEqual(len(writer.alarms), 2)
        self.assertEqual(writer.connect_count, 2)

    async def test_propagates_write_failure_for_engine_level_isolation(self) -> None:
        writer = RecordingAlarmWriter()
        writer.error = RuntimeError("alarm write failed")
        handler = AlarmCorrelationMatchHandler(writer)

        with self.assertRaisesRegex(RuntimeError, "alarm write failed"):
            await handler.handle(make_match())

    async def test_closes_alarm_repository(self) -> None:
        writer = RecordingAlarmWriter()
        handler = AlarmCorrelationMatchHandler(writer)

        await handler.close()

        self.assertEqual(writer.close_count, 1)
