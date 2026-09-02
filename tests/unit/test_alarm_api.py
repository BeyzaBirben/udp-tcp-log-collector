import base64
import unittest
from datetime import datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from log_collector.api.alarm_repository import (
    AlarmPage,
    AlarmQuery,
    NewAlarm,
)
from log_collector.api.app import create_app
from log_collector.api.config import APISettings
from log_collector.api.repository import LogPage, LogQuery


ALARM_ID = "d893cf4c-8f96-46d0-945b-8ae4eb73e583"
MISSING_ALARM_ID = "c2d6b991-3dcb-4b86-ac3c-e06e2df11ee2"
RULE_ID = "45302f50-35e0-4da8-b767-aaa647004637"
LOG_ID = "8c831afa-2695-4cec-a19c-06caa73180a7"
NOW = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


class FakeLogRepository:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.document: dict[str, Any] | None = None
        self.requested_ids: list[str] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        pass

    async def list_logs(
        self,
        query: LogQuery,
        page: int,
        page_size: int,
    ) -> LogPage:
        return LogPage([], 0)

    async def get_log(self, log_id: str) -> dict[str, Any] | None:
        self.requested_ids.append(log_id)

        return self.document

    async def list_sources(self) -> list[dict[str, Any]]:
        return []

    async def source_exists(self, source_ip: str) -> bool:
        return False


class FakeRuleRepository:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        pass

    async def list_rules(self) -> list[dict[str, Any]]:
        return []

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        return None

    async def create_rule(self, rule: object) -> dict[str, Any]:
        raise AssertionError("Rule creation was not expected")

    async def update_rule(
        self,
        rule_id: str,
        rule: object,
    ) -> dict[str, Any] | None:
        raise AssertionError("Rule update was not expected")

    async def delete_rule(self, rule_id: str) -> bool:
        raise AssertionError("Rule deletion was not expected")


class FakeAlarmRepository:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.connect_error: Exception | None = None
        self.ping_error: Exception | None = None
        self.page = AlarmPage([], 0)
        self.document: dict[str, Any] | None = None
        self.query_calls: list[tuple[AlarmQuery, int, int]] = []
        self.requested_ids: list[str] = []

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error is not None:
            raise self.ping_error

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, Any]:
        raise AssertionError("Alarm creation was not expected")

    async def list_alarms(
        self,
        query: AlarmQuery,
        page: int,
        page_size: int,
    ) -> AlarmPage:
        self.query_calls.append((query, page, page_size))

        return self.page

    async def get_alarm(self, alarm_id: str) -> dict[str, Any] | None:
        self.requested_ids.append(alarm_id)

        return self.document


def make_alarm_document() -> dict[str, Any]:
    return {
        "_id": ALARM_ID,
        "ruleId": RULE_ID,
        "ruleName": "SSH Login Failed",
        "sourceIp": "192.168.1.15",
        "logId": LOG_ID,
        "rawLog": "sshd: login failed",
        "createdAt": NOW,
        "emailSent": False,
    }


def make_log_document() -> dict[str, Any]:
    raw_log = b"sshd: login failed"

    return {
        "_id": LOG_ID,
        "schemaVersion": 1,
        "messageId": LOG_ID,
        "receivedAt": NOW,
        "protocol": "TCP",
        "sourceIp": "192.168.1.15",
        "sourcePort": 55140,
        "rawLog": "sshd: login failed",
        "rawLogBase64": base64.b64encode(raw_log).decode("ascii"),
        "rawLogSize": len(raw_log),
        "rawLogBytes": raw_log,
    }


class AlarmAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.log_repository = FakeLogRepository()
        self.rule_repository = FakeRuleRepository()
        self.alarm_repository = FakeAlarmRepository()
        self.settings = APISettings(default_page_size=2, max_page_size=3)
        self.client_context = TestClient(
            create_app(
                settings=self.settings,
                repository=self.log_repository,
                rule_repository=self.rule_repository,
                alarm_repository=self.alarm_repository,
            )
        )
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        if not self.alarm_repository.closed:
            self.client_context.__exit__(None, None, None)

    def test_lifespan_connects_and_closes_alarm_repository(self) -> None:
        self.assertTrue(self.alarm_repository.connected)

        self.client_context.__exit__(None, None, None)

        self.assertTrue(self.alarm_repository.closed)
        self.assertTrue(self.rule_repository.closed)
        self.assertTrue(self.log_repository.closed)

    def test_alarm_connection_failure_closes_previous_repositories(self) -> None:
        log_repository = FakeLogRepository()
        rule_repository = FakeRuleRepository()
        alarm_repository = FakeAlarmRepository()
        alarm_repository.connect_error = RuntimeError("alarms unavailable")
        client = TestClient(
            create_app(
                settings=self.settings,
                repository=log_repository,
                rule_repository=rule_repository,
                alarm_repository=alarm_repository,
            )
        )

        with self.assertRaisesRegex(RuntimeError, "alarms unavailable"):
            with client:
                pass

        self.assertTrue(rule_repository.closed)
        self.assertTrue(log_repository.closed)

    def test_health_includes_alarm_database_connection(self) -> None:
        self.alarm_repository.ping_error = RuntimeError("database unavailable")

        response = self.client.get("/health")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Database is unavailable"})

    def test_lists_alarms_with_all_filters_and_pagination(self) -> None:
        self.alarm_repository.page = AlarmPage([make_alarm_document()], 7)
        parameters = {
            "from": "2026-08-24T09:00:00+03:00",
            "to": "2026-08-24T12:00:00+03:00",
            "ruleId": RULE_ID,
            "sourceIp": "192.168.1.15",
            "page": 2,
            "pageSize": 3,
        }

        response = self.client.get("/api/alarms", params=parameters)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["page"], 2)
        self.assertEqual(body["pageSize"], 3)
        self.assertEqual(body["total"], 7)
        self.assertEqual(body["items"][0]["id"], ALARM_ID)
        self.assertFalse(body["items"][0]["emailSent"])

        query, page, page_size = self.alarm_repository.query_calls[0]
        self.assertEqual(
            query.start,
            datetime(2026, 8, 24, 6, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            query.end,
            datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(query.rule_id, RULE_ID)
        self.assertEqual(query.source_ip, "192.168.1.15")
        self.assertEqual((page, page_size), (2, 3))

    def test_lists_all_alarms_with_configured_defaults(self) -> None:
        response = self.client.get("/api/alarms")

        self.assertEqual(response.status_code, 200)
        query, page, page_size = self.alarm_repository.query_calls[0]
        self.assertEqual(query, AlarmQuery())
        self.assertEqual((page, page_size), (1, 2))

    def test_alarm_query_validates_filters(self) -> None:
        examples = (
            ({"from": "2026-08-24T09:00:00"}, "from must include a timezone"),
            ({"to": "2026-08-24T10:00:00"}, "to must include a timezone"),
            (
                {
                    "from": "2026-08-24T11:00:00Z",
                    "to": "2026-08-24T10:00:00Z",
                },
                "from must be earlier than or equal to to",
            ),
            (
                {"sourceIp": "not-an-ip"},
                "sourceIp must be a valid IPv4 or IPv6 address",
            ),
        )

        for parameters, detail in examples:
            with self.subTest(parameters=parameters):
                response = self.client.get("/api/alarms", params=parameters)

                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json(), {"detail": detail})

        self.assertEqual(self.alarm_repository.query_calls, [])

    def test_alarm_query_validates_uuid_and_page_limit(self) -> None:
        invalid_rule = self.client.get(
            "/api/alarms",
            params={"ruleId": "not-a-uuid"},
        )
        oversized_page = self.client.get(
            "/api/alarms",
            params={"pageSize": 4},
        )

        self.assertEqual(invalid_rule.status_code, 422)
        self.assertEqual(oversized_page.status_code, 422)

    def test_get_alarm_includes_the_triggering_log(self) -> None:
        self.alarm_repository.document = make_alarm_document()
        self.log_repository.document = make_log_document()

        response = self.client.get(f"/api/alarms/{ALARM_ID}")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["id"], ALARM_ID)
        self.assertEqual(body["ruleId"], RULE_ID)
        self.assertEqual(body["logId"], LOG_ID)
        self.assertEqual(body["log"]["id"], LOG_ID)
        self.assertEqual(body["log"]["rawLog"], "sshd: login failed")
        self.assertEqual(self.log_repository.requested_ids, [LOG_ID])

    def test_get_alarm_reports_missing_log_as_null(self) -> None:
        self.alarm_repository.document = make_alarm_document()

        response = self.client.get(f"/api/alarms/{ALARM_ID}")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["log"])

    def test_get_alarm_returns_not_found(self) -> None:
        response = self.client.get(f"/api/alarms/{MISSING_ALARM_ID}")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Alarm was not found"})
        self.assertEqual(self.log_repository.requested_ids, [])

    def test_openapi_documents_alarm_endpoints_and_filters(self) -> None:
        response = self.client.get("/openapi.json")

        self.assertEqual(response.status_code, 200)
        paths = response.json()["paths"]
        self.assertEqual(set(paths["/api/alarms"]), {"get"})
        self.assertEqual(set(paths["/api/alarms/{id}"]), {"get"})
        parameters = {
            parameter["name"]: parameter
            for parameter in paths["/api/alarms"]["get"]["parameters"]
        }
        self.assertEqual(
            set(parameters),
            {"from", "to", "ruleId", "sourceIp", "page", "pageSize"},
        )
        self.assertEqual(parameters["pageSize"]["schema"]["maximum"], 3)
