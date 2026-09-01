import base64
import unittest
from datetime import datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from log_collector.api.app import create_app
from log_collector.api.config import APISettings
from log_collector.api.repository import LogPage, LogQuery


class FakeLogQueryRepository:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.ping_error: Exception | None = None
        self.page = LogPage([], 0)
        self.document: dict[str, Any] | None = None
        self.sources: list[dict[str, Any]] = []
        self.query_calls: list[tuple[LogQuery, int, int]] = []
        self.log_ids: list[str] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error is not None:
            raise self.ping_error

    async def list_logs(
        self,
        query: LogQuery,
        page: int,
        page_size: int,
    ) -> LogPage:
        self.query_calls.append((query, page, page_size))

        return self.page

    async def get_log(self, log_id: str) -> dict[str, Any] | None:
        self.log_ids.append(log_id)

        return self.document

    async def list_sources(self) -> list[dict[str, Any]]:
        return self.sources


def make_document() -> dict[str, Any]:
    raw_log = b"login failed|\x00\xff"

    return {
        "_id": "8c831afa-2695-4cec-a19c-06caa73180a7",
        "schemaVersion": 1,
        "messageId": "8c831afa-2695-4cec-a19c-06caa73180a7",
        "receivedAt": datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc),
        "protocol": "TCP",
        "sourceIp": "192.168.1.15",
        "sourcePort": 55140,
        "rawLog": "login failed|\x00�",
        "rawLogBase64": base64.b64encode(raw_log).decode("ascii"),
        "rawLogSize": len(raw_log),
        "rawLogBytes": raw_log,
    }


class LogAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = FakeLogQueryRepository()
        self.settings = APISettings(default_page_size=2, max_page_size=3)
        self.client_context = TestClient(
            create_app(settings=self.settings, repository=self.repository)
        )
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        if not self.repository.closed:
            self.client_context.__exit__(None, None, None)

    def test_lifespan_connects_and_closes_repository(self) -> None:
        self.assertTrue(self.repository.connected)

        self.client_context.__exit__(None, None, None)

        self.assertTrue(self.repository.closed)

    def test_health_checks_database(self) -> None:
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_health_returns_service_unavailable_when_database_fails(self) -> None:
        self.repository.ping_error = RuntimeError("database unavailable")

        response = self.client.get("/health")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Database is unavailable"})

    def test_log_query_requires_time_range(self) -> None:
        response = self.client.get("/api/logs")

        self.assertEqual(response.status_code, 422)
        missing_parameters = {
            error["loc"][-1]
            for error in response.json()["detail"]
            if error["type"] == "missing"
        }
        self.assertEqual(missing_parameters, {"from", "to"})

    def test_log_query_validates_timezones_and_range_order(self) -> None:
        examples = (
            (
                {"from": "2026-08-24T09:00:00", "to": "2026-08-24T10:00:00Z"},
                "from must include a timezone",
            ),
            (
                {"from": "2026-08-24T09:00:00Z", "to": "2026-08-24T10:00:00"},
                "to must include a timezone",
            ),
            (
                {"from": "2026-08-24T11:00:00Z", "to": "2026-08-24T10:00:00Z"},
                "from must be earlier than or equal to to",
            ),
        )

        for parameters, detail in examples:
            with self.subTest(parameters=parameters):
                response = self.client.get("/api/logs", params=parameters)

                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json(), {"detail": detail})

    def test_log_query_forwards_filters_and_pagination(self) -> None:
        document = make_document()
        self.repository.page = LogPage([document], 7)
        parameters = {
            "from": "2026-08-24T09:00:00+03:00",
            "to": "2026-08-24T12:00:00+03:00",
            "sourceIp": "192.168.1.15",
            "q": " login failed ",
            "page": 2,
            "pageSize": 3,
        }

        response = self.client.get("/api/logs", params=parameters)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["page"], 2)
        self.assertEqual(body["pageSize"], 3)
        self.assertEqual(body["total"], 7)
        self.assertEqual(len(body["items"]), 1)
        self.assertEqual(body["items"][0]["id"], document["_id"])
        self.assertEqual(body["items"][0]["rawLogBytes"], document["rawLogBase64"])

        query, page, page_size = self.repository.query_calls[0]

        self.assertEqual(query.start, datetime(2026, 8, 24, 6, 0, tzinfo=timezone.utc))
        self.assertEqual(query.end, datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc))
        self.assertEqual(query.source_ip, "192.168.1.15")
        self.assertEqual(query.text, "login failed")
        self.assertEqual((page, page_size), (2, 3))

    def test_log_query_uses_configured_defaults_and_limits(self) -> None:
        parameters = {
            "from": "2026-08-24T09:00:00Z",
            "to": "2026-08-24T10:00:00Z",
        }

        response = self.client.get("/api/logs", params=parameters)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["pageSize"], 2)
        self.assertEqual(self.repository.query_calls[0][1:], (1, 2))

        parameters["pageSize"] = 4
        response = self.client.get("/api/logs", params=parameters)

        self.assertEqual(response.status_code, 422)

    def test_log_query_rejects_invalid_source_ip(self) -> None:
        response = self.client.get(
            "/api/logs",
            params={
                "from": "2026-08-24T09:00:00Z",
                "to": "2026-08-24T10:00:00Z",
                "sourceIp": "not-an-ip",
            },
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json(),
            {"detail": "sourceIp must be a valid IPv4 or IPv6 address"},
        )

    def test_get_log_returns_all_persisted_fields(self) -> None:
        self.repository.document = make_document()

        response = self.client.get(
            "/api/logs/8c831afa-2695-4cec-a19c-06caa73180a7"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            set(response.json()),
            {
                "id",
                "schemaVersion",
                "messageId",
                "receivedAt",
                "protocol",
                "sourceIp",
                "sourcePort",
                "rawLog",
                "rawLogBase64",
                "rawLogSize",
                "rawLogBytes",
            },
        )

    def test_get_log_returns_not_found(self) -> None:
        response = self.client.get(
            "/api/logs/8c831afa-2695-4cec-a19c-06caa73180a7"
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Log was not found"})

    def test_lists_source_summaries(self) -> None:
        self.repository.sources = [
            {
                "sourceIp": "192.168.1.15",
                "logCount": 128340,
                "firstSeen": datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
                "lastSeen": datetime(2026, 8, 24, 14, 22, 10, tzinfo=timezone.utc),
            }
        ]

        response = self.client.get("/api/sources")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()[0]["sourceIp"], "192.168.1.15")
        self.assertEqual(response.json()[0]["logCount"], 128340)

    def test_source_log_query_uses_path_source_and_same_filters(self) -> None:
        response = self.client.get(
            "/api/sources/2001:db8::1/logs",
            params={
                "from": "2026-08-24T09:00:00Z",
                "to": "2026-08-24T10:00:00Z",
                "q": "warning",
                "page": 2,
                "pageSize": 3,
            },
        )

        self.assertEqual(response.status_code, 200)
        query, page, page_size = self.repository.query_calls[0]
        self.assertEqual(query.source_ip, "2001:db8::1")
        self.assertEqual(query.text, "warning")
        self.assertEqual((page, page_size), (2, 3))

    def test_openapi_documents_every_log_api_endpoint(self) -> None:
        response = self.client.get("/openapi.json")

        self.assertEqual(response.status_code, 200)
        paths = response.json()["paths"]
        self.assertTrue(
            {
                "/health",
                "/api/logs",
                "/api/logs/{id}",
                "/api/sources",
                "/api/sources/{sourceIp}/logs",
            }.issubset(paths)
        )
        parameters = {
            parameter["name"]: parameter
            for parameter in paths["/api/logs"]["get"]["parameters"]
        }
        self.assertTrue(parameters["from"]["required"])
        self.assertTrue(parameters["to"]["required"])
        self.assertEqual(parameters["pageSize"]["schema"]["maximum"], 3)
