import copy
import unittest
from datetime import datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from log_collector.api.app import create_app
from log_collector.api.config import APISettings
from log_collector.api.repository import LogPage, LogQuery
from log_collector.api.rule_models import RuleCreateRequest, RuleUpdateRequest


RULE_ID = "45302f50-35e0-4da8-b767-aaa647004637"
MISSING_RULE_ID = "80a0fe95-caba-4354-bf80-e882fa2a2d70"
NOW = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


class FakeLogRepository:
    def __init__(self) -> None:
        self.existing_sources = {"192.168.1.15"}
        self.connected = False
        self.closed = False

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
        return None

    async def list_sources(self) -> list[dict[str, Any]]:
        return []

    async def source_exists(self, source_ip: str) -> bool:
        return source_ip in self.existing_sources


class FakeRuleRepository:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.ping_error: Exception | None = None
        self.connect_error: Exception | None = None
        self.documents: dict[str, dict[str, Any]] = {}
        self.created_requests: list[RuleCreateRequest] = []
        self.updated_requests: list[tuple[str, RuleUpdateRequest]] = []
        self.deleted_ids: list[str] = []

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        if self.ping_error is not None:
            raise self.ping_error

    async def list_rules(self) -> list[dict[str, Any]]:
        return list(self.documents.values())

    async def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        return self.documents.get(rule_id)

    async def create_rule(self, rule: RuleCreateRequest) -> dict[str, Any]:
        self.created_requests.append(rule)
        document = self._document(RULE_ID, rule, NOW, NOW)
        self.documents[RULE_ID] = document

        return document

    async def update_rule(
        self,
        rule_id: str,
        rule: RuleUpdateRequest,
    ) -> dict[str, Any] | None:
        self.updated_requests.append((rule_id, rule))
        existing = self.documents.get(rule_id)

        if existing is None:
            return None

        document = self._document(
            rule_id,
            rule,
            existing["createdAt"],
            datetime(2026, 8, 24, 11, 0, tzinfo=timezone.utc),
        )
        self.documents[rule_id] = document

        return document

    async def delete_rule(self, rule_id: str) -> bool:
        self.deleted_ids.append(rule_id)

        return self.documents.pop(rule_id, None) is not None

    @staticmethod
    def _document(
        rule_id: str,
        rule: RuleCreateRequest | RuleUpdateRequest,
        created_at: datetime,
        updated_at: datetime,
    ) -> dict[str, Any]:
        document = rule.model_dump()
        document["_id"] = rule_id
        document["createdAt"] = created_at
        document["updatedAt"] = updated_at

        return document


def valid_rule_payload() -> dict[str, Any]:
    return {
        "name": "SSH Login Failed",
        "enabled": True,
        "sourceIp": "192.168.1.15",
        "conditions": [
            {
                "field": "rawLog",
                "operator": "contains",
                "value": "login failed",
            }
        ],
        "actions": ["Alarm", "Email"],
        "emailTo": ["soc@example.com"],
    }


class RuleAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.log_repository = FakeLogRepository()
        self.rule_repository = FakeRuleRepository()
        self.client_context = TestClient(
            create_app(
                settings=APISettings(),
                repository=self.log_repository,
                rule_repository=self.rule_repository,
            )
        )
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        if not self.rule_repository.closed:
            self.client_context.__exit__(None, None, None)

    def seed_rule(self) -> None:
        request = RuleCreateRequest.model_validate(valid_rule_payload())
        self.rule_repository.documents[RULE_ID] = self.rule_repository._document(
            RULE_ID,
            request,
            NOW,
            NOW,
        )

    def test_lifespan_connects_and_closes_rule_repository(self) -> None:
        self.assertTrue(self.rule_repository.connected)

        self.client_context.__exit__(None, None, None)

        self.assertTrue(self.rule_repository.closed)

    def test_lifespan_closes_log_repository_when_rule_connection_fails(
        self,
    ) -> None:
        log_repository = FakeLogRepository()
        rule_repository = FakeRuleRepository()
        rule_repository.connect_error = RuntimeError("rules unavailable")
        client = TestClient(
            create_app(
                settings=APISettings(),
                repository=log_repository,
                rule_repository=rule_repository,
            )
        )

        with self.assertRaisesRegex(RuntimeError, "rules unavailable"):
            with client:
                pass

        self.assertTrue(log_repository.connected)
        self.assertTrue(log_repository.closed)

    def test_health_includes_rule_database_connection(self) -> None:
        self.rule_repository.ping_error = RuntimeError("database unavailable")

        response = self.client.get("/health")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Database is unavailable"})

    def test_creates_rule_for_existing_source(self) -> None:
        response = self.client.post("/api/rules", json=valid_rule_payload())

        self.assertEqual(response.status_code, 201)
        body = response.json()
        self.assertEqual(body["id"], RULE_ID)
        self.assertEqual(body["name"], "SSH Login Failed")
        self.assertEqual(body["sourceIp"], "192.168.1.15")
        self.assertEqual(body["conditions"][0]["operator"], "contains")
        self.assertEqual(body["actions"], ["Alarm", "Email"])
        self.assertEqual(body["emailTo"], ["soc@example.com"])
        self.assertEqual(len(self.rule_repository.created_requests), 1)

    def test_rejects_rule_for_unknown_source(self) -> None:
        payload = valid_rule_payload()
        payload["sourceIp"] = "192.168.1.99"

        response = self.client.post("/api/rules", json=payload)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json(),
            {
                "detail": (
                    "sourceIp must identify a source that exists in collected logs"
                )
            },
        )
        self.assertEqual(self.rule_repository.created_requests, [])

    def test_rejects_invalid_rule_payloads(self) -> None:
        examples = []

        empty_name = valid_rule_payload()
        empty_name["name"] = "  "
        examples.append(empty_name)

        empty_conditions = valid_rule_payload()
        empty_conditions["conditions"] = []
        examples.append(empty_conditions)

        invalid_regex = valid_rule_payload()
        invalid_regex["conditions"] = [
            {"field": "rawLog", "operator": "regex", "value": "[invalid"}
        ]
        examples.append(invalid_regex)

        for payload in examples:
            with self.subTest(payload=payload):
                response = self.client.post("/api/rules", json=payload)

                self.assertEqual(response.status_code, 422)

        self.assertEqual(self.rule_repository.created_requests, [])

    def test_lists_and_gets_rules(self) -> None:
        self.seed_rule()

        list_response = self.client.get("/api/rules")
        detail_response = self.client.get(f"/api/rules/{RULE_ID}")

        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(len(list_response.json()), 1)
        self.assertEqual(list_response.json()[0]["id"], RULE_ID)
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()["id"], RULE_ID)

    def test_get_returns_not_found_for_unknown_rule(self) -> None:
        response = self.client.get(f"/api/rules/{MISSING_RULE_ID}")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Rule was not found"})

    def test_updates_rule_without_application_restart(self) -> None:
        self.seed_rule()
        payload = copy.deepcopy(valid_rule_payload())
        payload["name"] = "Updated SSH Rule"
        payload["enabled"] = False
        payload["conditions"][0]["value"] = "authentication failed"

        response = self.client.put(f"/api/rules/{RULE_ID}", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "Updated SSH Rule")
        self.assertFalse(response.json()["enabled"])
        self.assertEqual(
            response.json()["createdAt"],
            NOW.isoformat().replace("+00:00", "Z"),
        )
        self.assertEqual(len(self.rule_repository.updated_requests), 1)

    def test_update_rejects_unknown_source(self) -> None:
        self.seed_rule()
        payload = valid_rule_payload()
        payload["sourceIp"] = "192.168.1.99"

        response = self.client.put(f"/api/rules/{RULE_ID}", json=payload)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(self.rule_repository.updated_requests, [])

    def test_update_returns_not_found_for_unknown_rule(self) -> None:
        response = self.client.put(
            f"/api/rules/{MISSING_RULE_ID}",
            json=valid_rule_payload(),
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Rule was not found"})

    def test_deletes_rule_and_returns_empty_response(self) -> None:
        self.seed_rule()

        response = self.client.delete(f"/api/rules/{RULE_ID}")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        self.assertEqual(self.rule_repository.deleted_ids, [RULE_ID])

    def test_delete_returns_not_found_for_unknown_rule(self) -> None:
        response = self.client.delete(f"/api/rules/{MISSING_RULE_ID}")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Rule was not found"})

    def test_openapi_documents_rule_crud_contract(self) -> None:
        response = self.client.get("/openapi.json")

        self.assertEqual(response.status_code, 200)
        paths = response.json()["paths"]
        self.assertEqual(set(paths["/api/rules"]), {"get", "post"})
        self.assertEqual(
            set(paths["/api/rules/{id}"]),
            {"get", "put", "delete"},
        )
