import copy
import json
import os
import re
import socket
import subprocess
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from uuid import uuid4

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_WEEK_THREE_TESTS = os.environ.get("RUN_WEEK_THREE_TESTS") == "1"


@unittest.skipUnless(
    RUN_WEEK_THREE_TESTS,
    "Week three integration tests are opt-in",
)
class WeekThreeIntegrationTests(unittest.TestCase):
    provider: str
    collector_port: int
    api_port: int
    mailhog_http_port: int
    mongodb_port: int
    rule_refresh_seconds: float
    command_environment: dict[str, str]
    database_client: MongoClient
    log_collection: Any
    rule_collection: Any
    alarm_collection: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.provider = os.environ.get(
            "BROKER_PROVIDER",
            "rabbitmq",
        ).strip().lower()

        if cls.provider not in {"rabbitmq", "kafka"}:
            raise ValueError("BROKER_PROVIDER must be rabbitmq or kafka")

        cls.collector_port = int(os.environ.get("COLLECTOR_PORT", "5514"))
        cls.api_port = int(os.environ.get("API_PORT", "8000"))
        cls.mailhog_http_port = int(
            os.environ.get("MAILHOG_HTTP_PORT", "8025")
        )
        cls.mongodb_port = int(os.environ.get("MONGODB_PORT", "27017"))
        cls.rule_refresh_seconds = 0.2
        cls.command_environment = dict(os.environ)
        cls.command_environment.update(
            {
                "BROKER_PROVIDER": cls.provider,
                "CORRELATION_ENABLED": "true",
                "CORRELATION_RULE_REFRESH_SECONDS": str(
                    cls.rule_refresh_seconds
                ),
                "CONSUMER_BATCH_TIMEOUT_SECONDS": "0.2",
                "SMTP_HOST": "mailhog",
                "SMTP_PORT": "1025",
                "SMTP_USE_TLS": "false",
                "SMTP_TIMEOUT_SECONDS": "1.0",
            }
        )
        cls.command_environment.pop("SMTP_USERNAME", None)
        cls.command_environment.pop("SMTP_PASSWORD", None)

        cls.run_command(["docker", "info"])
        cls.compose("down", "--remove-orphans", check=False)

        try:
            cls.compose("up", "--build", "-d")
            cls.database_client = MongoClient(
                f"mongodb://127.0.0.1:{cls.mongodb_port}/",
                serverSelectionTimeoutMS=5000,
                tz_aware=True,
            )
            cls.wait_for_infrastructure()

            database_name = os.environ.get(
                "MONGODB_DATABASE",
                "log_collection",
            )
            log_collection_name = os.environ.get(
                "MONGODB_COLLECTION",
                "raw_logs",
            )
            rule_collection_name = os.environ.get(
                "MONGODB_RULES_COLLECTION",
                "correlation_rules",
            )
            alarm_collection_name = os.environ.get(
                "MONGODB_ALARMS_COLLECTION",
                "alarms",
            )
            database = cls.database_client[database_name]
            cls.log_collection = database[log_collection_name]
            cls.rule_collection = database[rule_collection_name]
            cls.alarm_collection = database[alarm_collection_name]
        except BaseException:
            database_client = getattr(cls, "database_client", None)

            if database_client is not None:
                database_client.close()

            cls.compose("logs", "--no-color", "--tail", "120", check=False)
            cls.compose("down", "--remove-orphans", check=False)
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        database_client = getattr(cls, "database_client", None)

        if database_client is not None:
            database_client.close()

        cls.compose("down", "--remove-orphans")

    def setUp(self) -> None:
        self.log_collection.delete_many({})
        self.rule_collection.delete_many({})
        self.alarm_collection.delete_many({})
        self.clear_mailhog()
        self.wait_for_rule_refresh()

        marker = f"week3-source-{self.provider}-{uuid4().hex}"
        self.send_tcp([marker.encode("utf-8")])
        document = self.wait_for_log(marker)
        self.source_ip = document["sourceIp"]

    @classmethod
    def run_command(
        cls,
        arguments: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            arguments,
            cwd=PROJECT_ROOT,
            env=cls.command_environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=900,
        )

        if check and result.returncode != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(arguments)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        return result

    @classmethod
    def compose(
        cls,
        *arguments: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return cls.run_command(
            ["docker", "compose", *arguments],
            check=check,
        )

    @classmethod
    def wait_for_infrastructure(cls, timeout: float = 120.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                cls.database_client.admin.command("ping")

                with socket.create_connection(
                    ("127.0.0.1", cls.collector_port),
                    timeout=1.0,
                ):
                    pass

                health = cls.api_json("GET", "/health")

                if health != {"status": "ok"}:
                    raise RuntimeError("Log API health response is invalid")

                messages = cls.mailhog_json("GET", "/api/v2/messages")

                if not isinstance(messages, dict):
                    raise RuntimeError("MailHog response is invalid")

                return
            except Exception as error:
                last_error = error
                time.sleep(0.25)

        raise TimeoutError(
            "Week three infrastructure did not become ready"
        ) from last_error

    @classmethod
    def request(
        cls,
        host: str,
        port: int,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> tuple[int, bytes]:
        target = path

        if query:
            parameters = {
                key: value
                for key, value in query.items()
                if value is not None
            }
            target = f"{path}?{urlencode(parameters, doseq=True)}"

        body = None
        headers = {"Accept": "application/json"}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        connection = HTTPConnection(host, port, timeout=10)

        try:
            connection.request(method, target, body=body, headers=headers)
            response = connection.getresponse()

            return response.status, response.read()
        finally:
            connection.close()

    @classmethod
    def json_request(
        cls,
        host: str,
        port: int,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        expected_status: int = 200,
    ) -> Any:
        status_code, raw_body = cls.request(
            host,
            port,
            method,
            path,
            query=query,
            payload=payload,
        )

        if status_code != expected_status:
            body_text = raw_body.decode("utf-8", errors="replace")
            raise AssertionError(
                f"Expected HTTP {expected_status}, received {status_code} "
                f"for {method} {path}: {body_text}"
            )

        if not raw_body:
            return None

        return json.loads(raw_body)

    @classmethod
    def api_json(
        cls,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        expected_status: int = 200,
    ) -> Any:
        return cls.json_request(
            "127.0.0.1",
            cls.api_port,
            method,
            path,
            query=query,
            payload=payload,
            expected_status=expected_status,
        )

    @classmethod
    def mailhog_json(
        cls,
        method: str,
        path: str,
    ) -> Any:
        return cls.json_request(
            "127.0.0.1",
            cls.mailhog_http_port,
            method,
            path,
        )

    @classmethod
    def clear_mailhog(cls) -> None:
        status_code, raw_body = cls.request(
            "127.0.0.1",
            cls.mailhog_http_port,
            "DELETE",
            "/api/v1/messages",
        )

        if status_code not in {200, 204}:
            body_text = raw_body.decode("utf-8", errors="replace")
            raise AssertionError(
                f"MailHog cleanup failed with HTTP {status_code}: {body_text}"
            )

    @classmethod
    def wait_for_mailhog(cls, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                response = cls.mailhog_json("GET", "/api/v2/messages")

                if isinstance(response, dict):
                    return
            except Exception as error:
                last_error = error

            time.sleep(0.2)

        raise TimeoutError("MailHog did not become ready") from last_error

    @classmethod
    def send_tcp(cls, payloads: list[bytes]) -> None:
        with socket.create_connection(
            ("127.0.0.1", cls.collector_port),
            timeout=15,
        ) as client:
            for payload in payloads:
                client.sendall(payload + b"\n")

    @classmethod
    def send_tcp_from_api_container(cls, payload: bytes) -> None:
        program = (
            "import socket;"
            "client=socket.create_connection(('collector',5514),timeout=10);"
            f"client.sendall({payload!r}+b'\\n');"
            "client.close()"
        )
        cls.compose("exec", "-T", "api", "python", "-c", program)

    def create_rule(
        self,
        *,
        name: str,
        enabled: bool,
        conditions: list[dict[str, str]],
        actions: list[str],
        email_to: list[str] | None = None,
        source_ip: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "name": name,
            "enabled": enabled,
            "sourceIp": self.source_ip if source_ip is None else source_ip,
            "conditions": conditions,
            "actions": actions,
            "emailTo": [] if email_to is None else email_to,
        }

        return self.api_json(
            "POST",
            "/api/rules",
            payload=payload,
            expected_status=201,
        )

    def wait_for_rule_refresh(self) -> None:
        time.sleep(max(0.35, self.rule_refresh_seconds * 2))

    def wait_for_log(
        self,
        raw_log: str,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            document = self.log_collection.find_one({"rawLog": raw_log})

            if document is not None:
                return document

            time.sleep(0.1)

        self.fail(f"Log was not persisted: {raw_log}")

    def wait_for_logs(
        self,
        marker: str,
        expected_count: int,
        timeout: float = 60.0,
    ) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        query = {"rawLog": {"$regex": re.escape(marker)}}

        while time.monotonic() < deadline:
            documents = list(self.log_collection.find(query))

            if len(documents) >= expected_count:
                return documents

            time.sleep(0.1)

        actual_count = self.log_collection.count_documents(query)
        self.fail(
            f"Expected {expected_count} logs for {marker}, found {actual_count}"
        )

    def wait_for_alarms(
        self,
        rule_id: str,
        expected_count: int,
        timeout: float = 60.0,
    ) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            documents = list(self.alarm_collection.find({"ruleId": rule_id}))

            if len(documents) >= expected_count:
                return documents

            time.sleep(0.1)

        actual_count = self.alarm_collection.count_documents(
            {"ruleId": rule_id}
        )
        self.fail(
            f"Expected {expected_count} alarms for {rule_id}, "
            f"found {actual_count}"
        )

    def wait_for_alarm_email_state(
        self,
        rule_id: str,
        raw_log: str,
        email_sent: bool,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        query = {
            "ruleId": rule_id,
            "rawLog": raw_log,
            "emailSent": email_sent,
        }

        while time.monotonic() < deadline:
            document = self.alarm_collection.find_one(query)

            if document is not None:
                return document

            time.sleep(0.1)

        self.fail(
            f"Alarm emailSent={email_sent} was not observed for {raw_log}"
        )

    def assert_no_alarm(
        self,
        rule_id: str,
        duration: float = 1.0,
    ) -> None:
        deadline = time.monotonic() + duration

        while time.monotonic() < deadline:
            count = self.alarm_collection.count_documents(
                {"ruleId": rule_id}
            )

            if count:
                self.fail(f"Expected no alarms for {rule_id}, found {count}")

            time.sleep(0.1)

    def wait_for_mail(
        self,
        marker: str,
        expected_count: int = 1,
        timeout: float = 30.0,
    ) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            response = self.mailhog_json("GET", "/api/v2/messages")
            messages = [
                message
                for message in response.get("items", [])
                if marker in json.dumps(message, ensure_ascii=False)
            ]

            if len(messages) >= expected_count:
                return messages

            time.sleep(0.1)

        self.fail(
            f"Expected {expected_count} MailHog messages containing {marker}"
        )

    def wait_for_consumer_log(
        self,
        expected_text: str,
        timeout: float = 30.0,
    ) -> None:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            result = self.compose(
                "logs",
                "--no-color",
                "--tail",
                "200",
                "consumer",
                check=False,
            )
            output = f"{result.stdout}\n{result.stderr}"

            if expected_text in output:
                return

            time.sleep(0.2)

        self.fail(f"Consumer log did not contain: {expected_text}")

    @staticmethod
    def isoformat(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace(
            "+00:00",
            "Z",
        )

    def test_01_creates_alarm_and_delivers_email(self) -> None:
        marker = f"week3-basic-{self.provider}-{uuid4().hex}"
        rule_name = f"Basic alarm {marker}"
        rule = self.create_rule(
            name=rule_name,
            enabled=True,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": marker,
                },
                {
                    "field": "sourceIp",
                    "operator": "equals",
                    "value": self.source_ip,
                },
            ],
            actions=["Alarm", "Email"],
            email_to=["soc@example.com"],
        )
        self.wait_for_rule_refresh()
        raw_log = f"{marker}|sshd: login failed"

        self.send_tcp([raw_log.encode("utf-8")])

        log_document = self.wait_for_log(raw_log)
        alarm_document = self.wait_for_alarm_email_state(
            rule["id"],
            raw_log,
            True,
        )
        messages = self.wait_for_mail(marker)
        created_at = alarm_document["createdAt"]
        alarm_page = self.api_json(
            "GET",
            "/api/alarms",
            query={
                "from": self.isoformat(created_at - timedelta(seconds=1)),
                "to": self.isoformat(created_at + timedelta(seconds=1)),
                "ruleId": rule["id"],
                "sourceIp": self.source_ip,
                "page": 1,
                "pageSize": 10,
            },
        )
        detail = self.api_json(
            "GET",
            f"/api/alarms/{alarm_document['_id']}",
        )
        schema = self.api_json("GET", "/openapi.json")

        self.assertEqual(alarm_page["total"], 1)
        self.assertEqual(alarm_page["items"][0]["emailSent"], True)
        self.assertEqual(detail["ruleId"], rule["id"])
        self.assertEqual(detail["ruleName"], rule_name)
        self.assertEqual(detail["sourceIp"], self.source_ip)
        self.assertEqual(detail["logId"], str(log_document["_id"]))
        self.assertEqual(detail["rawLog"], raw_log)
        self.assertEqual(detail["log"]["id"], str(log_document["_id"]))
        self.assertEqual(detail["log"]["rawLog"], raw_log)
        self.assertEqual(len(messages), 1)
        serialized_mail = json.dumps(messages[0], ensure_ascii=False)
        self.assertIn("soc@example.com", serialized_mail)
        self.assertIn(rule_name, serialized_mail)
        self.assertIn(raw_log, serialized_mail)
        self.assertTrue(
            {
                "/api/logs",
                "/api/sources",
                "/api/rules",
                "/api/alarms",
            }.issubset(schema["paths"])
        )

    def test_02_non_matching_log_is_persisted_without_alarm(self) -> None:
        marker = f"week3-no-match-{self.provider}-{uuid4().hex}"
        expected_value = f"{marker}|result=blocked"
        rule = self.create_rule(
            name=f"Non matching {marker}",
            enabled=True,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": expected_value,
                }
            ],
            actions=["Alarm", "Email"],
            email_to=["soc@example.com"],
        )
        self.wait_for_rule_refresh()
        raw_log = f"{marker}|result=allowed"

        self.send_tcp([raw_log.encode("utf-8")])

        document = self.wait_for_log(raw_log)
        self.assert_no_alarm(rule["id"])
        response = self.api_json(
            "GET",
            "/api/logs",
            query={
                "from": self.isoformat(
                    document["receivedAt"] - timedelta(seconds=1)
                ),
                "to": self.isoformat(
                    document["receivedAt"] + timedelta(seconds=1)
                ),
                "q": marker,
                "page": 1,
                "pageSize": 10,
            },
        )
        mail = self.mailhog_json("GET", "/api/v2/messages")

        self.assertEqual(response["total"], 1)
        self.assertEqual(response["items"][0]["rawLog"], raw_log)
        self.assertEqual(mail["total"], 0)

    def test_03_matching_log_from_different_source_does_not_alarm(self) -> None:
        marker = f"week3-source-mismatch-{self.provider}-{uuid4().hex}"
        rule = self.create_rule(
            name=f"Source mismatch {marker}",
            enabled=True,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": marker,
                }
            ],
            actions=["Alarm"],
        )
        self.wait_for_rule_refresh()

        self.send_tcp_from_api_container(marker.encode("utf-8"))

        document = self.wait_for_log(marker)
        self.assertNotEqual(document["sourceIp"], self.source_ip)
        self.assert_no_alarm(rule["id"])
        sources = self.api_json("GET", "/api/sources")
        source_ips = {source["sourceIp"] for source in sources}

        self.assertIn(self.source_ip, source_ips)
        self.assertIn(document["sourceIp"], source_ips)

    def test_04_disabled_rule_does_not_create_alarm(self) -> None:
        marker = f"week3-disabled-{self.provider}-{uuid4().hex}"
        rule = self.create_rule(
            name=f"Disabled {marker}",
            enabled=False,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": marker,
                }
            ],
            actions=["Alarm"],
        )
        self.wait_for_rule_refresh()

        self.send_tcp([marker.encode("utf-8")])

        self.wait_for_log(marker)
        self.assert_no_alarm(rule["id"])
        stored_rule = self.api_json("GET", f"/api/rules/{rule['id']}")

        self.assertFalse(stored_rule["enabled"])

    def test_05_rule_update_changes_behavior_without_restart(self) -> None:
        marker = f"week3-hot-rule-{self.provider}-{uuid4().hex}"
        first_value = f"{marker}|stage=one|"
        second_value = f"{marker}|stage=two|"
        rule = self.create_rule(
            name=f"Hot rule {marker}",
            enabled=True,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": first_value,
                }
            ],
            actions=["Alarm"],
        )
        self.wait_for_rule_refresh()
        initial_log = f"{first_value}message=initial"

        self.send_tcp([initial_log.encode("utf-8")])
        self.wait_for_log(initial_log)
        self.wait_for_alarms(rule["id"], 1)

        update_payload = {
            "name": f"Updated hot rule {marker}",
            "enabled": True,
            "sourceIp": self.source_ip,
            "conditions": [
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": second_value,
                }
            ],
            "actions": ["Alarm"],
            "emailTo": [],
        }
        updated_rule = self.api_json(
            "PUT",
            f"/api/rules/{rule['id']}",
            payload=update_payload,
        )
        self.wait_for_rule_refresh()
        old_log = f"{first_value}message=after-update"
        new_log = f"{second_value}message=after-update"

        self.send_tcp(
            [
                old_log.encode("utf-8"),
                new_log.encode("utf-8"),
            ]
        )

        self.wait_for_log(old_log)
        self.wait_for_log(new_log)
        self.wait_for_alarms(rule["id"], 2)
        time.sleep(0.5)
        alarms = list(self.alarm_collection.find({"ruleId": rule["id"]}))

        self.assertEqual(updated_rule["name"], update_payload["name"])
        self.assertEqual(len(alarms), 2)
        self.assertEqual(
            {alarm["rawLog"] for alarm in alarms},
            {initial_log, new_log},
        )

    def test_06_invalid_rules_are_rejected(self) -> None:
        marker = f"week3-invalid-{self.provider}-{uuid4().hex}"
        base_payload = {
            "name": f"Valid {marker}",
            "enabled": True,
            "sourceIp": self.source_ip,
            "conditions": [
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": marker,
                }
            ],
            "actions": ["Alarm"],
            "emailTo": [],
        }
        invalid_payloads: list[dict[str, Any]] = []

        empty_name = copy.deepcopy(base_payload)
        empty_name["name"] = ""
        invalid_payloads.append(empty_name)

        empty_conditions = copy.deepcopy(base_payload)
        empty_conditions["conditions"] = []
        invalid_payloads.append(empty_conditions)

        invalid_regex = copy.deepcopy(base_payload)
        invalid_regex["conditions"] = [
            {
                "field": "rawLog",
                "operator": "regex",
                "value": "[",
            }
        ]
        invalid_payloads.append(invalid_regex)

        unknown_source = copy.deepcopy(base_payload)
        unknown_source["sourceIp"] = (
            "192.0.2.250"
            if self.source_ip != "192.0.2.250"
            else "198.51.100.250"
        )
        invalid_payloads.append(unknown_source)
        before = self.api_json("GET", "/api/rules")

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                response = self.api_json(
                    "POST",
                    "/api/rules",
                    payload=payload,
                    expected_status=422,
                )
                self.assertIn("detail", response)

        after = self.api_json("GET", "/api/rules")

        self.assertEqual(after, before)

    def test_07_smtp_failure_does_not_stop_alarm_or_log_flow(self) -> None:
        marker = f"week3-smtp-down-{self.provider}-{uuid4().hex}"
        rule = self.create_rule(
            name=f"SMTP failure {marker}",
            enabled=True,
            conditions=[
                {
                    "field": "rawLog",
                    "operator": "contains",
                    "value": marker,
                }
            ],
            actions=["Alarm", "Email"],
            email_to=["soc@example.com"],
        )
        self.wait_for_rule_refresh()
        failed_logs = [
            f"{marker}|message=failed-{index}"
            for index in range(1, 3)
        ]
        self.compose("stop", "mailhog")

        try:
            self.send_tcp(
                [raw_log.encode("utf-8") for raw_log in failed_logs]
            )

            for raw_log in failed_logs:
                self.wait_for_log(raw_log)

            failed_alarms = self.wait_for_alarms(rule["id"], 2)

            for alarm in failed_alarms:
                self.wait_for_consumer_log(
                    "Alarm email delivery failed "
                    f"alarm_id={alarm['_id']}"
                )

            persisted = list(
                self.alarm_collection.find({"ruleId": rule["id"]})
            )
            health = self.api_json("GET", "/health")

            self.assertEqual(health, {"status": "ok"})
            self.assertEqual(len(persisted), 2)
            self.assertTrue(
                all(alarm["emailSent"] is False for alarm in persisted)
            )
        finally:
            self.compose("start", "mailhog", check=False)
            self.wait_for_mailhog()

        self.clear_mailhog()
        recovered_log = f"{marker}|message=recovered"
        self.send_tcp([recovered_log.encode("utf-8")])
        self.wait_for_log(recovered_log)
        recovered_alarm = self.wait_for_alarm_email_state(
            rule["id"],
            recovered_log,
            True,
        )
        self.wait_for_mail(marker)
        alarms = self.api_json(
            "GET",
            "/api/alarms",
            query={
                "ruleId": rule["id"],
                "page": 1,
                "pageSize": 10,
            },
        )

        self.assertTrue(recovered_alarm["emailSent"])
        self.assertEqual(alarms["total"], 3)
        self.assertEqual(
            sum(item["emailSent"] is True for item in alarms["items"]),
            1,
        )

    def test_08_api_filters_sources_and_pagination_are_consistent(self) -> None:
        self.log_collection.delete_many({})
        marker = f"week3-api-{self.provider}-{uuid4().hex}"
        payloads = [
            f"{marker}|group=alpha|index={index:02d}"
            for index in range(1, 5)
        ] + [
            f"{marker}|group=beta|index={index:02d}"
            for index in range(5, 8)
        ]
        start = datetime.now(timezone.utc) - timedelta(seconds=1)

        self.send_tcp([payload.encode("utf-8") for payload in payloads])

        documents = self.wait_for_logs(marker, len(payloads))
        end = datetime.now(timezone.utc) + timedelta(seconds=1)
        base_query = {
            "from": self.isoformat(start),
            "to": self.isoformat(end),
            "page": 1,
            "pageSize": 20,
        }
        time_page = self.api_json(
            "GET",
            "/api/logs",
            query=base_query,
        )
        source_page = self.api_json(
            "GET",
            "/api/logs",
            query={**base_query, "sourceIp": self.source_ip},
        )
        text_page = self.api_json(
            "GET",
            "/api/logs",
            query={**base_query, "q": "GROUP=ALPHA"},
        )
        combined_page = self.api_json(
            "GET",
            "/api/logs",
            query={
                **base_query,
                "sourceIp": self.source_ip,
                "q": "group=alpha",
            },
        )
        source_path = quote(self.source_ip, safe="")
        source_log_page = self.api_json(
            "GET",
            f"/api/sources/{source_path}/logs",
            query={**base_query, "q": "group=beta"},
        )
        paged_ids: list[str] = []

        for page_number in (1, 2, 3):
            page = self.api_json(
                "GET",
                "/api/logs",
                query={
                    **base_query,
                    "page": page_number,
                    "pageSize": 3,
                },
            )
            self.assertEqual(page["total"], len(payloads))
            self.assertEqual(page["page"], page_number)
            paged_ids.extend(item["id"] for item in page["items"])

        sources = self.api_json("GET", "/api/sources")
        source = next(
            item for item in sources if item["sourceIp"] == self.source_ip
        )
        selected_id = str(documents[0]["_id"])
        selected_log = self.api_json("GET", f"/api/logs/{selected_id}")

        self.assertEqual(time_page["total"], len(payloads))
        self.assertEqual(source_page["total"], len(payloads))
        self.assertTrue(
            all(item["sourceIp"] == self.source_ip for item in source_page["items"])
        )
        self.assertEqual(text_page["total"], 4)
        self.assertEqual(combined_page["total"], 4)
        self.assertEqual(source_log_page["total"], 3)
        self.assertEqual(len(paged_ids), len(payloads))
        self.assertEqual(len(set(paged_ids)), len(payloads))
        self.assertEqual(source["logCount"], len(payloads))
        self.assertEqual(selected_log["id"], selected_id)
        self.assertIn(selected_log["rawLog"], payloads)


if __name__ == "__main__":
    unittest.main()
