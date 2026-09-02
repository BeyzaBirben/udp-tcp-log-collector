import json
import os
import re
import socket
import subprocess
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import uuid4

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_WEEK_THREE_LOAD_TESTS = (
    os.environ.get("RUN_WEEK_THREE_LOAD_TESTS") == "1"
)


@unittest.skipUnless(
    RUN_WEEK_THREE_LOAD_TESTS,
    "Week three load and scaling tests are opt-in",
)
class WeekThreeLoadScalingIntegrationTests(unittest.TestCase):
    client_count = 20
    messages_per_client = 1000
    expected_log_count = client_count * messages_per_client
    expected_alarm_count = 50
    rule_refresh_seconds = 0.2
    provider: str
    collector_port: int
    api_port: int
    mongodb_port: int
    command_environment: dict[str, str]
    database_client: MongoClient
    log_collection: Any
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
        cls.mongodb_port = int(os.environ.get("MONGODB_PORT", "27017"))
        cls.command_environment = dict(os.environ)
        cls.command_environment.update(
            {
                "BROKER_PROVIDER": cls.provider,
                "COLLECTOR_STDOUT_ENABLED": "false",
                "CORRELATION_ENABLED": "true",
                "CORRELATION_RULE_REFRESH_SECONDS": str(
                    cls.rule_refresh_seconds
                ),
                "CONSUMER_BATCH_SIZE": "500",
                "CONSUMER_BATCH_TIMEOUT_SECONDS": "0.2",
            }
        )

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
            alarm_collection_name = os.environ.get(
                "MONGODB_ALARMS_COLLECTION",
                "alarms",
            )
            database = cls.database_client[database_name]
            cls.log_collection = database[log_collection_name]
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
            timeout=1200,
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

                if cls.api_json("GET", "/health") == {"status": "ok"}:
                    return
            except Exception as error:
                last_error = error
                time.sleep(0.25)

        raise TimeoutError(
            "Week three load infrastructure did not become ready"
        ) from last_error

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
        target = path

        if query:
            target = f"{path}?{urlencode(query, doseq=True)}"

        body = None
        headers = {"Accept": "application/json"}

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        connection = HTTPConnection("127.0.0.1", cls.api_port, timeout=15)

        try:
            connection.request(method, target, body=body, headers=headers)
            response = connection.getresponse()
            raw_body = response.read()

            if response.status != expected_status:
                body_text = raw_body.decode("utf-8", errors="replace")
                raise AssertionError(
                    f"Expected HTTP {expected_status}, received "
                    f"{response.status} for {method} {path}: {body_text}"
                )

            return None if not raw_body else json.loads(raw_body)
        finally:
            connection.close()

    @classmethod
    def wait_for_consumer_replicas(
        cls,
        expected_count: int,
        timeout: float = 90.0,
    ) -> list[str]:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            consumer_ids = cls.compose(
                "ps",
                "-q",
                "consumer",
            ).stdout.splitlines()

            if len(consumer_ids) == expected_count:
                ready_count = 0

                for consumer_id in consumer_ids:
                    logs = cls.run_command(
                        ["docker", "logs", consumer_id],
                        check=False,
                    )

                    if "Log consumer connected with batch_size=" in (
                        logs.stdout + logs.stderr
                    ):
                        ready_count += 1

                if ready_count == expected_count:
                    return consumer_ids

            time.sleep(0.5)

        raise TimeoutError(
            f"Expected {expected_count} ready consumer replica(s)"
        )

    @classmethod
    def send_tcp(cls, payloads: list[bytes]) -> None:
        with socket.create_connection(
            ("127.0.0.1", cls.collector_port),
            timeout=15,
        ) as client:
            client.sendall(b"\n".join(payloads) + b"\n")

    @classmethod
    def send_tcp_group(
        cls,
        payloads: list[bytes],
        barrier: threading.Barrier,
    ) -> None:
        with socket.create_connection(
            ("127.0.0.1", cls.collector_port),
            timeout=30,
        ) as client:
            client.settimeout(120)
            barrier.wait(timeout=30)
            client.sendall(b"\n".join(payloads) + b"\n")

    def wait_for_rule_refresh(self) -> None:
        time.sleep(max(0.4, self.rule_refresh_seconds * 2))

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

    def wait_for_count(
        self,
        collection: Any,
        query: dict[str, Any],
        expected_count: int,
        description: str,
        timeout: float = 300.0,
    ) -> int:
        deadline = time.monotonic() + timeout
        last_count = 0

        while time.monotonic() < deadline:
            last_count = collection.count_documents(query)

            if last_count == expected_count:
                return last_count

            if last_count > expected_count:
                self.fail(
                    f"Expected exactly {expected_count} {description}, "
                    f"found {last_count}"
                )

            time.sleep(0.2)

        self.fail(
            f"Expected {expected_count} {description}, found {last_count} "
            f"after {timeout:.0f} seconds"
        )

    def prepare_scenario(self, consumer_count: int) -> str:
        self.compose(
            "up",
            "-d",
            "--scale",
            f"consumer={consumer_count}",
        )
        consumer_ids = self.wait_for_consumer_replicas(consumer_count)
        self.assertEqual(len(consumer_ids), consumer_count)
        time.sleep(0.5)
        self.wait_for_rule_refresh()
        source_marker = (
            f"week3-load-source-{self.provider}-{consumer_count}-"
            f"{uuid4().hex}"
        )

        self.send_tcp([source_marker.encode("utf-8")])

        source_document = self.wait_for_log(source_marker)

        return str(source_document["sourceIp"])

    def create_rule(
        self,
        source_ip: str,
        marker: str,
    ) -> dict[str, Any]:
        return self.api_json(
            "POST",
            "/api/rules",
            payload={
                "name": f"Load correlation {marker}",
                "enabled": True,
                "sourceIp": source_ip,
                "conditions": [
                    {
                        "field": "rawLog",
                        "operator": "contains",
                        "value": f"{marker}|alert=true|",
                    }
                ],
                "actions": ["Alarm"],
                "emailTo": [],
            },
            expected_status=201,
        )

    def build_payload_groups(
        self,
        marker: str,
    ) -> tuple[list[list[bytes]], set[str]]:
        groups: list[list[bytes]] = []
        matching_logs: set[str] = set()

        for client_id in range(1, self.client_count + 1):
            payloads: list[bytes] = []

            for message_id in range(1, self.messages_per_client + 1):
                global_index = (
                    (client_id - 1) * self.messages_per_client + message_id
                )
                is_match = global_index % 400 == 0
                raw_log = (
                    f"{marker}|alert={str(is_match).lower()}|"
                    f"client={client_id:02d}|message={message_id:04d}"
                )
                payloads.append(raw_log.encode("utf-8"))

                if is_match:
                    matching_logs.add(raw_log)

            groups.append(payloads)

        self.assertEqual(len(groups), self.client_count)
        self.assertTrue(
            all(len(group) == self.messages_per_client for group in groups)
        )
        self.assertEqual(len(matching_logs), self.expected_alarm_count)

        return groups, matching_logs

    def run_load_scenario(self, consumer_count: int) -> None:
        source_ip = self.prepare_scenario(consumer_count)
        marker = (
            f"week3-load-{self.provider}-{consumer_count}-"
            f"{uuid4().hex}"
        )
        rule = self.create_rule(source_ip, marker)
        self.wait_for_rule_refresh()
        payload_groups, matching_logs = self.build_payload_groups(marker)
        barrier = threading.Barrier(self.client_count)
        started_at = time.monotonic()

        with ThreadPoolExecutor(max_workers=self.client_count) as executor:
            futures = [
                executor.submit(self.send_tcp_group, payloads, barrier)
                for payloads in payload_groups
            ]

            for future in futures:
                future.result(timeout=180)

        send_duration = time.monotonic() - started_at
        log_query = {
            "rawLog": {
                "$regex": f"^{re.escape(marker)}\\|",
            }
        }
        alarm_query = {"ruleId": rule["id"]}
        self.wait_for_count(
            self.log_collection,
            log_query,
            self.expected_log_count,
            "persisted load logs",
        )
        self.wait_for_count(
            self.alarm_collection,
            alarm_query,
            self.expected_alarm_count,
            "correlation alarms",
        )
        end_to_end_duration = time.monotonic() - started_at
        time.sleep(1.0)
        logs = list(
            self.log_collection.find(
                log_query,
                {
                    "_id": 1,
                    "messageId": 1,
                    "sourceIp": 1,
                    "sourcePort": 1,
                    "rawLog": 1,
                },
            )
        )
        alarms = list(self.alarm_collection.find(alarm_query))
        alarm_log_ids = {alarm["logId"] for alarm in alarms}
        api_page = self.api_json(
            "GET",
            "/api/alarms",
            query={
                "ruleId": rule["id"],
                "page": 1,
                "pageSize": 200,
            },
        )

        self.assertEqual(len(logs), self.expected_log_count)
        self.assertEqual(
            len({str(document["_id"]) for document in logs}),
            self.expected_log_count,
        )
        self.assertEqual(
            len({document["messageId"] for document in logs}),
            self.expected_log_count,
        )
        self.assertEqual(
            len({document["rawLog"] for document in logs}),
            self.expected_log_count,
        )
        self.assertEqual(
            len({document["sourcePort"] for document in logs}),
            self.client_count,
        )
        self.assertTrue(
            all(document["sourceIp"] == source_ip for document in logs)
        )

        for client_id in range(1, self.client_count + 1):
            client_marker = f"|client={client_id:02d}|"
            client_total = sum(
                client_marker in document["rawLog"] for document in logs
            )
            self.assertEqual(client_total, self.messages_per_client)

        self.assertEqual(len(alarms), self.expected_alarm_count)
        self.assertEqual(len(alarm_log_ids), self.expected_alarm_count)
        self.assertEqual(
            {alarm["rawLog"] for alarm in alarms},
            matching_logs,
        )
        self.assertTrue(
            all(alarm["emailSent"] is False for alarm in alarms)
        )
        self.assertEqual(
            self.log_collection.count_documents(
                {"_id": {"$in": list(alarm_log_ids)}}
            ),
            self.expected_alarm_count,
        )
        self.assertEqual(api_page["total"], self.expected_alarm_count)
        self.assertEqual(len(api_page["items"]), self.expected_alarm_count)
        self.assertEqual(
            len(self.wait_for_consumer_replicas(consumer_count)),
            consumer_count,
        )

        throughput = self.expected_log_count / end_to_end_duration
        print(
            f"\nLoad result provider={self.provider} "
            f"consumers={consumer_count} logs={self.expected_log_count} "
            f"alarms={self.expected_alarm_count} "
            f"sendSeconds={send_duration:.3f} "
            f"endToEndSeconds={end_to_end_duration:.3f} "
            f"logsPerSecond={throughput:.3f}"
        )

    def test_01_exact_alarm_count_under_load(self) -> None:
        self.run_load_scenario(consumer_count=1)

    def test_02_exact_alarm_count_with_three_consumers(self) -> None:
        self.run_load_scenario(consumer_count=3)


if __name__ == "__main__":
    unittest.main()
