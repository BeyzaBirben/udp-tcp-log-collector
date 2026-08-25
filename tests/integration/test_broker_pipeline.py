import base64
import ipaddress
import json
import os
import re
import socket
import subprocess
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_PIPELINE_TESTS = os.environ.get("RUN_PIPELINE_TESTS") == "1"


@unittest.skipUnless(
    RUN_PIPELINE_TESTS,
    "Broker pipeline integration tests are opt-in",
)
class BrokerPipelineIntegrationTests(unittest.TestCase):
    provider: str
    collector_port: int
    mongodb_port: int
    expected_source_ip: str
    database_client: MongoClient
    collection: Any
    command_environment: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.provider = os.environ.get("BROKER_PROVIDER", "rabbitmq").strip().lower()

        if cls.provider not in {"rabbitmq", "kafka"}:
            raise ValueError("BROKER_PROVIDER must be rabbitmq or kafka")

        cls.collector_port = int(os.environ.get("COLLECTOR_PORT", "5514"))
        cls.mongodb_port = int(os.environ.get("MONGODB_PORT", "27017"))
        cls.command_environment = dict(os.environ)
        cls.command_environment["BROKER_PROVIDER"] = cls.provider

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

            database_name = os.environ.get("MONGODB_DATABASE", "log_collection")
            collection_name = os.environ.get("MONGODB_COLLECTION", "raw_logs")

            cls.collection = cls.database_client[database_name][collection_name]
            cls.expected_source_ip = cls.collector_gateway()
            cls.wait_for_udp_pipeline()
        except Exception:
            database_client = getattr(cls, "database_client", None)

            if database_client is not None:
                database_client.close()

            cls.compose("logs", "--tail", "80", check=False)
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
            timeout=600,
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
        return cls.run_command(["docker", "compose", *arguments], check=check)

    @classmethod
    def wait_for_infrastructure(cls, timeout: float = 90.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                cls.database_client.admin.command("ping")

                with socket.create_connection(
                    ("127.0.0.1", cls.collector_port),
                    timeout=1.0,
                ):
                    return
            except Exception as error:
                last_error = error
                time.sleep(0.25)

        raise TimeoutError("Collector and MongoDB did not become ready") from last_error

    @classmethod
    def collector_gateway(cls) -> str:
        container_id = cls.compose("ps", "-q", "collector").stdout.strip()

        if not container_id:
            raise RuntimeError("Collector container identifier is unavailable")

        result = cls.run_command(
            [
                "docker",
                "inspect",
                "--format",
                "{{json .NetworkSettings.Networks}}",
                container_id,
            ]
        )
        networks = json.loads(result.stdout)
        gateways = {
            str(settings["Gateway"])
            for settings in networks.values()
            if settings.get("Gateway")
        }

        if len(gateways) != 1:
            raise RuntimeError("Collector network gateway is ambiguous")

        gateway = gateways.pop()
        ipaddress.ip_address(gateway)

        return gateway

    @classmethod
    def wait_for_udp_pipeline(cls, timeout: float = 30.0) -> None:
        marker = f"pipeline-readiness-{cls.provider}-{uuid4().hex}"
        payload = marker.encode("utf-8")
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            cls.send_udp([payload])
            attempt_deadline = min(time.monotonic() + 1.0, deadline)

            while time.monotonic() < attempt_deadline:
                document = cls.collection.find_one(
                    {"rawLog": marker, "protocol": "UDP"}
                )

                if document is not None:
                    return

                time.sleep(0.1)

        raise TimeoutError("UDP pipeline did not become ready")

    @classmethod
    def wait_for_consumer_replicas(
        cls,
        expected_count: int,
        timeout: float = 60.0,
    ) -> list[str]:
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            consumer_ids = cls.compose("ps", "-q", "consumer").stdout.splitlines()

            if len(consumer_ids) == expected_count:
                ready_consumers = 0

                for consumer_id in consumer_ids:
                    logs = cls.run_command(
                        ["docker", "logs", "--tail", "100", consumer_id],
                        check=False,
                    )

                    if "Log consumer connected with batch_size=" in (
                        logs.stdout + logs.stderr
                    ):
                        ready_consumers += 1

                if ready_consumers == expected_count:
                    return consumer_ids

            time.sleep(0.5)

        raise TimeoutError(f"Expected {expected_count} ready consumer replica(s)")

    @classmethod
    def send_udp(
        cls,
        payloads: list[bytes],
        barrier: threading.Barrier | None = None,
    ) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            if barrier is not None:
                barrier.wait(timeout=15)

            for payload in payloads:
                sent_size = client.sendto(payload, ("127.0.0.1", cls.collector_port))

                if sent_size != len(payload):
                    raise RuntimeError("UDP payload was not sent completely")

    @classmethod
    def send_tcp(
        cls,
        payloads: list[bytes],
        barrier: threading.Barrier | None = None,
    ) -> None:
        with socket.create_connection(
            ("127.0.0.1", cls.collector_port),
            timeout=15,
        ) as client:
            if barrier is not None:
                barrier.wait(timeout=15)

            for payload in payloads:
                client.sendall(payload + b"\n")

    def wait_for_documents(
        self,
        marker: str,
        expected_count: int,
        timeout: float = 90.0,
    ) -> list[dict[str, Any]]:
        query = {"rawLog": {"$regex": re.escape(marker)}}
        deadline = time.monotonic() + timeout
        documents: list[dict[str, Any]] = []

        while time.monotonic() < deadline:
            documents = list(self.collection.find(query))

            if len(documents) >= expected_count:
                return documents

            time.sleep(0.1)

        logs = self.compose("logs", "--tail", "40", check=False)

        raise AssertionError(
            f"Expected {expected_count} MongoDB document(s) for {marker}, "
            f"found {len(documents)}\n"
            f"Docker Compose logs:\n{logs.stdout}\n{logs.stderr}"
        )

    def assert_document(
        self,
        document: dict[str, Any],
        payload: bytes,
        protocol: str,
    ) -> None:
        self.assertEqual(document["schemaVersion"], 1)
        self.assertEqual(document["_id"], document["messageId"])
        self.assertEqual(str(UUID(document["messageId"])), document["messageId"])
        self.assertEqual(document["protocol"], protocol)
        self.assertEqual(document["sourceIp"], self.expected_source_ip)
        self.assertEqual(
            str(ipaddress.ip_address(document["sourceIp"])),
            document["sourceIp"],
        )
        self.assertIsInstance(document["sourcePort"], int)
        self.assertGreaterEqual(document["sourcePort"], 1)
        self.assertLessEqual(document["sourcePort"], 65535)
        self.assertEqual(document["rawLog"], payload.decode("utf-8", errors="replace"))
        self.assertEqual(
            document["rawLogBase64"],
            base64.b64encode(payload).decode("ascii"),
        )
        self.assertEqual(document["rawLogSize"], len(payload))
        self.assertEqual(bytes(document["rawLogBytes"]), payload)

        received_at = document["receivedAt"]

        self.assertIsInstance(received_at, datetime)
        self.assertIsNotNone(received_at.tzinfo)
        self.assertLessEqual(
            received_at,
            datetime.now(timezone.utc).replace(microsecond=999999),
        )

    def assert_expected_documents(
        self,
        marker: str,
        expected_payloads: dict[bytes, str],
    ) -> list[dict[str, Any]]:
        documents = self.wait_for_documents(marker, len(expected_payloads))

        self.assertEqual(len(documents), len(expected_payloads))

        documents_by_payload = {
            bytes(document["rawLogBytes"]): document for document in documents
        }

        self.assertEqual(set(documents_by_payload), set(expected_payloads))

        for payload, protocol in expected_payloads.items():
            self.assert_document(documents_by_payload[payload], payload, protocol)

        return documents

    def test_01_udp_message_reaches_mongodb_without_data_loss(self) -> None:
        marker = f"pipeline-udp-{self.provider}-{uuid4().hex}"
        payload = marker.encode("utf-8") + b"|\x00\xff\x80|" + "İstanbul".encode(
            "utf-8"
        )

        self.send_udp([payload])

        self.assert_expected_documents(marker, {payload: "UDP"})

    def test_02_tcp_messages_reach_mongodb_without_data_loss(self) -> None:
        marker = f"pipeline-tcp-{self.provider}-{uuid4().hex}"
        payloads = [
            f"{marker}|client=tcp-001|message={index:04d}|Türkçe".encode("utf-8")
            for index in range(1, 6)
        ]

        self.send_tcp(payloads)

        documents = self.assert_expected_documents(
            marker,
            {payload: "TCP" for payload in payloads},
        )

        self.assertEqual(len({document["sourcePort"] for document in documents}), 1)

    def test_03_processes_udp_and_tcp_concurrently(self) -> None:
        marker = f"pipeline-mixed-{self.provider}-{uuid4().hex}"
        tcp_payloads = [
            f"{marker}|protocol=tcp|message={index:04d}".encode("utf-8")
            for index in range(1, 6)
        ]
        udp_payloads = [
            f"{marker}|protocol=udp|message={index:04d}".encode("utf-8")
            for index in range(1, 6)
        ]
        barrier = threading.Barrier(2)

        with ThreadPoolExecutor(max_workers=2) as executor:
            tcp_future = executor.submit(self.send_tcp, tcp_payloads, barrier)
            udp_future = executor.submit(self.send_udp, udp_payloads, barrier)

            tcp_future.result(timeout=20)
            udp_future.result(timeout=20)

        expected_payloads = {
            **{payload: "TCP" for payload in tcp_payloads},
            **{payload: "UDP" for payload in udp_payloads},
        }

        self.assert_expected_documents(marker, expected_payloads)

    def test_04_handles_1_5_and_20_concurrent_tcp_clients(self) -> None:
        for client_count in (1, 5, 20):
            with self.subTest(client_count=client_count):
                marker = (
                    f"pipeline-clients-{self.provider}-{client_count}-{uuid4().hex}"
                )
                payload_groups = [
                    [
                        (
                            f"{marker}|client={client_id:03d}|"
                            f"message={message_id:04d}"
                        ).encode("utf-8")
                        for message_id in range(1, 4)
                    ]
                    for client_id in range(1, client_count + 1)
                ]
                barrier = threading.Barrier(client_count)

                with ThreadPoolExecutor(max_workers=client_count) as executor:
                    futures = [
                        executor.submit(self.send_tcp, payloads, barrier)
                        for payloads in payload_groups
                    ]

                    for future in futures:
                        future.result(timeout=20)

                expected_payloads = {
                    payload: "TCP"
                    for payloads in payload_groups
                    for payload in payloads
                }
                documents = self.assert_expected_documents(marker, expected_payloads)

                self.assertEqual(
                    len({document["sourcePort"] for document in documents}),
                    client_count,
                )

    def test_05_three_consumers_persist_each_message_once(self) -> None:
        self.compose("up", "-d", "--scale", "consumer=3")

        consumer_ids = self.wait_for_consumer_replicas(3)

        self.assertEqual(len(consumer_ids), 3)
        self.wait_for_udp_pipeline()

        marker = f"pipeline-scaled-{self.provider}-{uuid4().hex}"
        client_count = 5
        messages_per_client = 20
        payload_groups = [
            [
                (
                    f"{marker}|client={client_id:03d}|"
                    f"message={message_id:04d}"
                ).encode("utf-8")
                for message_id in range(1, messages_per_client + 1)
            ]
            for client_id in range(1, client_count + 1)
        ]
        barrier = threading.Barrier(client_count)

        with ThreadPoolExecutor(max_workers=client_count) as executor:
            futures = [
                executor.submit(self.send_tcp, payloads, barrier)
                for payloads in payload_groups
            ]

            for future in futures:
                future.result(timeout=20)

        expected_payloads = {
            payload: "TCP" for payloads in payload_groups for payload in payloads
        }
        documents = self.assert_expected_documents(marker, expected_payloads)
        expected_count = client_count * messages_per_client

        self.assertEqual(len(documents), expected_count)
        self.assertEqual(
            len({document["messageId"] for document in documents}),
            expected_count,
        )
        self.assertEqual(
            len({bytes(document["rawLogBytes"]) for document in documents}),
            expected_count,
        )


if __name__ == "__main__":
    unittest.main()