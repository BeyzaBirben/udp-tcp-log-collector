import base64
import json
import os
import re
import time
import unittest
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4

from tests.integration import test_broker_pipeline


RUN_RECOVERY_TESTS = os.environ.get("RUN_RECOVERY_TESTS") == "1"
PIPELINE_SUPPORT = vars(test_broker_pipeline.BrokerPipelineIntegrationTests)


@unittest.skipUnless(
    RUN_RECOVERY_TESTS,
    "Broker recovery integration tests are opt-in",
)
class BrokerRecoveryIntegrationTests(unittest.TestCase):
    provider: str
    collector_port: int
    mongodb_port: int
    expected_source_ip: str
    collection: Any
    command_environment: dict[str, str]

    setUpClass = PIPELINE_SUPPORT["setUpClass"]
    tearDownClass = PIPELINE_SUPPORT["tearDownClass"]
    run_command = PIPELINE_SUPPORT["run_command"]
    compose = PIPELINE_SUPPORT["compose"]
    wait_for_infrastructure = PIPELINE_SUPPORT["wait_for_infrastructure"]
    collector_gateway = PIPELINE_SUPPORT["collector_gateway"]
    wait_for_udp_pipeline = PIPELINE_SUPPORT["wait_for_udp_pipeline"]
    wait_for_consumer_replicas = PIPELINE_SUPPORT["wait_for_consumer_replicas"]
    send_udp = PIPELINE_SUPPORT["send_udp"]
    send_tcp = PIPELINE_SUPPORT["send_tcp"]
    wait_for_documents = PIPELINE_SUPPORT["wait_for_documents"]
    assert_document = PIPELINE_SUPPORT["assert_document"]
    assert_expected_documents = PIPELINE_SUPPORT["assert_expected_documents"]

    def setUp(self) -> None:
        self.wait_for_service_state("broker", running=True, healthy=True)
        self.wait_for_service_state("collector", running=True)
        self.wait_for_consumer_replicas(1, timeout=90.0)
        self.wait_for_udp_pipeline(timeout=45.0)

    def tearDown(self) -> None:
        for service_name in ("broker", "collector", "consumer"):
            try:
                service_state = self.service_state(service_name)
            except Exception:
                continue

            if not service_state.get("Running", False):
                self.compose("start", service_name, check=False)

    @classmethod
    def service_state(cls, service_name: str) -> dict[str, Any]:
        container_ids = cls.compose("ps", "-a", "-q", service_name).stdout.splitlines()

        if len(container_ids) != 1:
            raise RuntimeError(
                f"Expected one container for {service_name}, found {len(container_ids)}"
            )

        container_id = container_ids[0]
        result = cls.run_command(
            [
                "docker",
                "inspect",
                "--format",
                "{{json .State}}",
                container_id,
            ]
        )
        state = json.loads(result.stdout)
        state["containerId"] = container_id

        return state

    @classmethod
    def wait_for_service_state(
        cls,
        service_name: str,
        *,
        running: bool,
        healthy: bool = False,
        timeout: float = 120.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_state: dict[str, Any] | None = None

        while time.monotonic() < deadline:
            last_state = cls.service_state(service_name)
            has_expected_running_state = bool(last_state.get("Running")) is running
            health_status = last_state.get("Health", {}).get("Status")
            has_expected_health = not healthy or health_status == "healthy"

            if has_expected_running_state and has_expected_health:
                return last_state

            time.sleep(0.5)

        raise TimeoutError(
            f"Service {service_name} did not reach running={running} "
            f"healthy={healthy}; last state: {last_state}"
        )

    def rabbitmq_backlog(self) -> int:
        management_port = os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672")
        queue_name = os.environ.get("RABBITMQ_QUEUE", "raw-logs")
        username = os.environ.get("RABBITMQ_USERNAME", "logcollector")
        password = os.environ.get("RABBITMQ_PASSWORD", "logcollector")
        encoded_credentials = base64.b64encode(
            f"{username}:{password}".encode("utf-8")
        ).decode("ascii")
        virtual_host = quote("/", safe="")
        encoded_queue = quote(queue_name, safe="")
        endpoint = (
            f"http://127.0.0.1:{management_port}/api/queues/"
            f"{virtual_host}/{encoded_queue}"
        )
        request = Request(
            endpoint,
            headers={"Authorization": f"Basic {encoded_credentials}"},
        )

        with urlopen(request, timeout=5.0) as response:
            payload = json.load(response)

        return int(payload["messages"])

    def kafka_backlog(self) -> int:
        topic_name = os.environ.get("KAFKA_TOPIC", "raw-logs")
        consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "log-consumers")
        result = self.compose(
            "exec",
            "-T",
            "broker",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "broker:19092",
            "--describe",
            "--group",
            consumer_group,
        )
        partition_lags: list[int] = []

        for line in result.stdout.splitlines():
            columns = line.split()

            if len(columns) < 6:
                continue

            if columns[0] != consumer_group or columns[1] != topic_name:
                continue

            lag_value = columns[5]

            if lag_value == "-":
                continue

            partition_lags.append(int(lag_value))

        if not partition_lags:
            raise RuntimeError("Kafka consumer group partition lag is unavailable")

        return sum(partition_lags)

    def broker_backlog(self) -> int:
        if self.provider == "rabbitmq":
            return self.rabbitmq_backlog()

        if self.provider == "kafka":
            return self.kafka_backlog()

        raise ValueError(f"Unsupported broker provider: {self.provider}")

    def wait_for_broker_backlog(
        self,
        minimum_count: int,
        timeout: float = 90.0,
    ) -> int:
        deadline = time.monotonic() + timeout
        last_count = 0
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                last_count = self.broker_backlog()

                if last_count >= minimum_count:
                    return last_count
            except Exception as error:
                last_error = error

            time.sleep(0.5)

        raise AssertionError(
            f"Expected at least {minimum_count} pending broker message(s), "
            f"found {last_count}"
        ) from last_error

    def count_documents(self, marker: str) -> int:
        return int(
            self.collection.count_documents(
                {"rawLog": {"$regex": re.escape(marker)}}
            )
        )

    def wait_for_collector_records(
        self,
        marker: str,
        expected_count: int,
        timeout: float = 30.0,
    ) -> int:
        deadline = time.monotonic() + timeout
        received_count = 0

        while time.monotonic() < deadline:
            logs = self.compose("logs", "--no-color", "collector")
            received_count = sum(
                marker in line and '"raw_log_text"' in line
                for line in logs.stdout.splitlines()
            )

            if received_count >= expected_count:
                return received_count

            time.sleep(0.25)

        raise AssertionError(
            f"Expected collector to receive {expected_count} record(s) for "
            f"{marker}, found {received_count}"
        )

    @staticmethod
    def create_payloads(
        marker: str,
        protocol: str,
        count: int,
        *,
        phase: str = "default",
    ) -> list[bytes]:
        return [
            (
                f"{marker}|phase={phase}|protocol={protocol.lower()}|"
                f"message={index:04d}"
            ).encode("utf-8")
            for index in range(1, count + 1)
        ]

    def test_01_messages_wait_in_broker_while_consumer_is_stopped(self) -> None:
        marker = f"recovery-consumer-stopped-{self.provider}-{uuid4().hex}"
        tcp_payloads = self.create_payloads(marker, "TCP", 6)
        udp_payloads = self.create_payloads(marker, "UDP", 4)
        expected_payloads = {
            **{payload: "TCP" for payload in tcp_payloads},
            **{payload: "UDP" for payload in udp_payloads},
        }
        collector_before = self.service_state("collector")

        self.compose("stop", "consumer")
        self.wait_for_service_state("consumer", running=False)

        try:
            initial_backlog = self.broker_backlog()
            self.send_tcp(tcp_payloads)
            self.send_udp(udp_payloads)
            self.wait_for_broker_backlog(initial_backlog + len(expected_payloads))

            self.assertEqual(self.count_documents(marker), 0)

            collector_after = self.service_state("collector")

            self.assertTrue(collector_after["Running"])
            self.assertEqual(
                collector_after["StartedAt"],
                collector_before["StartedAt"],
            )
        finally:
            self.compose("start", "consumer", check=False)

        self.wait_for_consumer_replicas(1, timeout=90.0)
        self.assert_expected_documents(marker, expected_payloads)

    def test_02_collector_recovers_after_broker_restart(self) -> None:
        marker = f"recovery-broker-stopped-{self.provider}-{uuid4().hex}"
        tcp_payloads = self.create_payloads(marker, "TCP", 6)
        udp_payloads = self.create_payloads(marker, "UDP", 4)
        expected_payloads = {
            **{payload: "TCP" for payload in tcp_payloads},
            **{payload: "UDP" for payload in udp_payloads},
        }
        collector_before = self.service_state("collector")

        self.compose("stop", "broker")
        self.wait_for_service_state("broker", running=False)

        try:
            self.send_tcp(tcp_payloads)
            self.send_udp(udp_payloads)
            self.wait_for_collector_records(marker, len(expected_payloads))

            collector_during_outage = self.service_state("collector")

            self.assertTrue(collector_during_outage["Running"])
            self.assertEqual(
                collector_during_outage["StartedAt"],
                collector_before["StartedAt"],
            )
            self.assertEqual(self.count_documents(marker), 0)
        finally:
            self.compose("start", "broker", check=False)

        self.wait_for_service_state(
            "broker",
            running=True,
            healthy=True,
            timeout=180.0,
        )
        self.wait_for_documents(marker, len(expected_payloads), timeout=180.0)
        self.assert_expected_documents(marker, expected_payloads)

        collector_after = self.service_state("collector")

        self.assertEqual(
            collector_after["containerId"],
            collector_before["containerId"],
        )
        self.assertEqual(
            collector_after["StartedAt"],
            collector_before["StartedAt"],
        )

    def test_03_collector_restart_preserves_broker_and_consumer(self) -> None:
        marker = f"recovery-collector-restart-{self.provider}-{uuid4().hex}"
        broker_before = self.service_state("broker")
        consumer_before = self.service_state("consumer")
        collector_before = self.service_state("collector")
        before_payloads = self.create_payloads(
            marker,
            "TCP",
            4,
            phase="before-restart",
        )

        self.send_tcp(before_payloads)
        self.assert_expected_documents(
            marker,
            {payload: "TCP" for payload in before_payloads},
        )

        self.compose("restart", "collector")
        collector_after = self.wait_for_service_state("collector", running=True)
        self.wait_for_infrastructure()
        self.wait_for_udp_pipeline(timeout=45.0)

        self.assertEqual(
            collector_after["containerId"],
            collector_before["containerId"],
        )
        self.assertNotEqual(
            collector_after["StartedAt"],
            collector_before["StartedAt"],
        )

        broker_after = self.service_state("broker")
        consumer_after = self.service_state("consumer")

        self.assertEqual(broker_after["StartedAt"], broker_before["StartedAt"])
        self.assertEqual(consumer_after["StartedAt"], consumer_before["StartedAt"])

        tcp_payloads = self.create_payloads(
            marker,
            "TCP",
            4,
            phase="after-restart",
        )
        udp_payloads = self.create_payloads(
            marker,
            "UDP",
            4,
            phase="after-restart",
        )

        self.send_tcp(tcp_payloads)
        self.send_udp(udp_payloads)

        expected_payloads = {
            **{payload: "TCP" for payload in before_payloads},
            **{payload: "TCP" for payload in tcp_payloads},
            **{payload: "UDP" for payload in udp_payloads},
        }

        self.assert_expected_documents(marker, expected_payloads)

    def test_04_consumer_restart_preserves_pending_messages(self) -> None:
        marker = f"recovery-consumer-restart-{self.provider}-{uuid4().hex}"
        consumer_before = self.service_state("consumer")
        pending_tcp_payloads = self.create_payloads(
            marker,
            "TCP",
            8,
            phase="pending",
        )
        pending_udp_payloads = self.create_payloads(
            marker,
            "UDP",
            4,
            phase="pending",
        )
        pending_payloads = {
            **{payload: "TCP" for payload in pending_tcp_payloads},
            **{payload: "UDP" for payload in pending_udp_payloads},
        }

        self.compose("stop", "consumer")
        self.wait_for_service_state("consumer", running=False)

        try:
            initial_backlog = self.broker_backlog()
            self.send_tcp(pending_tcp_payloads)
            self.send_udp(pending_udp_payloads)
            self.wait_for_broker_backlog(initial_backlog + len(pending_payloads))

            self.assertEqual(self.count_documents(marker), 0)

            self.compose("start", "consumer")
            self.compose("restart", "consumer")
        finally:
            consumer_state = self.service_state("consumer")

            if not consumer_state.get("Running", False):
                self.compose("start", "consumer", check=False)

        consumer_after = self.wait_for_service_state("consumer", running=True)
        self.wait_for_consumer_replicas(1, timeout=90.0)

        self.assertEqual(
            consumer_after["containerId"],
            consumer_before["containerId"],
        )
        self.assertNotEqual(
            consumer_after["StartedAt"],
            consumer_before["StartedAt"],
        )

        self.assert_expected_documents(marker, pending_payloads)

        resumed_tcp_payloads = self.create_payloads(
            marker,
            "TCP",
            4,
            phase="resumed",
        )
        resumed_udp_payloads = self.create_payloads(
            marker,
            "UDP",
            4,
            phase="resumed",
        )

        self.send_tcp(resumed_tcp_payloads)
        self.send_udp(resumed_udp_payloads)

        expected_payloads = {
            **pending_payloads,
            **{payload: "TCP" for payload in resumed_tcp_payloads},
            **{payload: "UDP" for payload in resumed_udp_payloads},
        }
        documents = self.assert_expected_documents(marker, expected_payloads)

        self.assertEqual(
            len({document["messageId"] for document in documents}),
            len(expected_payloads),
        )


if __name__ == "__main__":
    unittest.main()
