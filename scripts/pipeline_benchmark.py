import argparse
import json
import math
import os
import re
import socket
import subprocess
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any
from uuid import uuid4

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def positive_int(value: str) -> int:
    parsed = int(value)

    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer")

    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)

    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive number")

    return parsed


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_memory_mib(value: str) -> float:
    current_usage = value.split("/", maxsplit=1)[0].strip().replace(" ", "")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)", current_usage)

    if match is None:
        raise ValueError(f"Unsupported Docker memory value: {value}")

    amount = float(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "b": 1 / (1024 * 1024),
        "kb": 1000 / (1024 * 1024),
        "kib": 1 / 1024,
        "mb": 1_000_000 / (1024 * 1024),
        "mib": 1.0,
        "gb": 1_000_000_000 / (1024 * 1024),
        "gib": 1024.0,
        "tb": 1_000_000_000_000 / (1024 * 1024),
        "tib": 1024.0 * 1024.0,
    }

    if unit not in factors:
        raise ValueError(f"Unsupported Docker memory unit: {unit}")

    return amount * factors[unit]


@dataclass(frozen=True, slots=True)
class BenchmarkSettings:
    provider: str
    host: str
    collector_port: int
    mongodb_port: int
    mongodb_database: str
    mongodb_collection: str
    tcp_count: int
    udp_count: int
    tcp_clients: int
    consumer_count: int
    udp_rate: float
    udp_burst_size: int
    timeout: float
    idle_timeout: float
    sample_interval: float
    build_images: bool
    keep_stack: bool
    output_path: Path

    def __post_init__(self) -> None:
        if self.provider not in {"rabbitmq", "kafka"}:
            raise ValueError("Provider must be rabbitmq or kafka")

        if self.tcp_clients > self.tcp_count:
            raise ValueError("TCP client count cannot exceed TCP message count")


class CommandRunner:
    def __init__(self, environment: dict[str, str]) -> None:
        self.environment = environment

    def run(
        self,
        arguments: list[str],
        *,
        check: bool = True,
        timeout: float = 600.0,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            arguments,
            cwd=PROJECT_ROOT,
            env=self.environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )

        if check and result.returncode != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(arguments)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        return result

    def compose(
        self,
        *arguments: str,
        check: bool = True,
        timeout: float = 600.0,
    ) -> subprocess.CompletedProcess[str]:
        return self.run(
            ["docker", "compose", *arguments],
            check=check,
            timeout=timeout,
        )


class DockerResourceMonitor:
    def __init__(self, runner: CommandRunner, interval: float) -> None:
        self.runner = runner
        self.interval = interval
        self.container_metadata: dict[str, dict[str, str]] = {}
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        container_ids = self.runner.compose("ps", "-q").stdout.splitlines()

        if not container_ids:
            raise RuntimeError("No running Compose containers are available")

        inspection = self.runner.run(["docker", "inspect", *container_ids])

        for container in json.loads(inspection.stdout):
            container_id = str(container["Id"])
            labels = container.get("Config", {}).get("Labels", {}) or {}
            self.container_metadata[container_id] = {
                "name": str(container.get("Name", "")).lstrip("/"),
                "service": str(labels.get("com.docker.compose.service", "unknown")),
            }

        self.thread = threading.Thread(
            target=self._run,
            name="docker-resource-monitor",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()

        if self.thread is not None:
            self.thread.join(timeout=max(10.0, self.interval * 4))

    def _run(self) -> None:
        sample_id = 0

        while not self.stop_event.is_set():
            sample_started = time.monotonic()

            try:
                self._capture_sample(sample_id)
            except Exception as error:
                self.errors.append(str(error))

            sample_id += 1
            elapsed = time.monotonic() - sample_started
            self.stop_event.wait(max(0.0, self.interval - elapsed))

    def _capture_sample(self, sample_id: int) -> None:
        container_ids = list(self.container_metadata)
        result = self.runner.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                *container_ids,
            ],
            timeout=60.0,
        )
        captured_at = utc_now().isoformat()

        for line in result.stdout.splitlines():
            if not line.strip():
                continue

            payload = json.loads(line)
            container_id = str(payload.get("ID", ""))
            metadata = self._metadata_for(container_id, str(payload.get("Name", "")))
            cpu_percent = float(str(payload["CPUPerc"]).rstrip("%"))
            memory_mib = parse_memory_mib(str(payload["MemUsage"]))
            self.samples.append(
                {
                    "sampleId": sample_id,
                    "capturedAt": captured_at,
                    "container": metadata["name"],
                    "service": metadata["service"],
                    "cpuPercent": cpu_percent,
                    "memoryMiB": memory_mib,
                }
            )

    def _metadata_for(self, container_id: str, name: str) -> dict[str, str]:
        for known_id, metadata in self.container_metadata.items():
            if known_id.startswith(container_id) or metadata["name"] == name:
                return metadata

        return {"name": name, "service": "unknown"}

    def summary(self) -> dict[str, Any]:
        aggregated: dict[tuple[str, int], dict[str, float]] = defaultdict(
            lambda: {"cpuPercent": 0.0, "memoryMiB": 0.0, "containers": 0.0}
        )

        for sample in self.samples:
            key = (str(sample["service"]), int(sample["sampleId"]))
            aggregated[key]["cpuPercent"] += float(sample["cpuPercent"])
            aggregated[key]["memoryMiB"] += float(sample["memoryMiB"])
            aggregated[key]["containers"] += 1

        by_service: dict[str, list[dict[str, float]]] = defaultdict(list)

        for (service, _), values in aggregated.items():
            by_service[service].append(values)

        summary: dict[str, Any] = {}

        for service, samples in sorted(by_service.items()):
            cpu_values = [sample["cpuPercent"] for sample in samples]
            memory_values = [sample["memoryMiB"] for sample in samples]
            container_counts = [sample["containers"] for sample in samples]
            summary[service] = {
                "sampleCount": len(samples),
                "containerCount": int(max(container_counts)),
                "averageCpuPercent": round(fmean(cpu_values), 3),
                "peakCpuPercent": round(max(cpu_values), 3),
                "averageMemoryMiB": round(fmean(memory_values), 3),
                "peakMemoryMiB": round(max(memory_values), 3),
            }

        return summary


class PipelineBenchmark:
    def __init__(self, settings: BenchmarkSettings) -> None:
        self.settings = settings
        self.run_token = uuid4().hex
        self.broker_destination = f"benchmark-raw-logs-{self.run_token}"
        environment = dict(os.environ)
        environment["BROKER_PROVIDER"] = settings.provider
        environment["RABBITMQ_QUEUE"] = self.broker_destination
        environment["KAFKA_TOPIC"] = self.broker_destination
        environment["KAFKA_CONSUMER_GROUP"] = (
            f"benchmark-consumers-{self.run_token}"
        )
        environment["COLLECTOR_PORT"] = str(settings.collector_port)
        environment["MONGODB_PORT"] = str(settings.mongodb_port)
        environment["MONGODB_DATABASE"] = settings.mongodb_database
        environment["MONGODB_COLLECTION"] = settings.mongodb_collection
        environment["COLLECTOR_STDOUT_ENABLED"] = "false"
        self.runner = CommandRunner(environment)
        self.database_client: MongoClient | None = None
        self.collection: Any = None

    def execute(self) -> dict[str, Any]:
        self.start_stack()

        try:
            self.connect_database()
            self.wait_for_stack()
            self.wait_for_consumers()
            self.wait_for_udp_pipeline()
            result = self.run_workload()
            self.write_result(result)
            return result
        except Exception:
            logs = self.runner.compose("logs", "--tail", "100", check=False)
            print(logs.stdout)
            print(logs.stderr)
            raise
        finally:
            if self.database_client is not None:
                self.database_client.close()

            if not self.settings.keep_stack:
                self.runner.compose("down", "--remove-orphans", check=False)

    def start_stack(self) -> None:
        self.runner.run(["docker", "info"])
        self.runner.compose("down", "--remove-orphans", check=False)
        arguments = ["up", "-d"]

        if self.settings.build_images:
            arguments.append("--build")

        arguments.extend(
            ["--scale", f"consumer={self.settings.consumer_count}"]
        )
        self.runner.compose(*arguments, timeout=900.0)

    def connect_database(self) -> None:
        self.database_client = MongoClient(
            f"mongodb://127.0.0.1:{self.settings.mongodb_port}/",
            serverSelectionTimeoutMS=5000,
            tz_aware=True,
        )
        self.collection = self.database_client[self.settings.mongodb_database][
            self.settings.mongodb_collection
        ]

    def wait_for_stack(self) -> None:
        if self.database_client is None:
            raise RuntimeError("MongoDB client is unavailable")

        deadline = time.monotonic() + 120.0
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                self.database_client.admin.command("ping")

                with socket.create_connection(
                    (self.settings.host, self.settings.collector_port),
                    timeout=1.0,
                ):
                    return
            except Exception as error:
                last_error = error
                time.sleep(0.25)

        raise TimeoutError("Collector and MongoDB did not become ready") from last_error

    def wait_for_consumers(self) -> None:
        deadline = time.monotonic() + 120.0

        while time.monotonic() < deadline:
            consumer_ids = self.runner.compose(
                "ps",
                "-q",
                "consumer",
            ).stdout.splitlines()

            if len(consumer_ids) == self.settings.consumer_count:
                ready_count = 0

                for container_id in consumer_ids:
                    logs = self.runner.run(
                        ["docker", "logs", "--tail", "100", container_id],
                        check=False,
                    )

                    if "Log consumer connected with batch_size=" in (
                        logs.stdout + logs.stderr
                    ):
                        ready_count += 1

                if ready_count == self.settings.consumer_count:
                    return

            time.sleep(0.5)

        raise TimeoutError(
            f"Expected {self.settings.consumer_count} ready consumer replica(s)"
        )

    def wait_for_udp_pipeline(self) -> None:
        marker = f"benchmark-readiness-{self.settings.provider}-{uuid4().hex}"
        payload = marker.encode("utf-8")
        deadline = time.monotonic() + 45.0

        while time.monotonic() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                client.sendto(
                    payload,
                    (self.settings.host, self.settings.collector_port),
                )

            attempt_deadline = min(time.monotonic() + 1.0, deadline)

            while time.monotonic() < attempt_deadline:
                document = self.collection.find_one(
                    {"rawLog": marker, "protocol": "UDP"}
                )

                if document is not None:
                    return

                time.sleep(0.1)

        raise TimeoutError("UDP pipeline did not become ready")

    def run_workload(self) -> dict[str, Any]:
        marker = f"benchmark-{self.settings.provider}-{self.run_token}"
        tcp_groups = self.create_tcp_payloads(marker)
        udp_payloads = self.create_udp_payloads(marker)
        expected_payloads = {
            payload.decode("utf-8")
            for group in tcp_groups
            for payload in group
        }
        expected_payloads.update(payload.decode("utf-8") for payload in udp_payloads)
        worker_count = self.settings.tcp_clients + 1
        barrier = threading.Barrier(worker_count + 1)
        monitor = DockerResourceMonitor(self.runner, self.settings.sample_interval)
        monitor.start()
        started_at = utc_now()
        send_started = time.perf_counter()

        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures: list[Future[None]] = [
                    executor.submit(self.send_tcp, payloads, barrier)
                    for payloads in tcp_groups
                ]
                futures.append(executor.submit(self.send_udp, udp_payloads, barrier))
                barrier.wait(timeout=60.0)

                for future in futures:
                    future.result(timeout=self.settings.timeout)

            send_completed = time.perf_counter()
            self.wait_for_documents(
                marker,
                self.settings.tcp_count + self.settings.udp_count,
            )
            completed_at = utc_now()
            completed = time.perf_counter()
        finally:
            monitor.stop()

        documents = list(
            self.collection.find(
                self.marker_query(marker),
                {"_id": 0, "messageId": 1, "protocol": 1, "rawLog": 1},
            )
        )
        actual_payloads = [str(document.get("rawLog", "")) for document in documents]
        payload_counts = Counter(actual_payloads)
        actual_payload_set = set(actual_payloads)
        message_ids = [str(document.get("messageId", "")) for document in documents]
        protocol_counts = Counter(
            str(document.get("protocol", "")) for document in documents
        )
        missing_payloads = expected_payloads - actual_payload_set
        unexpected_payloads = actual_payload_set - expected_payloads
        duplicate_payload_count = sum(
            count - 1 for count in payload_counts.values() if count > 1
        )
        duplicate_message_id_count = len(message_ids) - len(set(message_ids))
        expected_count = self.settings.tcp_count + self.settings.udp_count
        send_duration = send_completed - send_started
        end_to_end_duration = completed - send_started
        drain_duration = completed - send_completed
        resources_by_service = monitor.summary()
        required_services = {"broker", "collector", "consumer", "database"}
        resource_metrics_available = (
            required_services.issubset(resources_by_service)
            and not monitor.errors
        )
        validation = {
            "passed": (
                len(documents) == expected_count
                and not missing_payloads
                and not unexpected_payloads
                and duplicate_payload_count == 0
                and duplicate_message_id_count == 0
                and protocol_counts["TCP"] == self.settings.tcp_count
                and protocol_counts["UDP"] == self.settings.udp_count
                and resource_metrics_available
            ),
            "noDataLoss": not missing_payloads,
            "noUnexpectedPayloads": not unexpected_payloads,
            "noDuplicatePayloads": duplicate_payload_count == 0,
            "noDuplicateMessageIds": duplicate_message_id_count == 0,
            "protocolCountsMatch": (
                protocol_counts["TCP"] == self.settings.tcp_count
                and protocol_counts["UDP"] == self.settings.udp_count
            ),
            "resourceMetricsAvailable": resource_metrics_available,
        }

        return {
            "benchmarkId": marker,
            "provider": self.settings.provider,
            "startedAt": started_at.isoformat(),
            "completedAt": completed_at.isoformat(),
            "configuration": {
                "tcpMessages": self.settings.tcp_count,
                "udpMessages": self.settings.udp_count,
                "tcpClients": self.settings.tcp_clients,
                "consumerReplicas": self.settings.consumer_count,
                "udpMessagesPerSecond": self.settings.udp_rate,
                "udpBurstSize": self.settings.udp_burst_size,
                "idleTimeoutSeconds": self.settings.idle_timeout,
                "resourceSampleIntervalSeconds": self.settings.sample_interval,
                "brokerDestination": self.broker_destination,
            },
            "counts": {
                "expected": expected_count,
                "persisted": len(documents),
                "tcpPersisted": protocol_counts["TCP"],
                "udpPersisted": protocol_counts["UDP"],
                "missing": len(missing_payloads),
                "unexpected": len(unexpected_payloads),
                "duplicatePayloads": duplicate_payload_count,
                "duplicateMessageIds": duplicate_message_id_count,
            },
            "timingsSeconds": {
                "send": round(send_duration, 6),
                "brokerAndDatabaseDrain": round(drain_duration, 6),
                "endToEnd": round(end_to_end_duration, 6),
            },
            "throughputLogsPerSecond": {
                "send": round(expected_count / send_duration, 3),
                "endToEnd": round(len(documents) / end_to_end_duration, 3),
            },
            "resourcesByService": resources_by_service,
            "resourceMonitoringErrors": monitor.errors,
            "validation": validation,
        }

    def create_tcp_payloads(self, marker: str) -> list[list[bytes]]:
        base_size, remainder = divmod(
            self.settings.tcp_count,
            self.settings.tcp_clients,
        )
        groups: list[list[bytes]] = []
        message_index = 1

        for client_index in range(1, self.settings.tcp_clients + 1):
            group_size = base_size + int(client_index <= remainder)
            payloads = [
                (
                    f"{marker}|protocol=tcp|client={client_index:03d}|"
                    f"message={current_index:06d}"
                ).encode("utf-8")
                for current_index in range(
                    message_index,
                    message_index + group_size,
                )
            ]
            groups.append(payloads)
            message_index += group_size

        return groups

    def create_udp_payloads(self, marker: str) -> list[bytes]:
        return [
            (
                f"{marker}|protocol=udp|client=001|message={index:06d}"
            ).encode("utf-8")
            for index in range(1, self.settings.udp_count + 1)
        ]

    def send_tcp(
        self,
        payloads: list[bytes],
        barrier: threading.Barrier,
    ) -> None:
        deadline = time.monotonic() + 30.0
        client: socket.socket | None = None
        last_error: OSError | None = None

        while time.monotonic() < deadline:
            try:
                client = socket.create_connection(
                    (self.settings.host, self.settings.collector_port),
                    timeout=15.0,
                )
                break
            except OSError as error:
                last_error = error
                time.sleep(0.1)

        if client is None:
            raise ConnectionError("TCP client could not connect") from last_error

        with client:
            client.settimeout(30.0)
            barrier.wait(timeout=60.0)

            for payload in payloads:
                client.sendall(payload + b"\n")

    def send_udp(
        self,
        payloads: list[bytes],
        barrier: threading.Barrier,
    ) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
            barrier.wait(timeout=60.0)
            started = time.perf_counter()

            for index, payload in enumerate(payloads, start=1):
                sent_size = client.sendto(
                    payload,
                    (self.settings.host, self.settings.collector_port),
                )

                if sent_size != len(payload):
                    raise RuntimeError("UDP payload was not sent completely")

                if (
                    index % self.settings.udp_burst_size == 0
                    or index == len(payloads)
                ):
                    target_elapsed = index / self.settings.udp_rate
                    remaining = target_elapsed - (time.perf_counter() - started)

                    if remaining > 0:
                        time.sleep(remaining)

    def marker_query(self, marker: str) -> dict[str, Any]:
        return {"rawLog": {"$regex": f"^{re.escape(marker)}\\|"}}

    def wait_for_documents(self, marker: str, expected_count: int) -> int:
        deadline = time.monotonic() + self.settings.timeout
        last_progress = time.monotonic()
        previous_count = -1
        count = 0

        while time.monotonic() < deadline:
            count = int(self.collection.count_documents(self.marker_query(marker)))

            if count >= expected_count:
                return count

            if count != previous_count:
                previous_count = count
                last_progress = time.monotonic()

            if time.monotonic() - last_progress >= self.settings.idle_timeout:
                return count

            time.sleep(0.25)

        return count

    def write_result(self, result: dict[str, Any]) -> None:
        self.settings.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="pipeline-benchmark")
    parser.add_argument(
        "--provider",
        choices=("rabbitmq", "kafka"),
        required=True,
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--collector-port",
        type=positive_int,
        default=int(os.environ.get("COLLECTOR_PORT", "5514")),
    )
    parser.add_argument(
        "--mongodb-port",
        type=positive_int,
        default=int(os.environ.get("MONGODB_PORT", "27017")),
    )
    parser.add_argument(
        "--mongodb-database",
        default=os.environ.get("MONGODB_DATABASE", "log_collection"),
    )
    parser.add_argument(
        "--mongodb-collection",
        default=os.environ.get("MONGODB_COLLECTION", "raw_logs"),
    )
    parser.add_argument("--tcp-count", type=positive_int, default=20_000)
    parser.add_argument("--udp-count", type=positive_int, default=5_000)
    parser.add_argument("--tcp-clients", type=positive_int, default=20)
    parser.add_argument("--consumers", type=positive_int, default=3)
    parser.add_argument("--udp-rate", type=positive_float, default=250.0)
    parser.add_argument("--udp-burst-size", type=positive_int, default=1)
    parser.add_argument("--timeout", type=positive_float, default=300.0)
    parser.add_argument("--idle-timeout", type=positive_float, default=15.0)
    parser.add_argument("--sample-interval", type=positive_float, default=0.5)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-stack", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def settings_from_arguments(arguments: argparse.Namespace) -> BenchmarkSettings:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    output_path = arguments.output or (
        PROJECT_ROOT
        / "benchmark-results"
        / f"{arguments.provider}-{timestamp}.json"
    )
    return BenchmarkSettings(
        provider=arguments.provider,
        host=arguments.host,
        collector_port=arguments.collector_port,
        mongodb_port=arguments.mongodb_port,
        mongodb_database=arguments.mongodb_database,
        mongodb_collection=arguments.mongodb_collection,
        tcp_count=arguments.tcp_count,
        udp_count=arguments.udp_count,
        tcp_clients=arguments.tcp_clients,
        consumer_count=arguments.consumers,
        udp_rate=arguments.udp_rate,
        udp_burst_size=arguments.udp_burst_size,
        timeout=arguments.timeout,
        idle_timeout=arguments.idle_timeout,
        sample_interval=arguments.sample_interval,
        build_images=not arguments.skip_build,
        keep_stack=arguments.keep_stack,
        output_path=output_path.resolve(),
    )


def print_summary(result: dict[str, Any], output_path: Path) -> None:
    status = "PASSED" if result["validation"]["passed"] else "FAILED"
    counts = result["counts"]
    timings = result["timingsSeconds"]
    throughput = result["throughputLogsPerSecond"]
    print(f"Benchmark status: {status}")
    print(f"Provider: {result['provider']}")
    print(f"Persisted logs: {counts['persisted']}/{counts['expected']}")
    print(f"TCP logs: {counts['tcpPersisted']}")
    print(f"UDP logs: {counts['udpPersisted']}")
    print(f"End-to-end duration: {timings['endToEnd']:.3f} seconds")
    print(f"End-to-end throughput: {throughput['endToEnd']:.3f} logs/second")
    print(f"Result file: {output_path}")


def main() -> None:
    arguments = parse_arguments()
    settings = settings_from_arguments(arguments)
    result = PipelineBenchmark(settings).execute()
    print_summary(result, settings.output_path)

    if not result["validation"]["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
