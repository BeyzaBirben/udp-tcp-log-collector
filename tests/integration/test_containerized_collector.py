import base64
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import threading
import time
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
PORT = 5514
TEST_TIMEOUT = 20


def run_command(command, check=True):
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    if check and result.returncode != 0:
        command_text = " ".join(command)
        raise RuntimeError(
            f"Command failed: {command_text}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def compose(*arguments, check=True):
    return run_command(["docker", "compose", *arguments], check=check)


def wait_for_tcp():
    deadline = time.monotonic() + TEST_TIMEOUT
    last_error = None

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=1):
                return
        except OSError as error:
            last_error = error
            time.sleep(0.2)

    raise RuntimeError("Container TCP listener did not become ready") from last_error


def read_records():
    result = compose("logs", "--no-color", "collector")
    output = f"{result.stdout}\n{result.stderr}"
    records = []

    for line in output.splitlines():
        json_start = line.find("{")
        if json_start == -1:
            continue

        try:
            record = json.loads(line[json_start:])
        except json.JSONDecodeError:
            continue

        if isinstance(record, dict) and "protocol" in record:
            records.append(record)

    return records


def wait_for_records(marker, expected_count):
    deadline = time.monotonic() + TEST_TIMEOUT

    while time.monotonic() < deadline:
        records = [
            record
            for record in read_records()
            if marker in record.get("raw_log_text", "")
        ]
        if len(records) >= expected_count:
            return records

        time.sleep(0.2)

    records = [
        record
        for record in read_records()
        if marker in record.get("raw_log_text", "")
    ]
    raise AssertionError(
        f"Expected {expected_count} record(s) for {marker}, found {len(records)}"
    )


def collector_container_id():
    container_id = compose("ps", "-q", "collector").stdout.strip()
    if not container_id:
        raise RuntimeError("Collector container is not running")
    return container_id


def inspect_container(container_id):
    result = run_command(["docker", "inspect", container_id])
    return json.loads(result.stdout)[0]


def collector_gateway():
    container = inspect_container(collector_container_id())
    networks = container["NetworkSettings"]["Networks"]
    gateways = {
        network["Gateway"]
        for network in networks.values()
        if network.get("Gateway")
    }

    if len(gateways) != 1:
        raise RuntimeError("Collector network gateway could not be determined")

    gateway = gateways.pop()
    ipaddress.ip_address(gateway)
    return gateway


def send_udp(payload):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
        client.sendto(payload, (HOST, PORT))

def wait_for_udp():
    marker = f"container-udp-ready-{uuid.uuid4().hex}"
    deadline = time.monotonic() + TEST_TIMEOUT

    while time.monotonic() < deadline:
        send_udp(marker.encode("utf-8"))
        time.sleep(0.25)

        if any(
            marker in record.get("raw_log_text", "")
            for record in read_records()
        ):
            return

    raise RuntimeError("Container UDP listener did not become ready")

def send_tcp(text):
    with socket.create_connection((HOST, PORT), timeout=5) as client:
        client.sendall(text.encode("utf-8") + b"\n")


def send_tcp_clients(marker, client_count):
    barrier = threading.Barrier(client_count)

    def worker(client_id):
        with socket.create_connection((HOST, PORT), timeout=5) as client:
            barrier.wait(timeout=10)
            message = (
                f"{marker}|clientId=tcp-{client_id:03d}|messageId=0001"
            )
            client.sendall(message.encode("utf-8") + b"\n")

    with ThreadPoolExecutor(max_workers=client_count) as executor:
        futures = [
            executor.submit(worker, client_id)
            for client_id in range(1, client_count + 1)
        ]
        for future in futures:
            future.result(timeout=15)


def abort_connection(client):
    linger_format = "hh" if os.name == "nt" else "ii"
    linger_value = struct.pack(linger_format, 1, 0)
    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger_value)
    client.close()


def wait_for_ports_available():
    deadline = time.monotonic() + TEST_TIMEOUT
    last_error = None

    while time.monotonic() < deadline:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            tcp_socket.bind((HOST, PORT))
            tcp_socket.listen(1)
            udp_socket.bind((HOST, PORT))
            return
        except OSError as error:
            last_error = error
            time.sleep(0.2)
        finally:
            tcp_socket.close()
            udp_socket.close()

    raise RuntimeError("TCP and UDP ports were not released") from last_error


@unittest.skipUnless(
    os.environ.get("RUN_CONTAINER_TESTS") == "1",
    "Container integration tests are opt-in",
)
class ContainerizedCollectorIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_command(["docker", "info"])
        compose("down", "--remove-orphans", check=False)

        try:
            compose("up", "--build", "-d")
            wait_for_tcp()
            wait_for_udp()
            cls.gateway = collector_gateway()
        except BaseException:
            compose("down", "--remove-orphans", check=False)
            raise

    @classmethod
    def tearDownClass(cls):
        compose("down", "--remove-orphans", check=False)

    def assert_source_address(self, record):
        self.assertEqual(record["source_ip"], self.gateway)
        self.assertGreater(record["source_port"], 0)
        self.assertLessEqual(record["source_port"], 65535)

    def test_01_receives_lossless_udp_payload(self):
        marker = f"container-udp-{uuid.uuid4().hex}"
        payload = marker.encode("utf-8") + b"\x00\xff"

        send_udp(payload)
        records = wait_for_records(marker, 1)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["protocol"], "UDP")
        self.assertEqual(
            base64.b64decode(records[0]["raw_log_base64"]),
            payload,
        )
        self.assertEqual(records[0]["raw_log_size"], len(payload))
        self.assert_source_address(records[0])

    def test_02_handles_1_5_and_20_concurrent_tcp_clients(self):
        for client_count in (1, 5, 20):
            with self.subTest(client_count=client_count):
                marker = (
                    f"container-tcp-{client_count}-"
                    f"{uuid.uuid4().hex}"
                )

                send_tcp_clients(marker, client_count)
                records = wait_for_records(marker, client_count)

                self.assertEqual(len(records), client_count)
                self.assertTrue(
                    all(record["protocol"] == "TCP" for record in records)
                )
                self.assertEqual(
                    len({record["source_port"] for record in records}),
                    client_count,
                )

                for client_id in range(1, client_count + 1):
                    expected_id = f"clientId=tcp-{client_id:03d}"
                    self.assertTrue(
                        any(
                            expected_id in record["raw_log_text"]
                            for record in records
                        )
                    )

                for record in records:
                    self.assertEqual(
                        base64.b64decode(record["raw_log_base64"]),
                        record["raw_log_text"].encode("utf-8"),
                    )
                    self.assert_source_address(record)

    def test_03_client_disconnect_does_not_interrupt_others(self):
        test_id = uuid.uuid4().hex
        healthy_marker = f"container-disconnect-{test_id}-healthy"
        broken_marker = f"container-disconnect-{test_id}-broken"
        barrier = threading.Barrier(3)
        disconnected = threading.Event()

        def healthy_client(client_id):
            with socket.create_connection((HOST, PORT), timeout=5) as client:
                barrier.wait(timeout=10)

                first_message = (
                    f"{healthy_marker}|clientId=tcp-{client_id:03d}|"
                    f"messageId=0001"
                )
                client.sendall(first_message.encode("utf-8") + b"\n")

                if not disconnected.wait(timeout=5):
                    raise TimeoutError("Broken client did not disconnect")

                second_message = (
                    f"{healthy_marker}|clientId=tcp-{client_id:03d}|"
                    f"messageId=0002"
                )
                client.sendall(second_message.encode("utf-8") + b"\n")

        def broken_client():
            client = socket.create_connection((HOST, PORT), timeout=5)
            try:
                barrier.wait(timeout=10)
                client.sendall(
                    f"{broken_marker}|partial".encode("utf-8")
                )
                abort_connection(client)
            finally:
                disconnected.set()
                client.close()

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(healthy_client, 1),
                executor.submit(healthy_client, 2),
                executor.submit(broken_client),
            ]
            for future in futures:
                future.result(timeout=15)

        records = wait_for_records(healthy_marker, 4)

        self.assertEqual(len(records), 4)
        self.assertEqual(
            len({record["source_port"] for record in records}),
            2,
        )
        self.assertTrue(
            compose(
                "ps",
                "--status",
                "running",
                "-q",
                "collector",
            ).stdout.strip()
        )

        for client_id in (1, 2):
            for message_id in (1, 2):
                expected_message = (
                    f"clientId=tcp-{client_id:03d}|"
                    f"messageId={message_id:04d}"
                )
                self.assertTrue(
                    any(
                        expected_message in record["raw_log_text"]
                        for record in records
                    )
                )

        for record in records:
            self.assert_source_address(record)

    def test_04_processes_udp_and_tcp_simultaneously(self):
        test_id = uuid.uuid4().hex
        udp_marker = f"container-simultaneous-{test_id}-udp"
        tcp_marker = f"container-simultaneous-{test_id}-tcp"
        barrier = threading.Barrier(2)

        def udp_sender():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                barrier.wait(timeout=10)
                client.sendto(udp_marker.encode("utf-8"), (HOST, PORT))

        def tcp_sender():
            with socket.create_connection((HOST, PORT), timeout=5) as client:
                barrier.wait(timeout=10)
                client.sendall(tcp_marker.encode("utf-8") + b"\n")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(udp_sender),
                executor.submit(tcp_sender),
            ]
            for future in futures:
                future.result(timeout=15)

        udp_records = wait_for_records(udp_marker, 1)
        tcp_records = wait_for_records(tcp_marker, 1)

        self.assertEqual(udp_records[0]["protocol"], "UDP")
        self.assertEqual(tcp_records[0]["protocol"], "TCP")
        self.assert_source_address(udp_records[0])
        self.assert_source_address(tcp_records[0])

    def test_05_stops_cleanly_and_releases_ports(self):
        container_id = collector_container_id()

        compose("stop")

        container = inspect_container(container_id)
        self.assertEqual(container["State"]["ExitCode"], 0)

        logs = compose("logs", "--no-color", "collector")
        output = f"{logs.stdout}\n{logs.stderr}"
        self.assertIn("TCP listener stopped", output)
        self.assertIn("UDP listener stopped", output)

        wait_for_ports_available()

        compose("up", "-d")
        wait_for_tcp()
        wait_for_udp()
        self.gateway = collector_gateway()

        marker = f"container-restarted-{uuid.uuid4().hex}"
        send_udp(marker.encode("utf-8"))
        records = wait_for_records(marker, 1)

        self.assertEqual(records[0]["protocol"], "UDP")
        self.assert_source_address(records[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)