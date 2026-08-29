import asyncio
import socket
import unittest

from log_collector.models import LogRecord
from log_collector.tcp_server import TCPLogServer, start_tcp_listener
from log_collector.udp_server import start_udp_listener


class TransportConcurrencyIntegrationTests(
    unittest.IsolatedAsyncioTestCase
):
    records: asyncio.Queue[LogRecord]
    tcp_server: TCPLogServer
    udp_transport: asyncio.DatagramTransport
    listener_port: int

    async def asyncSetUp(self) -> None:
        self.records = asyncio.Queue()
        last_error: OSError | None = None

        for _ in range(50):
            udp_transport = await start_udp_listener(
                host="127.0.0.1",
                port=0,
                record_handler=self.records.put_nowait,
            )

            listener_address = udp_transport.get_extra_info("sockname")

            if listener_address is None:
                udp_transport.close()
                await asyncio.sleep(0)
                self.fail("UDP listener address is unavailable")

            listener_port = int(listener_address[1])

            try:
                tcp_server = await start_tcp_listener(
                    host="127.0.0.1",
                    port=listener_port,
                    record_handler=self.records.put_nowait,
                )
            except OSError as error:
                last_error = error
                udp_transport.close()
                await asyncio.sleep(0)
                continue

            self.udp_transport = udp_transport
            self.tcp_server = tcp_server
            self.listener_port = listener_port
            return

        raise RuntimeError(
            "A shared TCP and UDP port is unavailable"
        ) from last_error

    async def asyncTearDown(self) -> None:
        await self.tcp_server.close()
        self.udp_transport.close()
        await asyncio.sleep(0)

    def get_writer_address(
        self,
        writer: asyncio.StreamWriter,
    ) -> tuple[str, int]:
        address = writer.get_extra_info("sockname")

        if address is None:
            raise RuntimeError("TCP client address is unavailable")

        return str(address[0]), int(address[1])

    async def close_writer(
        self,
        writer: asyncio.StreamWriter,
    ) -> None:
        writer.close()

        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def send_tcp(
        self,
        payload: bytes,
    ) -> tuple[str, int]:
        _, writer = await asyncio.open_connection(
            "127.0.0.1",
            self.listener_port,
        )
        address = self.get_writer_address(writer)

        try:
            writer.write(payload + b"\n")
            await writer.drain()
            return address
        finally:
            await self.close_writer(writer)

    async def send_udp(
        self,
        payload: bytes,
    ) -> tuple[str, int]:
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        ) as client:
            client.bind(("127.0.0.1", 0))
            address = client.getsockname()
            client.sendto(
                payload,
                ("127.0.0.1", self.listener_port),
            )

        await asyncio.sleep(0)
        return str(address[0]), int(address[1])

    async def collect_expected_records(
        self,
        expected_payloads: set[bytes],
    ) -> list[LogRecord]:
        async def collect() -> list[LogRecord]:
            remaining = set(expected_payloads)
            collected: list[LogRecord] = []

            while remaining:
                record = await self.records.get()

                if record.raw_log in remaining:
                    remaining.remove(record.raw_log)
                    collected.append(record)

            return collected

        return await asyncio.wait_for(collect(), timeout=5)

    async def test_handles_1_5_and_20_concurrent_clients(
        self,
    ) -> None:
        for client_count in (1, 5, 20):
            with self.subTest(client_count=client_count):
                payloads = [
                    (
                        f"CEF:0|Test|TCP|1.0|{client_count}|"
                        f"Concurrent test|1| "
                        f"clientId=tcp-{client_count}-{index}"
                    ).encode("utf-8")
                    for index in range(client_count)
                ]

                addresses = await asyncio.gather(
                    *(self.send_tcp(payload) for payload in payloads)
                )
                records = await self.collect_expected_records(
                    set(payloads)
                )
                expected_sources = dict(zip(payloads, addresses))

                self.assertEqual(len(records), client_count)
                self.assertEqual(
                    {record.raw_log for record in records},
                    set(payloads),
                )

                for record in records:
                    expected_address = expected_sources[record.raw_log]
                    self.assertEqual(record.protocol, "TCP")
                    self.assertEqual(
                        record.source_ip,
                        expected_address[0],
                    )
                    self.assertEqual(
                        record.source_port,
                        expected_address[1],
                    )

    async def test_client_disconnect_does_not_interrupt_others(
        self,
    ) -> None:
        connections = await asyncio.gather(
            asyncio.open_connection(
                "127.0.0.1",
                self.listener_port,
            ),
            asyncio.open_connection(
                "127.0.0.1",
                self.listener_port,
            ),
            asyncio.open_connection(
                "127.0.0.1",
                self.listener_port,
            ),
        )

        _, first_writer = connections[0]
        _, second_writer = connections[1]
        _, broken_writer = connections[2]

        first_address = self.get_writer_address(first_writer)
        second_address = self.get_writer_address(second_writer)

        first_before = b"CEF:0|Test|TCP|1.0|1|Before drop|1| clientId=one"
        second_before = b"CEF:0|Test|TCP|1.0|2|Before drop|1| clientId=two"
        first_after = b"CEF:0|Test|TCP|1.0|3|After drop|1| clientId=one"
        second_after = b"CEF:0|Test|TCP|1.0|4|After drop|1| clientId=two"

        try:
            first_writer.write(first_before + b"\n")
            second_writer.write(second_before + b"\n")
            await asyncio.gather(
                first_writer.drain(),
                second_writer.drain(),
            )

            broken_writer.write(b"CEF:0|Incomplete TCP log")
            await broken_writer.drain()
            broken_writer.transport.abort()
            await asyncio.sleep(0.05)

            first_writer.write(first_after + b"\n")
            second_writer.write(second_after + b"\n")
            await asyncio.gather(
                first_writer.drain(),
                second_writer.drain(),
            )

            expected_payloads = {
                first_before,
                second_before,
                first_after,
                second_after,
            }
            records = await self.collect_expected_records(
                expected_payloads
            )
            records_by_payload = {
                record.raw_log: record for record in records
            }

            self.assertEqual(
                records_by_payload[first_before].source_port,
                first_address[1],
            )
            self.assertEqual(
                records_by_payload[first_after].source_port,
                first_address[1],
            )
            self.assertEqual(
                records_by_payload[second_before].source_port,
                second_address[1],
            )
            self.assertEqual(
                records_by_payload[second_after].source_port,
                second_address[1],
            )
        finally:
            broken_writer.transport.abort()
            await self.close_writer(first_writer)
            await self.close_writer(second_writer)

    async def test_processes_udp_and_tcp_at_the_same_time(
        self,
    ) -> None:
        tcp_payload = (
            b"CEF:0|Test|TCP|1.0|10|Simultaneous|1| clientId=tcp"
        )
        udp_payload = (
            b"CEF:0|Test|UDP|1.0|20|Simultaneous|1| clientId=udp"
        )

        tcp_address, udp_address = await asyncio.gather(
            self.send_tcp(tcp_payload),
            self.send_udp(udp_payload),
        )
        records = await self.collect_expected_records(
            {tcp_payload, udp_payload}
        )
        records_by_protocol = {
            record.protocol: record for record in records
        }

        self.assertEqual(
            records_by_protocol["TCP"].raw_log,
            tcp_payload,
        )
        self.assertEqual(
            records_by_protocol["TCP"].source_ip,
            tcp_address[0],
        )
        self.assertEqual(
            records_by_protocol["TCP"].source_port,
            tcp_address[1],
        )
        self.assertEqual(
            records_by_protocol["UDP"].raw_log,
            udp_payload,
        )
        self.assertEqual(
            records_by_protocol["UDP"].source_ip,
            udp_address[0],
        )
        self.assertEqual(
            records_by_protocol["UDP"].source_port,
            udp_address[1],
        )