import asyncio
import base64
import socket
import unittest

from log_collector.models import LogRecord
from log_collector.udp_server import start_udp_listener


class UDPListenerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    records: asyncio.Queue[LogRecord]
    transport: asyncio.DatagramTransport
    listener_port: int

    async def asyncSetUp(self) -> None:
        self.records = asyncio.Queue()
        self.transport = await start_udp_listener(
            host="127.0.0.1",
            port=0,
            record_handler=self.records.put_nowait,
        )

        listener_address = self.transport.get_extra_info("sockname")

        if listener_address is None:
            self.fail("UDP listener address is unavailable")

        self.listener_port = listener_address[1]

    async def asyncTearDown(self) -> None:
        self.transport.close()
        await asyncio.sleep(0)

    async def test_receives_raw_log_and_source_address(self) -> None:
        payload = b"CEF:0|TestVendor|TestProduct|1.0|100|UDP test|1|\x00\xff"

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
            client.bind(("127.0.0.1", 0))
            client_address = client.getsockname()
            client.sendto(payload, ("127.0.0.1", self.listener_port))

        record = await asyncio.wait_for(self.records.get(), timeout=2)

        self.assertEqual(record.protocol, "UDP")
        self.assertEqual(record.source_ip, client_address[0])
        self.assertEqual(record.source_port, client_address[1])
        self.assertEqual(record.raw_log, payload)

        encoded_log = record.as_dict()["raw_log_base64"]
        self.assertEqual(base64.b64decode(encoded_log), payload)