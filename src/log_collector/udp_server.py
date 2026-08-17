import asyncio
import logging
import socket
from collections.abc import Callable
from typing import cast

from .models import LogRecord


logger = logging.getLogger(__name__)
RecordHandler = Callable[[LogRecord], None]


class UDPLogProtocol(asyncio.DatagramProtocol):
    def __init__(self, record_handler: RecordHandler) -> None:
        self.record_handler = record_handler

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        address = transport.get_extra_info("sockname")
        logger.info("UDP listener started on %s", address)

    def datagram_received(
        self,
        data: bytes,
        address: tuple[str, int],
    ) -> None:
        source_ip, source_port = address
        record = LogRecord.capture(
            protocol="UDP",
            source_ip=source_ip,
            source_port=source_port,
            raw_log=data,
        )

        try:
            self.record_handler(record)
        except Exception:
            logger.exception("UDP record handling failed")

    def error_received(self, exception: Exception) -> None:
        logger.error("UDP listener error: %s", exception)

    def connection_lost(self, exception: Exception | None) -> None:
        if exception is None:
            logger.info("UDP listener stopped")
        else:
            logger.error("UDP listener stopped with error: %s", exception)


async def start_udp_listener(
    host: str,
    port: int,
    record_handler: RecordHandler,
) -> asyncio.DatagramTransport:
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPLogProtocol(record_handler),
        local_addr=(host, port),
        family=socket.AF_INET,
    )
    return cast(asyncio.DatagramTransport, transport)