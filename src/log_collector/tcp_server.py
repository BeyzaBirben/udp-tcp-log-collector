import asyncio
import logging
from collections.abc import Callable

from .models import LogRecord


logger = logging.getLogger(__name__)
RecordHandler = Callable[[LogRecord], None]
DEFAULT_MAX_FRAME_SIZE = 1024 * 1024


class TCPLogServer:
    def __init__(
        self,
        record_handler: RecordHandler,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
    ) -> None:
        self.record_handler = record_handler
        self.max_frame_size = max_frame_size
        self.server: asyncio.AbstractServer | None = None
        self.client_writers: set[asyncio.StreamWriter] = set()
        self.client_tasks: set[asyncio.Task[None]] = set()

    async def start(self, host: str, port: int) -> None:
        self.server = await asyncio.start_server(
            self.handle_client,
            host=host,
            port=port,
            limit=self.max_frame_size + 1,
        )
        addresses = [
            current_socket.getsockname()
            for current_socket in self.server.sockets or []
        ]
        logger.info("TCP listener started on %s", addresses)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        writers = tuple(self.client_writers)

        for writer in writers:
            writer.close()

        if writers:
            await asyncio.gather(
                *(self.wait_for_writer_close(writer) for writer in writers),
                return_exceptions=True,
            )

        active_tasks = tuple(
            task for task in self.client_tasks if not task.done()
        )

        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        logger.info("TCP listener stopped")

    async def wait_for_writer_close(
        self,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        current_task = asyncio.current_task()

        if current_task is not None:
            self.client_tasks.add(current_task)

        self.client_writers.add(writer)
        peer_address = writer.get_extra_info("peername")
        source_ip = str(peer_address[0])
        source_port = int(peer_address[1])

        logger.info(
            "TCP client connected from %s:%s",
            source_ip,
            source_port,
        )

        try:
            while True:
                frame = await reader.readline()

                if not frame:
                    break

                raw_log = frame[:-1] if frame.endswith(b"\n") else frame
                record = LogRecord.capture(
                    protocol="TCP",
                    source_ip=source_ip,
                    source_port=source_port,
                    raw_log=raw_log,
                )

                try:
                    self.record_handler(record)
                except Exception:
                    logger.exception("TCP record handling failed")
        except ValueError:
            logger.warning(
                "TCP frame exceeded the maximum size for %s:%s",
                source_ip,
                source_port,
            )
        except (ConnectionError, OSError) as exception:
            logger.warning(
                "TCP client connection failed for %s:%s: %s",
                source_ip,
                source_port,
                exception,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Unexpected TCP client error for %s:%s",
                source_ip,
                source_port,
            )
        finally:
            self.client_writers.discard(writer)
            writer.close()
            await self.wait_for_writer_close(writer)

            if current_task is not None:
                self.client_tasks.discard(current_task)

            logger.info(
                "TCP client disconnected from %s:%s",
                source_ip,
                source_port,
            )


async def start_tcp_listener(
    host: str,
    port: int,
    record_handler: RecordHandler,
) -> TCPLogServer:
    server = TCPLogServer(record_handler=record_handler)
    await server.start(host=host, port=port)
    return server