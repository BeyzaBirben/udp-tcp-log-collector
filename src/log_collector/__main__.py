import argparse
import asyncio
import logging
import signal

from .models import LogRecord
from .tcp_server import start_tcp_listener
from .udp_server import start_udp_listener


def valid_port(value: str) -> int:
    port = int(value)

    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("Port must be between 1 and 65535")

    return port


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="log-collector")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--udp-port", type=valid_port, default=5514)
    parser.add_argument("--tcp-port", type=valid_port, default=5514)
    return parser.parse_args()


def write_record(record: LogRecord) -> None:
    print(record.to_json(), flush=True)


def register_shutdown_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        stop_event.set()

    for current_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(current_signal, request_shutdown)
        except NotImplementedError:
            signal.signal(
                current_signal,
                lambda *_: loop.call_soon_threadsafe(request_shutdown),
            )


async def run_collector(
    host: str,
    udp_port: int,
    tcp_port: int,
) -> None:
    stop_event = asyncio.Event()
    register_shutdown_handlers(stop_event)

    udp_transport = await start_udp_listener(
        host=host,
        port=udp_port,
        record_handler=write_record,
    )

    try:
        tcp_server = await start_tcp_listener(
            host=host,
            port=tcp_port,
            record_handler=write_record,
        )

        try:
            await stop_event.wait()
        finally:
            await tcp_server.close()
    finally:
        udp_transport.close()
        await asyncio.sleep(0)


def main() -> None:
    arguments = parse_arguments()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(
        run_collector(
            host=arguments.host,
            udp_port=arguments.udp_port,
            tcp_port=arguments.tcp_port,
        )
    )


if __name__ == "__main__":
    main()