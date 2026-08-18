import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class ClientResult:
    client_id: int
    source_ip: str
    source_port: int
    complete_messages_sent: int
    disconnected: bool


def positive_integer(value: str) -> int:
    number = int(value)

    if number <= 0:
        raise argparse.ArgumentTypeError(
            "Value must be greater than zero"
        )

    return number


def non_negative_integer(value: str) -> int:
    number = int(value)

    if number < 0:
        raise argparse.ArgumentTypeError(
            "Value cannot be negative"
        )

    return number


def non_negative_float(value: str) -> float:
    number = float(value)

    if number < 0:
        raise argparse.ArgumentTypeError(
            "Value cannot be negative"
        )

    return number


def positive_float(value: str) -> float:
    number = float(value)

    if number <= 0:
        raise argparse.ArgumentTypeError(
            "Value must be greater than zero"
        )

    return number


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5514)
    parser.add_argument("--clients", type=positive_integer, default=1)
    parser.add_argument(
        "--messages-per-client",
        type=positive_integer,
        default=1,
    )
    parser.add_argument(
        "--delay",
        type=non_negative_float,
        default=0,
    )
    parser.add_argument(
        "--timeout",
        type=positive_float,
        default=5,
    )
    parser.add_argument(
        "--disconnect-client",
        type=positive_integer,
    )
    parser.add_argument(
        "--disconnect-after",
        type=non_negative_integer,
        default=0,
    )
    parser.add_argument(
        "--message",
        default=(
            "CEF:0|LoadGenerator|TCP|1.0|100|"
            "TCP load test|1|"
        ),
    )
    arguments = parser.parse_args()

    if (
        arguments.disconnect_client is not None
        and arguments.disconnect_client > arguments.clients
    ):
        parser.error(
            "--disconnect-client cannot exceed --clients"
        )

    if (
        arguments.disconnect_after
        > arguments.messages_per_client
    ):
        parser.error(
            "--disconnect-after cannot exceed "
            "--messages-per-client"
        )

    if "\n" in arguments.message or "\r" in arguments.message:
        parser.error("--message must contain a single-line log")

    return arguments


def build_payload(
    message: str,
    client_id: int,
    message_id: str,
) -> bytes:
    separator = "" if message.endswith((" ", "|")) else " "
    return (
        f"{message}{separator}"
        f"clientId=tcp-{client_id:03d} "
        f"messageId={message_id}"
    ).encode("utf-8")


async def close_writer(
    writer: asyncio.StreamWriter,
) -> None:
    writer.close()

    try:
        await writer.wait_closed()
    except (ConnectionError, OSError):
        pass


async def abort_with_partial_frame(
    writer: asyncio.StreamWriter,
    payload: bytes,
) -> None:
    writer.write(payload)
    await writer.drain()
    writer.transport.abort()


async def run_client(
    arguments: argparse.Namespace,
    client_id: int,
    barrier: asyncio.Barrier,
) -> ClientResult:
    _, writer = await asyncio.wait_for(
        asyncio.open_connection(
            arguments.host,
            arguments.port,
        ),
        timeout=arguments.timeout,
    )
    source_address = writer.get_extra_info("sockname")

    if source_address is None:
        await close_writer(writer)
        raise RuntimeError("Client source address is unavailable")

    source_ip = str(source_address[0])
    source_port = int(source_address[1])
    messages_sent = 0
    disconnected = False
    should_disconnect = (
        arguments.disconnect_client == client_id
    )

    try:
        await barrier.wait()

        for message_number in range(
            1,
            arguments.messages_per_client + 1,
        ):
            if (
                should_disconnect
                and messages_sent
                == arguments.disconnect_after
            ):
                partial_payload = build_payload(
                    arguments.message,
                    client_id,
                    "partial",
                )
                await abort_with_partial_frame(
                    writer,
                    partial_payload,
                )
                disconnected = True
                return ClientResult(
                    client_id=client_id,
                    source_ip=source_ip,
                    source_port=source_port,
                    complete_messages_sent=messages_sent,
                    disconnected=True,
                )

            payload = build_payload(
                arguments.message,
                client_id,
                f"{message_number:04d}",
            )
            writer.write(payload + b"\n")
            await writer.drain()
            messages_sent += 1

            if arguments.delay > 0:
                await asyncio.sleep(arguments.delay)

        if should_disconnect:
            partial_payload = build_payload(
                arguments.message,
                client_id,
                "partial",
            )
            await abort_with_partial_frame(
                writer,
                partial_payload,
            )
            disconnected = True

        return ClientResult(
            client_id=client_id,
            source_ip=source_ip,
            source_port=source_port,
            complete_messages_sent=messages_sent,
            disconnected=disconnected,
        )
    finally:
        if not disconnected:
            await close_writer(writer)


async def run_load_test(
    arguments: argparse.Namespace,
) -> list[ClientResult]:
    barrier = asyncio.Barrier(arguments.clients)
    tasks = [
        asyncio.create_task(
            run_client(
                arguments=arguments,
                client_id=client_id,
                barrier=barrier,
            )
        )
        for client_id in range(1, arguments.clients + 1)
    ]

    try:
        return list(await asyncio.gather(*tasks))
    finally:
        unfinished_tasks = [
            task for task in tasks if not task.done()
        ]

        for task in unfinished_tasks:
            task.cancel()

        if unfinished_tasks:
            await asyncio.gather(
                *unfinished_tasks,
                return_exceptions=True,
            )


def main() -> None:
    arguments = parse_arguments()

    try:
        results = asyncio.run(run_load_test(arguments))
    except Exception as exception:
        error = {
            "status": "failed",
            "error": (
                f"{type(exception).__name__}: "
                f"{exception}"
            ),
        }
        print(
            json.dumps(error, ensure_ascii=False, indent=2),
            file=sys.stderr,
        )
        raise SystemExit(1) from exception

    summary = {
        "status": "completed",
        "host": arguments.host,
        "port": arguments.port,
        "clients": arguments.clients,
        "messages_per_client": (
            arguments.messages_per_client
        ),
        "complete_messages_sent": sum(
            result.complete_messages_sent
            for result in results
        ),
        "disconnected_clients": [
            result.client_id
            for result in results
            if result.disconnected
        ],
        "client_results": [
            asdict(result)
            for result in sorted(
                results,
                key=lambda result: result.client_id,
            )
        ],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()