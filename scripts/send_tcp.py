import argparse
import socket
import time


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5514)
    parser.add_argument("--message", required=True)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0)
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    frame = arguments.message.encode("utf-8") + b"\n"

    with socket.create_connection(
        (arguments.host, arguments.port),
        timeout=5,
    ) as client:
        for _ in range(arguments.count):
            client.sendall(frame)

            if arguments.delay > 0:
                time.sleep(arguments.delay)

    print(
        f"Sent {arguments.count} TCP message(s) "
        f"to {arguments.host}:{arguments.port}"
    )


if __name__ == "__main__":
    main()