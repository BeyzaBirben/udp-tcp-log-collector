import asyncio
import logging
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass

from log_collector.broker import (
    BrokerMessage,
    BrokerProvider,
    BrokerSettings,
    KafkaProducer,
    MessageProducer,
    RabbitMQProducer,
)
from log_collector.models import LogRecord


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CollectorPublishingSettings:
    broker_enabled: bool = False
    stdout_enabled: bool = True
    worker_count: int = 8
    max_pending_messages: int = 0
    shutdown_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if type(self.broker_enabled) is not bool:
            raise ValueError("COLLECTOR_BROKER_ENABLED must be a boolean")

        if type(self.stdout_enabled) is not bool:
            raise ValueError("COLLECTOR_STDOUT_ENABLED must be a boolean")

        if not self.broker_enabled and not self.stdout_enabled:
            raise ValueError("Collector requires broker publishing or standard output")

        if type(self.worker_count) is not int or self.worker_count <= 0:
            raise ValueError("COLLECTOR_PUBLISH_WORKERS must be a positive integer")

        if type(self.max_pending_messages) is not int or self.max_pending_messages < 0:
            raise ValueError(
                "COLLECTOR_MAX_PENDING_MESSAGES must be a non-negative integer"
            )

        if (
            not isinstance(self.shutdown_timeout_seconds, (int, float))
            or isinstance(self.shutdown_timeout_seconds, bool)
            or not math.isfinite(self.shutdown_timeout_seconds)
            or self.shutdown_timeout_seconds <= 0
        ):
            raise ValueError("COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "CollectorPublishingSettings":
        environment = os.environ if values is None else values

        return cls(
            broker_enabled=_read_boolean(
                environment,
                "COLLECTOR_BROKER_ENABLED",
                False,
            ),
            stdout_enabled=_read_boolean(
                environment,
                "COLLECTOR_STDOUT_ENABLED",
                True,
            ),
            worker_count=_read_integer(
                environment,
                "COLLECTOR_PUBLISH_WORKERS",
                8,
                allow_zero=False,
            ),
            max_pending_messages=_read_integer(
                environment,
                "COLLECTOR_MAX_PENDING_MESSAGES",
                0,
                allow_zero=True,
            ),
            shutdown_timeout_seconds=_read_positive_float(
                environment,
                "COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS",
                30.0,
            ),
        )


class LogBrokerPublisher:
    def __init__(
        self,
        producer: MessageProducer,
        settings: CollectorPublishingSettings,
        reconnect_delay_seconds: float,
    ) -> None:
        if not settings.broker_enabled:
            raise ValueError("Broker publishing must be enabled")

        if (
            not isinstance(reconnect_delay_seconds, (int, float))
            or isinstance(reconnect_delay_seconds, bool)
            or not math.isfinite(reconnect_delay_seconds)
            or reconnect_delay_seconds <= 0
        ):
            raise ValueError("Broker reconnect delay must be positive")

        self._producer = producer
        self._settings = settings
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._pending: asyncio.Queue[BrokerMessage] = asyncio.Queue(
            maxsize=settings.max_pending_messages
        )
        self._workers: list[asyncio.Task[None]] = []
        self._lifecycle_lock = asyncio.Lock()
        self._producer_lock = asyncio.Lock()
        self._accepting_records = False
        self._connected = False
        self._connection_generation = 0
        self._in_flight = 0
        self._published_count = 0

    @property
    def pending_count(self) -> int:
        return self._pending.qsize() + self._in_flight

    @property
    def published_count(self) -> int:
        return self._published_count

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._workers:
                return

            self._accepting_records = True
            self._workers = [
                asyncio.create_task(
                    self._publish_loop(),
                    name=f"broker-publisher-{index + 1}",
                )
                for index in range(self._settings.worker_count)
            ]

            logger.info(
                "Broker publisher started with workers=%s max_pending=%s",
                self._settings.worker_count,
                self._settings.max_pending_messages,
            )

    def submit(self, record: LogRecord) -> BrokerMessage:
        if not self._accepting_records:
            raise RuntimeError("Broker publisher is not accepting records")

        message = BrokerMessage.from_record(record)

        try:
            self._pending.put_nowait(message)
        except asyncio.QueueFull as error:
            raise RuntimeError("Broker publishing queue is full") from error

        return message

    async def flush(self, timeout: float | None = None) -> None:
        if timeout is None:
            await self._pending.join()
            return

        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("Flush timeout must be positive")

        await asyncio.wait_for(self._pending.join(), timeout=timeout)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            self._accepting_records = False

            if not self._workers:
                return

            shutdown_error: RuntimeError | None = None

            try:
                await self.flush(timeout=self._settings.shutdown_timeout_seconds)
            except TimeoutError:
                pending_count = self.pending_count
                shutdown_error = RuntimeError(
                    "Broker publisher shutdown timed out with "
                    f"{pending_count} unpublished message(s)"
                )

                logger.error("%s", shutdown_error)

            workers = tuple(self._workers)
            self._workers.clear()

            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            try:
                await self._disconnect()
            except Exception:
                if shutdown_error is None:
                    raise

                logger.exception("Failed to close broker producer after shutdown timeout")

            logger.info(
                "Broker publisher stopped after publishing %s message(s)",
                self._published_count,
            )

            if shutdown_error is not None:
                raise shutdown_error

    async def _publish_loop(self) -> None:
        while True:
            message = await self._pending.get()
            self._in_flight += 1

            try:
                await self._publish_with_retry(message)
            finally:
                self._in_flight -= 1
                self._pending.task_done()

    async def _publish_with_retry(self, message: BrokerMessage) -> None:
        while True:
            generation: int | None = None

            try:
                generation = await self._ensure_connected()
                await self._producer.publish(message)
                self._published_count += 1
                return
            except Exception:
                logger.warning(
                    "Broker publish failed for message %s; retrying in %.2f seconds",
                    message.message_id,
                    self._reconnect_delay_seconds,
                    exc_info=True,
                )

                if generation is not None:
                    try:
                        await self._disconnect(generation)
                    except Exception:
                        logger.exception("Failed to reset broker producer connection")

                await asyncio.sleep(self._reconnect_delay_seconds)

    async def _ensure_connected(self) -> int:
        async with self._producer_lock:
            if not self._connected:
                await self._producer.connect()
                self._connected = True
                self._connection_generation += 1

            return self._connection_generation

    async def _disconnect(self, generation: int | None = None) -> None:
        async with self._producer_lock:
            if not self._connected:
                return

            if generation is not None and generation != self._connection_generation:
                return

            self._connected = False
            await self._producer.close()


def create_message_producer(settings: BrokerSettings) -> MessageProducer:
    if settings.provider is BrokerProvider.RABBITMQ:
        return RabbitMQProducer(settings)

    if settings.provider is BrokerProvider.KAFKA:
        return KafkaProducer(settings)

    raise ValueError(f"Unsupported broker provider: {settings.provider}")


def _read_boolean(
    environment: Mapping[str, str],
    variable_name: str,
    default: bool,
) -> bool:
    raw_value = environment.get(variable_name)

    if raw_value is None:
        return default

    if not isinstance(raw_value, str):
        raise ValueError(f"{variable_name} must be a boolean")

    normalized_value = raw_value.strip().lower()

    if normalized_value in {"1", "true", "yes", "on"}:
        return True

    if normalized_value in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"{variable_name} must be a boolean")


def _read_integer(
    environment: Mapping[str, str],
    variable_name: str,
    default: int,
    *,
    allow_zero: bool,
) -> int:
    raw_value = environment.get(variable_name, str(default))

    try:
        value = int(raw_value)
    except (TypeError, ValueError) as error:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{variable_name} must be a {qualifier} integer") from error

    minimum_value = 0 if allow_zero else 1

    if value < minimum_value:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{variable_name} must be a {qualifier} integer")

    return value


def _read_positive_float(
    environment: Mapping[str, str],
    variable_name: str,
    default: float,
) -> float:
    raw_value = environment.get(variable_name, str(default))

    try:
        value = float(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be a positive number") from error

    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{variable_name} must be a positive number")

    return value