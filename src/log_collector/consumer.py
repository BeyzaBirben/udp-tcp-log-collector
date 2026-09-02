import asyncio
import logging
import math
import os
import signal
from collections.abc import Mapping
from dataclasses import dataclass

from log_collector.api.config import RuleStoreSettings
from log_collector.broker import (
    BrokerProvider,
    BrokerSettings,
    KafkaConsumer,
    MessageConsumer,
    RabbitMQConsumer,
)
from log_collector.correlation import (
    CorrelationProcessor,
    CorrelationSettings,
    LoggingCorrelationMatchHandler,
    MongoActiveRuleProvider,
    StreamCorrelationEngine,
)
from log_collector.storage import (
    MongoDBSettings,
    MongoLogRepository,
    PersistenceResult,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    batch_size: int = 500
    batch_timeout_seconds: float = 1.0
    retry_delay_seconds: float = 1.0

    def __post_init__(self) -> None:
        if type(self.batch_size) is not int or self.batch_size <= 0:
            raise ValueError("CONSUMER_BATCH_SIZE must be a positive integer")

        if (
            not math.isfinite(self.batch_timeout_seconds)
            or self.batch_timeout_seconds <= 0
        ):
            raise ValueError("CONSUMER_BATCH_TIMEOUT_SECONDS must be positive")

        if (
            not math.isfinite(self.retry_delay_seconds)
            or self.retry_delay_seconds <= 0
        ):
            raise ValueError("CONSUMER_RETRY_DELAY_SECONDS must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "ConsumerSettings":
        environment = os.environ if values is None else values

        try:
            batch_size = int(environment.get("CONSUMER_BATCH_SIZE", "500"))
        except (TypeError, ValueError) as error:
            raise ValueError(
                "CONSUMER_BATCH_SIZE must be a positive integer"
            ) from error

        return cls(
            batch_size=batch_size,
            batch_timeout_seconds=_positive_float(
                environment,
                "CONSUMER_BATCH_TIMEOUT_SECONDS",
                1.0,
            ),
            retry_delay_seconds=_positive_float(
                environment,
                "CONSUMER_RETRY_DELAY_SECONDS",
                1.0,
            ),
        )


class LogConsumerService:
    def __init__(
        self,
        consumer: MessageConsumer,
        repository: MongoLogRepository,
        settings: ConsumerSettings,
        correlation: CorrelationProcessor | None = None,
    ) -> None:
        self._consumer = consumer
        self._repository = repository
        self._settings = settings
        self._correlation = correlation
        self._connected = False
        self._running = False
        self._stop_requested = asyncio.Event()
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._connected:
                return

            await self._repository.connect()

            if self._correlation is not None:
                try:
                    await self._correlation.connect()
                except Exception:
                    logger.exception(
                        "Correlation startup failed; log persistence will continue"
                    )

            try:
                await self._consumer.connect()
            except Exception:
                if self._correlation is not None:
                    try:
                        await self._correlation.close()
                    except Exception:
                        logger.exception("Failed to close correlation resources")

                await self._repository.close()
                raise

            self._connected = True

            logger.info(
                "Log consumer connected with batch_size=%s",
                self._settings.batch_size,
            )

    async def process_batch(self) -> PersistenceResult | None:
        if not self._connected:
            raise RuntimeError("Log consumer service is not connected")

        deliveries = await self._consumer.receive_batch(
            limit=self._settings.batch_size,
            timeout=self._settings.batch_timeout_seconds,
        )

        if not deliveries:
            return None

        try:
            messages = [delivery.message for delivery in deliveries]
            result = await self._repository.save_batch(messages)
        except Exception:
            try:
                await self._consumer.reject(deliveries, requeue=True)
            except Exception:
                logger.exception(
                    "Failed to requeue messages after MongoDB write failure"
                )

            raise

        if self._correlation is not None:
            try:
                match_count = await self._correlation.process(messages)

                if match_count:
                    logger.info(
                        "Correlation matched %s rule-message pair(s)",
                        match_count,
                    )
            except Exception:
                logger.exception(
                    "Correlation processing failed; persisted logs will be acknowledged"
                )

        await self._consumer.acknowledge(deliveries)

        logger.info(
            "Persisted batch: attempted=%s inserted=%s duplicates=%s",
            result.attempted_count,
            result.inserted_count,
            result.duplicate_count,
        )

        return result

    async def run(self) -> None:
        if self._running:
            raise RuntimeError("Log consumer service is already running")

        self._running = True

        try:
            while not self._stop_requested.is_set():
                try:
                    await self.connect()
                    await self.process_batch()
                except Exception:
                    logger.exception(
                        "Log consumer failed; retrying in %.2f seconds",
                        self._settings.retry_delay_seconds,
                    )

                    try:
                        await self.close()
                    except Exception:
                        logger.exception("Failed to close consumer resources")

                    try:
                        await asyncio.wait_for(
                            self._stop_requested.wait(),
                            timeout=self._settings.retry_delay_seconds,
                        )
                    except TimeoutError:
                        continue
        finally:
            self._running = False
            await self.close()

    def request_stop(self) -> None:
        self._stop_requested.set()

    async def close(self) -> None:
        async with self._connection_lock:
            if not self._connected:
                return

            self._connected = False

            consumer_error: Exception | None = None

            try:
                await self._consumer.close()
            except Exception as error:
                consumer_error = error

            if self._correlation is not None:
                try:
                    await self._correlation.close()
                except Exception:
                    logger.exception("Failed to close correlation resources")

            try:
                await self._repository.close()
            except Exception:
                if consumer_error is not None:
                    logger.exception(
                        "Failed to close MongoDB after broker close failure"
                    )
                    raise consumer_error

                raise

            if consumer_error is not None:
                raise consumer_error

            logger.info("Log consumer disconnected")


def create_message_consumer(settings: BrokerSettings) -> MessageConsumer:
    if settings.provider is BrokerProvider.RABBITMQ:
        return RabbitMQConsumer(settings)

    if settings.provider is BrokerProvider.KAFKA:
        return KafkaConsumer(settings)

    raise ValueError(f"Unsupported broker provider: {settings.provider}")


def create_correlation_processor(
    correlation_settings: CorrelationSettings,
    mongodb_settings: MongoDBSettings,
    rule_store_settings: RuleStoreSettings | None = None,
) -> CorrelationProcessor | None:
    if not correlation_settings.enabled:
        return None

    active_rule_store_settings = (
        RuleStoreSettings.from_environment()
        if rule_store_settings is None
        else rule_store_settings
    )

    return StreamCorrelationEngine(
        provider=MongoActiveRuleProvider(
            mongodb_settings,
            active_rule_store_settings,
        ),
        handler=LoggingCorrelationMatchHandler(),
        settings=correlation_settings,
    )


async def run_consumer() -> None:
    broker_settings = BrokerSettings.from_environment()
    mongodb_settings = MongoDBSettings.from_environment()
    consumer_settings = ConsumerSettings.from_environment()
    correlation_settings = CorrelationSettings.from_environment()
    correlation = create_correlation_processor(
        correlation_settings,
        mongodb_settings,
    )

    service = LogConsumerService(
        consumer=create_message_consumer(broker_settings),
        repository=MongoLogRepository(mongodb_settings),
        settings=consumer_settings,
        correlation=correlation,
    )

    event_loop = asyncio.get_running_loop()

    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            event_loop.add_signal_handler(shutdown_signal, service.request_stop)
        except (NotImplementedError, RuntimeError):
            continue

    await service.run()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        logger.info("Log consumer stopped")


def _positive_float(
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
