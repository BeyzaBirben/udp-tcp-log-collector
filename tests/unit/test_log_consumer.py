import asyncio
import math
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from log_collector.api.config import AlarmStoreSettings, RuleStoreSettings
from log_collector.broker import (
    BrokerDelivery,
    BrokerMessage,
    BrokerProvider,
    BrokerSettings,
    KafkaConsumer,
    MessageConsumer,
    RabbitMQConsumer,
)
from log_collector.consumer import (
    ConsumerSettings,
    LogConsumerService,
    create_correlation_processor,
    create_message_consumer,
)
from log_collector.correlation import (
    CorrelationProcessor,
    CorrelationSettings,
    StreamCorrelationEngine,
)
from log_collector.email_delivery import SMTPSettings
from log_collector.models import LogRecord
from log_collector.storage import (
    MongoDBSettings,
    MongoLogRepository,
    PersistenceResult,
)


class ConsumerSettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = ConsumerSettings.from_environment({})

        self.assertEqual(settings.batch_size, 500)
        self.assertEqual(settings.batch_timeout_seconds, 1.0)
        self.assertEqual(settings.retry_delay_seconds, 1.0)

    def test_loads_custom_configuration(self) -> None:
        settings = ConsumerSettings.from_environment(
            {
                "CONSUMER_BATCH_SIZE": "1000",
                "CONSUMER_BATCH_TIMEOUT_SECONDS": "0.25",
                "CONSUMER_RETRY_DELAY_SECONDS": "2.5",
            }
        )

        self.assertEqual(settings.batch_size, 1000)
        self.assertEqual(settings.batch_timeout_seconds, 0.25)
        self.assertEqual(settings.retry_delay_seconds, 2.5)

    def test_rejects_invalid_batch_sizes(self) -> None:
        for value in ("0", "-1", "1.5", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "CONSUMER_BATCH_SIZE"):
                    ConsumerSettings.from_environment({"CONSUMER_BATCH_SIZE": value})

        with self.assertRaisesRegex(ValueError, "CONSUMER_BATCH_SIZE"):
            ConsumerSettings(batch_size=True)

    def test_rejects_invalid_timeout_and_retry_values(self) -> None:
        for variable_name in (
            "CONSUMER_BATCH_TIMEOUT_SECONDS",
            "CONSUMER_RETRY_DELAY_SECONDS",
        ):
            for value in ("0", "-1", "nan", "inf", "invalid"):
                with self.subTest(variable_name=variable_name, value=value):
                    with self.assertRaisesRegex(ValueError, variable_name):
                        ConsumerSettings.from_environment({variable_name: value})

        for value in (0.0, -1.0, math.nan, math.inf):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "CONSUMER_BATCH_TIMEOUT"):
                    ConsumerSettings(batch_timeout_seconds=value)

                with self.assertRaisesRegex(ValueError, "CONSUMER_RETRY_DELAY"):
                    ConsumerSettings(retry_delay_seconds=value)


class MessageConsumerFactoryTests(unittest.TestCase):
    def test_creates_rabbitmq_consumer(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.RABBITMQ)

        consumer = create_message_consumer(settings)

        self.assertIsInstance(consumer, RabbitMQConsumer)

    def test_creates_kafka_consumer(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.KAFKA)

        consumer = create_message_consumer(settings)

        self.assertIsInstance(consumer, KafkaConsumer)

    def test_rejects_unsupported_provider(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.KAFKA)
        object.__setattr__(settings, "provider", "unsupported")

        with self.assertRaisesRegex(ValueError, "Unsupported broker provider"):
            create_message_consumer(settings)


class CorrelationProcessorFactoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mongodb_settings = MongoDBSettings()
        self.rule_store_settings = RuleStoreSettings()
        self.alarm_store_settings = AlarmStoreSettings()
        self.smtp_settings = SMTPSettings()

    def test_creates_stream_engine_when_enabled(self) -> None:
        processor = create_correlation_processor(
            CorrelationSettings(enabled=True),
            self.mongodb_settings,
            self.rule_store_settings,
        )

        self.assertIsInstance(processor, StreamCorrelationEngine)

    def test_creates_alarm_handler_with_configured_collection(self) -> None:
        alarm_repository = MagicMock()
        alarm_handler = MagicMock()
        email_sender = MagicMock()
        email_dispatcher = MagicMock()

        with (
            patch(
                "log_collector.consumer.MongoAlarmRepository",
                return_value=alarm_repository,
            ) as repository_factory,
            patch(
                "log_collector.consumer.AlarmCorrelationMatchHandler",
                return_value=alarm_handler,
            ) as handler_factory,
            patch(
                "log_collector.consumer.SMTPEmailSender",
                return_value=email_sender,
            ) as sender_factory,
            patch(
                "log_collector.consumer.AsyncEmailDispatcher",
                return_value=email_dispatcher,
            ) as dispatcher_factory,
        ):
            processor = create_correlation_processor(
                CorrelationSettings(enabled=True),
                self.mongodb_settings,
                self.rule_store_settings,
                self.alarm_store_settings,
                self.smtp_settings,
            )

        self.assertIsInstance(processor, StreamCorrelationEngine)
        repository_factory.assert_called_once_with(
            self.mongodb_settings,
            self.alarm_store_settings,
        )
        sender_factory.assert_called_once_with(self.smtp_settings)
        dispatcher_factory.assert_called_once_with(
            sender=email_sender,
            status_writer=alarm_repository,
        )
        handler_factory.assert_called_once_with(
            alarm_repository,
            email_dispatcher,
        )

    def test_disables_correlation_without_creating_provider(self) -> None:
        with patch(
            "log_collector.consumer.MongoAlarmRepository"
        ) as repository_factory:
            processor = create_correlation_processor(
                CorrelationSettings(enabled=False),
                self.mongodb_settings,
                self.rule_store_settings,
            )

        self.assertIsNone(processor)
        repository_factory.assert_not_called()


class LogConsumerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.settings = ConsumerSettings(
            batch_size=10,
            batch_timeout_seconds=0.5,
            retry_delay_seconds=0.01,
        )

        self.consumer = MagicMock(spec=MessageConsumer)
        self.consumer.connect = AsyncMock()
        self.consumer.receive_batch = AsyncMock(return_value=[])
        self.consumer.acknowledge = AsyncMock()
        self.consumer.reject = AsyncMock()
        self.consumer.close = AsyncMock()

        self.repository = MagicMock(spec=MongoLogRepository)
        self.repository.connect = AsyncMock()
        self.repository.save_batch = AsyncMock(
            return_value=PersistenceResult(1, 1, 0)
        )
        self.repository.close = AsyncMock()

        self.correlation = MagicMock(spec=CorrelationProcessor)
        self.correlation.connect = AsyncMock()
        self.correlation.process = AsyncMock(return_value=0)
        self.correlation.close = AsyncMock()

    def make_delivery(self, raw_log: bytes = b"consumer-test") -> BrokerDelivery:
        record = LogRecord.capture(
            protocol="UDP",
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=raw_log,
        )

        return BrokerDelivery(
            message=BrokerMessage.from_record(record),
            receipt=object(),
        )

    def make_service(self) -> LogConsumerService:
        return LogConsumerService(
            consumer=self.consumer,
            repository=self.repository,
            settings=self.settings,
        )

    def make_correlated_service(self) -> LogConsumerService:
        return LogConsumerService(
            consumer=self.consumer,
            repository=self.repository,
            settings=self.settings,
            correlation=self.correlation,
        )

    async def test_connects_database_before_message_broker(self) -> None:
        service = self.make_service()
        events: list[str] = []

        async def connect_repository() -> None:
            events.append("database")

        async def connect_consumer() -> None:
            events.append("broker")

        self.repository.connect.side_effect = connect_repository
        self.consumer.connect.side_effect = connect_consumer

        await service.connect()

        self.assertEqual(events, ["database", "broker"])

    async def test_connects_database_correlation_and_broker_in_order(self) -> None:
        service = self.make_correlated_service()
        events: list[str] = []

        async def connect_repository() -> None:
            events.append("database")

        async def connect_correlation() -> None:
            events.append("correlation")

        async def connect_consumer() -> None:
            events.append("broker")

        self.repository.connect.side_effect = connect_repository
        self.correlation.connect.side_effect = connect_correlation
        self.consumer.connect.side_effect = connect_consumer

        await service.connect()

        self.assertEqual(events, ["database", "correlation", "broker"])

    async def test_correlation_startup_failure_does_not_block_log_consumer(
        self,
    ) -> None:
        service = self.make_correlated_service()
        self.correlation.connect.side_effect = RuntimeError("rules unavailable")

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            await service.connect()

        self.consumer.connect.assert_awaited_once()

    async def test_closes_correlation_if_broker_connection_fails(self) -> None:
        service = self.make_correlated_service()
        self.consumer.connect.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await service.connect()

        self.correlation.close.assert_awaited_once()
        self.repository.close.assert_awaited_once()

    async def test_connect_is_idempotent(self) -> None:
        service = self.make_service()

        await asyncio.gather(service.connect(), service.connect())

        self.repository.connect.assert_awaited_once()
        self.consumer.connect.assert_awaited_once()

    async def test_does_not_connect_broker_if_database_is_unavailable(self) -> None:
        service = self.make_service()
        self.repository.connect.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await service.connect()

        self.consumer.connect.assert_not_awaited()

    async def test_closes_database_if_broker_connection_fails(self) -> None:
        service = self.make_service()
        self.consumer.connect.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await service.connect()

        self.repository.close.assert_awaited_once()

    async def test_receives_configured_batch_size_and_timeout(self) -> None:
        service = self.make_service()

        await service.connect()
        await service.process_batch()

        self.consumer.receive_batch.assert_awaited_once_with(limit=10, timeout=0.5)

    async def test_does_not_write_or_acknowledge_empty_batches(self) -> None:
        service = self.make_service()

        await service.connect()
        result = await service.process_batch()

        self.assertIsNone(result)
        self.repository.save_batch.assert_not_awaited()
        self.consumer.acknowledge.assert_not_awaited()

    async def test_acknowledges_messages_only_after_database_write(self) -> None:
        service = self.make_service()
        delivery = self.make_delivery(b"first-\x00\xff")
        self.consumer.receive_batch.return_value = [delivery]
        events: list[str] = []

        async def save_messages(messages: list[BrokerMessage]) -> PersistenceResult:
            events.append("database")
            self.assertEqual(messages[0].raw_log, b"first-\x00\xff")
            return PersistenceResult(1, 1, 0)

        async def acknowledge_messages(deliveries: list[BrokerDelivery]) -> None:
            events.append("acknowledge")
            self.assertEqual(deliveries, [delivery])

        self.repository.save_batch.side_effect = save_messages
        self.consumer.acknowledge.side_effect = acknowledge_messages

        await service.connect()
        result = await service.process_batch()

        self.assertEqual(events, ["database", "acknowledge"])
        self.assertEqual(result, PersistenceResult(1, 1, 0))

    async def test_correlates_persisted_messages_before_acknowledgement(self) -> None:
        service = self.make_correlated_service()
        delivery = self.make_delivery(b"login failed")
        self.consumer.receive_batch.return_value = [delivery]
        self.correlation.process.return_value = 1
        events: list[str] = []

        async def save_messages(messages: list[BrokerMessage]) -> PersistenceResult:
            events.append("database")
            return PersistenceResult(1, 1, 0)

        async def correlate_messages(messages: list[BrokerMessage]) -> int:
            events.append("correlation")
            self.assertEqual(messages, [delivery.message])
            return 1

        async def acknowledge_messages(deliveries: list[BrokerDelivery]) -> None:
            events.append("acknowledge")

        self.repository.save_batch.side_effect = save_messages
        self.correlation.process.side_effect = correlate_messages
        self.consumer.acknowledge.side_effect = acknowledge_messages

        await service.connect()
        result = await service.process_batch()

        self.assertEqual(events, ["database", "correlation", "acknowledge"])
        self.assertEqual(result, PersistenceResult(1, 1, 0))

    async def test_correlation_failure_does_not_prevent_acknowledgement(self) -> None:
        service = self.make_correlated_service()
        deliveries = [self.make_delivery()]
        self.consumer.receive_batch.return_value = deliveries
        self.correlation.process.side_effect = RuntimeError("rule failed")

        await service.connect()

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            result = await service.process_batch()

        self.assertEqual(result, PersistenceResult(1, 1, 0))
        self.consumer.acknowledge.assert_awaited_once_with(deliveries)

    async def test_database_failure_skips_correlation(self) -> None:
        service = self.make_correlated_service()
        deliveries = [self.make_delivery()]
        self.consumer.receive_batch.return_value = deliveries
        self.repository.save_batch.side_effect = RuntimeError("write failed")

        await service.connect()

        with self.assertRaisesRegex(RuntimeError, "write failed"):
            await service.process_batch()

        self.correlation.process.assert_not_awaited()

    async def test_writes_all_received_messages_in_one_database_batch(self) -> None:
        service = self.make_service()
        deliveries = [
            self.make_delivery(b"first"),
            self.make_delivery(b"second"),
            self.make_delivery(b"third"),
        ]
        self.consumer.receive_batch.return_value = deliveries
        self.repository.save_batch.return_value = PersistenceResult(3, 3, 0)

        await service.connect()
        result = await service.process_batch()

        self.repository.save_batch.assert_awaited_once_with(
            [delivery.message for delivery in deliveries]
        )
        self.consumer.acknowledge.assert_awaited_once_with(deliveries)
        self.assertEqual(result, PersistenceResult(3, 3, 0))

    async def test_acknowledges_already_persisted_duplicate_messages(self) -> None:
        service = self.make_service()
        deliveries = [self.make_delivery()]
        self.consumer.receive_batch.return_value = deliveries
        self.repository.save_batch.return_value = PersistenceResult(1, 0, 1)

        await service.connect()
        result = await service.process_batch()

        self.consumer.acknowledge.assert_awaited_once_with(deliveries)
        self.assertEqual(result, PersistenceResult(1, 0, 1))

    async def test_requeues_messages_when_database_write_fails(self) -> None:
        service = self.make_service()
        deliveries = [self.make_delivery()]
        self.consumer.receive_batch.return_value = deliveries
        self.repository.save_batch.side_effect = RuntimeError("write failed")

        await service.connect()

        with self.assertRaisesRegex(RuntimeError, "write failed"):
            await service.process_batch()

        self.consumer.reject.assert_awaited_once_with(deliveries, requeue=True)
        self.consumer.acknowledge.assert_not_awaited()

    async def test_preserves_database_error_when_requeue_also_fails(self) -> None:
        service = self.make_service()
        deliveries = [self.make_delivery()]
        self.consumer.receive_batch.return_value = deliveries
        self.repository.save_batch.side_effect = RuntimeError("write failed")
        self.consumer.reject.side_effect = RuntimeError("requeue failed")

        await service.connect()

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                await service.process_batch()

        self.consumer.acknowledge.assert_not_awaited()

    async def test_propagates_message_acknowledgement_failures(self) -> None:
        service = self.make_service()
        self.consumer.receive_batch.return_value = [self.make_delivery()]
        self.consumer.acknowledge.side_effect = RuntimeError("acknowledge failed")

        await service.connect()

        with self.assertRaisesRegex(RuntimeError, "acknowledge failed"):
            await service.process_batch()

        self.repository.save_batch.assert_awaited_once()

    async def test_requires_an_active_service_connection(self) -> None:
        service = self.make_service()

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await service.process_batch()

    async def test_close_is_idempotent(self) -> None:
        service = self.make_service()

        await service.connect()
        await service.close()
        await service.close()

        self.consumer.close.assert_awaited_once()
        self.repository.close.assert_awaited_once()

    async def test_close_releases_correlation_resources(self) -> None:
        service = self.make_correlated_service()

        await service.connect()
        await service.close()
        await service.close()

        self.consumer.close.assert_awaited_once()
        self.correlation.close.assert_awaited_once()
        self.repository.close.assert_awaited_once()

    async def test_correlation_close_failure_does_not_block_other_resources(
        self,
    ) -> None:
        service = self.make_correlated_service()
        self.correlation.close.side_effect = RuntimeError("close failed")

        await service.connect()

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            await service.close()

        self.consumer.close.assert_awaited_once()
        self.repository.close.assert_awaited_once()

    async def test_closes_database_even_if_broker_close_fails(self) -> None:
        service = self.make_service()
        self.consumer.close.side_effect = RuntimeError("broker close failed")

        await service.connect()

        with self.assertRaisesRegex(RuntimeError, "broker close failed"):
            await service.close()

        self.repository.close.assert_awaited_once()

    async def test_run_stops_cleanly_after_stop_request(self) -> None:
        service = self.make_service()

        async def receive_and_stop(**kwargs: object) -> list[BrokerDelivery]:
            service.request_stop()
            return []

        self.consumer.receive_batch.side_effect = receive_and_stop

        await service.run()

        self.consumer.connect.assert_awaited_once()
        self.repository.connect.assert_awaited_once()
        self.consumer.close.assert_awaited_once()
        self.repository.close.assert_awaited_once()

    async def test_run_reconnects_after_a_processing_failure(self) -> None:
        service = self.make_service()
        attempts = 0

        async def receive_or_stop(**kwargs: object) -> list[BrokerDelivery]:
            nonlocal attempts
            attempts += 1

            if attempts == 1:
                raise RuntimeError("broker unavailable")

            service.request_stop()
            return []

        self.consumer.receive_batch.side_effect = receive_or_stop

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            await service.run()

        self.assertEqual(self.consumer.connect.await_count, 2)
        self.assertEqual(self.repository.connect.await_count, 2)
        self.assertEqual(self.consumer.close.await_count, 2)
        self.assertEqual(self.repository.close.await_count, 2)

    async def test_run_retries_when_database_initially_is_unavailable(self) -> None:
        service = self.make_service()
        attempts = 0

        async def connect_repository() -> None:
            nonlocal attempts
            attempts += 1

            if attempts == 1:
                raise RuntimeError("database unavailable")

        async def receive_and_stop(**kwargs: object) -> list[BrokerDelivery]:
            service.request_stop()
            return []

        self.repository.connect.side_effect = connect_repository
        self.consumer.receive_batch.side_effect = receive_and_stop

        with self.assertLogs("log_collector.consumer", level="ERROR"):
            await service.run()

        self.assertEqual(self.repository.connect.await_count, 2)
        self.consumer.connect.assert_awaited_once()

    async def test_run_rejects_concurrent_execution(self) -> None:
        service = self.make_service()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def wait_for_release(**kwargs: object) -> list[BrokerDelivery]:
            entered.set()
            await release.wait()
            service.request_stop()
            return []

        self.consumer.receive_batch.side_effect = wait_for_release
        task = asyncio.create_task(service.run())
        await entered.wait()

        with self.assertRaisesRegex(RuntimeError, "already running"):
            await service.run()

        release.set()
        await task

    async def test_stop_before_run_does_not_open_connections(self) -> None:
        service = self.make_service()
        service.request_stop()

        await service.run()

        self.consumer.connect.assert_not_awaited()
        self.repository.connect.assert_not_awaited()

    async def test_consumer_factory_can_be_replaced_without_database_changes(
        self,
    ) -> None:
        settings = BrokerSettings(provider=BrokerProvider.KAFKA)

        with patch("log_collector.consumer.KafkaConsumer") as factory:
            factory.return_value = self.consumer
            selected_consumer = create_message_consumer(settings)

        factory.assert_called_once_with(settings)
        self.assertIs(selected_consumer, self.consumer)
