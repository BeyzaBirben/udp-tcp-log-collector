import asyncio
import math
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from log_collector.__main__ import run_collector
from log_collector.broker import (
    BrokerProvider,
    BrokerSettings,
    KafkaProducer,
    MessageProducer,
    RabbitMQProducer,
)
from log_collector.models import LogRecord
from log_collector.publishing import (
    CollectorPublishingSettings,
    LogBrokerPublisher,
    create_message_producer,
)


class CollectorPublishingSettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = CollectorPublishingSettings.from_environment({})

        self.assertFalse(settings.broker_enabled)
        self.assertTrue(settings.stdout_enabled)
        self.assertEqual(settings.worker_count, 8)
        self.assertEqual(settings.max_pending_messages, 0)
        self.assertEqual(settings.shutdown_timeout_seconds, 30.0)

    def test_loads_custom_configuration(self) -> None:
        settings = CollectorPublishingSettings.from_environment(
            {
                "COLLECTOR_BROKER_ENABLED": "true",
                "COLLECTOR_STDOUT_ENABLED": "false",
                "COLLECTOR_PUBLISH_WORKERS": "16",
                "COLLECTOR_MAX_PENDING_MESSAGES": "25000",
                "COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS": "45.5",
            }
        )

        self.assertTrue(settings.broker_enabled)
        self.assertFalse(settings.stdout_enabled)
        self.assertEqual(settings.worker_count, 16)
        self.assertEqual(settings.max_pending_messages, 25000)
        self.assertEqual(settings.shutdown_timeout_seconds, 45.5)

    def test_accepts_common_boolean_representations(self) -> None:
        for value in ("1", "TRUE", " yes ", "On"):
            with self.subTest(value=value):
                settings = CollectorPublishingSettings.from_environment(
                    {"COLLECTOR_BROKER_ENABLED": value}
                )

                self.assertTrue(settings.broker_enabled)

        for value in ("0", "FALSE", " no ", "Off"):
            with self.subTest(value=value):
                settings = CollectorPublishingSettings.from_environment(
                    {"COLLECTOR_BROKER_ENABLED": value}
                )

                self.assertFalse(settings.broker_enabled)

    def test_rejects_invalid_boolean_configuration(self) -> None:
        for variable_name in (
            "COLLECTOR_BROKER_ENABLED",
            "COLLECTOR_STDOUT_ENABLED",
        ):
            for value in ("", "maybe", "2"):
                with self.subTest(variable_name=variable_name, value=value):
                    with self.assertRaisesRegex(ValueError, variable_name):
                        CollectorPublishingSettings.from_environment(
                            {variable_name: value}
                        )

        with self.assertRaisesRegex(ValueError, "COLLECTOR_BROKER_ENABLED"):
            CollectorPublishingSettings(broker_enabled=1)

        with self.assertRaisesRegex(ValueError, "COLLECTOR_STDOUT_ENABLED"):
            CollectorPublishingSettings(stdout_enabled=0)

    def test_requires_at_least_one_output(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires broker publishing"):
            CollectorPublishingSettings(
                broker_enabled=False,
                stdout_enabled=False,
            )

    def test_rejects_invalid_worker_counts(self) -> None:
        for value in ("0", "-1", "1.5", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "COLLECTOR_PUBLISH_WORKERS"):
                    CollectorPublishingSettings.from_environment(
                        {"COLLECTOR_PUBLISH_WORKERS": value}
                    )

        with self.assertRaisesRegex(ValueError, "COLLECTOR_PUBLISH_WORKERS"):
            CollectorPublishingSettings(worker_count=True)

    def test_rejects_invalid_pending_message_limits(self) -> None:
        for value in ("-1", "1.5", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "COLLECTOR_MAX_PENDING_MESSAGES",
                ):
                    CollectorPublishingSettings.from_environment(
                        {"COLLECTOR_MAX_PENDING_MESSAGES": value}
                    )

        with self.assertRaisesRegex(ValueError, "COLLECTOR_MAX_PENDING_MESSAGES"):
            CollectorPublishingSettings(max_pending_messages=True)

    def test_rejects_invalid_shutdown_timeouts(self) -> None:
        for value in ("0", "-1", "nan", "inf", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS",
                ):
                    CollectorPublishingSettings.from_environment(
                        {"COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS": value}
                    )

        for value in (0.0, -1.0, math.nan, math.inf, True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "COLLECTOR_SHUTDOWN_TIMEOUT_SECONDS",
                ):
                    CollectorPublishingSettings(shutdown_timeout_seconds=value)


class MessageProducerFactoryTests(unittest.TestCase):
    def test_creates_rabbitmq_producer(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.RABBITMQ)

        producer = create_message_producer(settings)

        self.assertIsInstance(producer, RabbitMQProducer)

    def test_creates_kafka_producer(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.KAFKA)

        producer = create_message_producer(settings)

        self.assertIsInstance(producer, KafkaProducer)

    def test_rejects_unsupported_provider(self) -> None:
        settings = BrokerSettings(provider=BrokerProvider.KAFKA)
        object.__setattr__(settings, "provider", "unsupported")

        with self.assertRaisesRegex(ValueError, "Unsupported broker provider"):
            create_message_producer(settings)


class LogBrokerPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.producer = MagicMock(spec=MessageProducer)
        self.producer.connect = AsyncMock()
        self.producer.publish = AsyncMock()
        self.producer.close = AsyncMock()
        self.settings = CollectorPublishingSettings(
            broker_enabled=True,
            worker_count=3,
            shutdown_timeout_seconds=1.0,
        )
        self.logger_patch = patch("log_collector.publishing.logger")
        self.logger_patch.start()
        self.addCleanup(self.logger_patch.stop)

    def make_record(
        self,
        raw_log: bytes = b"publisher-test",
        protocol: str = "UDP",
        source_port: int = 55140,
    ) -> LogRecord:
        return LogRecord.capture(
            protocol=protocol,
            source_ip="127.0.0.1",
            source_port=source_port,
            raw_log=raw_log,
        )

    def make_publisher(
        self,
        settings: CollectorPublishingSettings | None = None,
    ) -> LogBrokerPublisher:
        return LogBrokerPublisher(
            producer=self.producer,
            settings=settings or self.settings,
            reconnect_delay_seconds=0.01,
        )

    async def test_rejects_disabled_publishing_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be enabled"):
            self.make_publisher(CollectorPublishingSettings())

    async def test_rejects_invalid_reconnect_delays(self) -> None:
        for value in (0.0, -1.0, math.nan, math.inf, True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "reconnect delay"):
                    LogBrokerPublisher(
                        producer=self.producer,
                        settings=self.settings,
                        reconnect_delay_seconds=value,
                    )

    async def test_starts_without_requiring_an_available_broker(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()

        self.producer.connect.assert_not_awaited()
        self.assertEqual(publisher.pending_count, 0)

        await publisher.close()

    async def test_start_is_idempotent(self) -> None:
        publisher = self.make_publisher()

        await asyncio.gather(publisher.start(), publisher.start())
        publisher.submit(self.make_record())
        await publisher.flush(timeout=1)

        self.producer.connect.assert_awaited_once()
        self.producer.publish.assert_awaited_once()

        await publisher.close()

    async def test_rejects_records_before_start(self) -> None:
        publisher = self.make_publisher()

        with self.assertRaisesRegex(RuntimeError, "not accepting records"):
            publisher.submit(self.make_record())

    async def test_preserves_raw_bytes_and_source_metadata(self) -> None:
        publisher = self.make_publisher()
        record = self.make_record(b"original-\x00\xff", "TCP", 42424)

        await publisher.start()
        message = publisher.submit(record)
        await publisher.flush(timeout=1)

        self.assertEqual(message.received_at, record.received_at)
        self.assertEqual(message.protocol, "TCP")
        self.assertEqual(message.source_ip, "127.0.0.1")
        self.assertEqual(message.source_port, 42424)
        self.assertEqual(message.raw_log, b"original-\x00\xff")
        self.producer.publish.assert_awaited_once_with(message)

        await publisher.close()

    async def test_generates_a_unique_identifier_for_each_record(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()
        first_message = publisher.submit(self.make_record(b"same"))
        second_message = publisher.submit(self.make_record(b"same"))
        await publisher.flush(timeout=1)

        self.assertNotEqual(first_message.message_id, second_message.message_id)

        await publisher.close()

    async def test_publishes_udp_and_tcp_records(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()
        publisher.submit(self.make_record(b"udp-log", "UDP"))
        publisher.submit(self.make_record(b"tcp-log", "TCP"))
        await publisher.flush(timeout=1)

        messages = [call.args[0] for call in self.producer.publish.await_args_list]

        self.assertEqual(
            {(message.protocol, message.raw_log) for message in messages},
            {("UDP", b"udp-log"), ("TCP", b"tcp-log")},
        )

        await publisher.close()

    async def test_handles_1_5_and_20_client_scenarios(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()

        for client_count in (1, 5, 20):
            with self.subTest(client_count=client_count):
                self.producer.publish.reset_mock()

                for client_id in range(client_count):
                    publisher.submit(
                        self.make_record(
                            f"client-{client_count}-{client_id}".encode(),
                            "TCP",
                            45000 + client_id,
                        )
                    )

                await publisher.flush(timeout=1)

                self.assertEqual(self.producer.publish.await_count, client_count)

        await publisher.close()

    async def test_retries_when_initial_broker_connection_fails(self) -> None:
        publisher = self.make_publisher()
        self.producer.connect.side_effect = [
            RuntimeError("broker unavailable"),
            None,
        ]

        await publisher.start()
        message = publisher.submit(self.make_record())
        await publisher.flush(timeout=1)

        self.assertEqual(self.producer.connect.await_count, 2)
        self.producer.publish.assert_awaited_once_with(message)

        await publisher.close()

    async def test_retries_failed_publish_with_the_same_message_identifier(self) -> None:
        publisher = self.make_publisher()
        self.producer.publish.side_effect = [
            RuntimeError("broker disconnected"),
            None,
        ]

        await publisher.start()
        message = publisher.submit(self.make_record(b"retry-\xff"))
        await publisher.flush(timeout=1)

        published_messages = [
            call.args[0] for call in self.producer.publish.await_args_list
        ]

        self.assertEqual(len(published_messages), 2)
        self.assertEqual(
            {current.message_id for current in published_messages},
            {message.message_id},
        )
        self.assertEqual(self.producer.connect.await_count, 2)
        self.assertEqual(self.producer.close.await_count, 1)

        await publisher.close()

    async def test_buffers_records_while_the_broker_is_unavailable(self) -> None:
        publisher = self.make_publisher()
        broker_available = asyncio.Event()

        async def connect_after_recovery() -> None:
            await broker_available.wait()

        self.producer.connect.side_effect = connect_after_recovery

        await publisher.start()

        for index in range(20):
            publisher.submit(self.make_record(f"buffered-{index}".encode()))

        await asyncio.sleep(0)

        self.assertEqual(publisher.pending_count, 20)
        self.producer.publish.assert_not_awaited()

        broker_available.set()
        await publisher.flush(timeout=1)

        self.assertEqual(self.producer.publish.await_count, 20)
        self.assertEqual(publisher.pending_count, 0)

        await publisher.close()

    async def test_uses_multiple_publish_workers(self) -> None:
        publisher = self.make_publisher()
        release_workers = asyncio.Event()
        all_workers_started = asyncio.Event()
        active_publishers = 0

        async def wait_for_parallel_workers(message: object) -> None:
            nonlocal active_publishers
            active_publishers += 1

            if active_publishers == 3:
                all_workers_started.set()

            await release_workers.wait()

        self.producer.publish.side_effect = wait_for_parallel_workers

        await publisher.start()

        for index in range(3):
            publisher.submit(self.make_record(f"parallel-{index}".encode()))

        await asyncio.wait_for(all_workers_started.wait(), timeout=1)

        self.assertEqual(active_publishers, 3)

        release_workers.set()
        await publisher.flush(timeout=1)
        await publisher.close()

    async def test_rejects_records_when_configured_queue_is_full(self) -> None:
        settings = CollectorPublishingSettings(
            broker_enabled=True,
            worker_count=1,
            max_pending_messages=1,
            shutdown_timeout_seconds=1,
        )
        publisher = self.make_publisher(settings)
        release_publish = asyncio.Event()
        publishing_started = asyncio.Event()

        async def block_publish(message: object) -> None:
            publishing_started.set()
            await release_publish.wait()

        self.producer.publish.side_effect = block_publish

        await publisher.start()
        publisher.submit(self.make_record(b"in-flight"))
        await asyncio.wait_for(publishing_started.wait(), timeout=1)
        publisher.submit(self.make_record(b"queued"))

        with self.assertRaisesRegex(RuntimeError, "queue is full"):
            publisher.submit(self.make_record(b"overflow"))

        release_publish.set()
        await publisher.flush(timeout=1)
        await publisher.close()

    async def test_unbounded_queue_accepts_a_large_burst(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()

        for index in range(500):
            publisher.submit(self.make_record(f"burst-{index}".encode()))

        await publisher.flush(timeout=2)

        self.assertEqual(self.producer.publish.await_count, 500)
        self.assertEqual(publisher.published_count, 500)

        await publisher.close()

    async def test_flush_rejects_invalid_timeouts(self) -> None:
        publisher = self.make_publisher()

        for timeout in (0.0, -1.0, math.nan, math.inf, True):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "Flush timeout"):
                    await publisher.flush(timeout=timeout)

    async def test_flush_times_out_without_dropping_pending_records(self) -> None:
        publisher = self.make_publisher()
        broker_available = asyncio.Event()
        self.producer.connect.side_effect = broker_available.wait

        await publisher.start()
        publisher.submit(self.make_record())

        with self.assertRaises(TimeoutError):
            await publisher.flush(timeout=0.01)

        self.assertEqual(publisher.pending_count, 1)

        broker_available.set()
        await publisher.flush(timeout=1)
        await publisher.close()

    async def test_close_flushes_records_before_closing_the_producer(self) -> None:
        publisher = self.make_publisher()
        events: list[str] = []

        async def publish_message(message: object) -> None:
            events.append("publish")

        async def close_producer() -> None:
            events.append("close")

        self.producer.publish.side_effect = publish_message
        self.producer.close.side_effect = close_producer

        await publisher.start()
        publisher.submit(self.make_record())
        await publisher.close()

        self.assertEqual(events, ["publish", "close"])

    async def test_close_reports_unpublished_records_after_timeout(self) -> None:
        settings = CollectorPublishingSettings(
            broker_enabled=True,
            worker_count=1,
            shutdown_timeout_seconds=0.02,
        )
        publisher = self.make_publisher(settings)
        publishing_started = asyncio.Event()

        async def never_finish(message: object) -> None:
            publishing_started.set()
            await asyncio.Event().wait()

        self.producer.publish.side_effect = never_finish

        await publisher.start()
        publisher.submit(self.make_record())
        await asyncio.wait_for(publishing_started.wait(), timeout=1)

        with self.assertRaisesRegex(RuntimeError, "1 unpublished message"):
            await publisher.close()

        self.producer.close.assert_awaited_once()

    async def test_close_is_idempotent(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()
        publisher.submit(self.make_record())
        await publisher.flush(timeout=1)
        await publisher.close()
        await publisher.close()

        self.producer.close.assert_awaited_once()

    async def test_rejects_records_after_close(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()
        await publisher.close()

        with self.assertRaisesRegex(RuntimeError, "not accepting records"):
            publisher.submit(self.make_record())

    async def test_can_restart_after_close(self) -> None:
        publisher = self.make_publisher()

        await publisher.start()
        publisher.submit(self.make_record(b"first"))
        await publisher.close()

        await publisher.start()
        publisher.submit(self.make_record(b"second"))
        await publisher.close()

        self.assertEqual(self.producer.connect.await_count, 2)
        self.assertEqual(self.producer.publish.await_count, 2)
        self.assertEqual(self.producer.close.await_count, 2)

    async def test_propagates_producer_close_failures(self) -> None:
        publisher = self.make_publisher()
        self.producer.close.side_effect = RuntimeError("close failed")

        await publisher.start()
        publisher.submit(self.make_record())
        await publisher.flush(timeout=1)

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            await publisher.close()


class CollectorApplicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.producer = MagicMock(spec=MessageProducer)
        self.producer.connect = AsyncMock()
        self.producer.publish = AsyncMock()
        self.producer.close = AsyncMock()
        self.transport = MagicMock(spec=asyncio.DatagramTransport)
        self.tcp_server = MagicMock()
        self.tcp_server.close = AsyncMock()
        self.stop_event: asyncio.Event | None = None
        self.handlers: dict[str, object] = {}

    def record(self, raw_log: bytes, protocol: str = "UDP") -> LogRecord:
        return LogRecord.capture(
            protocol=protocol,
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=raw_log,
        )

    def capture_stop_event(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event

    async def start_udp(self, **arguments: object) -> object:
        self.handlers["udp"] = arguments["record_handler"]
        return self.transport

    async def start_tcp(self, **arguments: object) -> object:
        self.handlers["tcp"] = arguments["record_handler"]

        if self.stop_event is None:
            raise RuntimeError("Collector stop event was not registered")

        self.stop_event.set()
        return self.tcp_server

    def application_patches(self) -> tuple[object, object, object]:
        return (
            patch(
                "log_collector.__main__.register_shutdown_handlers",
                side_effect=self.capture_stop_event,
            ),
            patch(
                "log_collector.__main__.start_udp_listener",
                new=AsyncMock(side_effect=self.start_udp),
            ),
            patch(
                "log_collector.__main__.start_tcp_listener",
                new=AsyncMock(side_effect=self.start_tcp),
            ),
        )

    async def test_preserves_stdout_only_behavior_by_default(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()

        async def start_tcp_with_record(**arguments: object) -> object:
            result = await self.start_tcp(**arguments)
            arguments["record_handler"](self.record(b"week-one"))
            return result

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.start_tcp_listener",
                new=AsyncMock(side_effect=start_tcp_with_record),
            ),
            patch("log_collector.__main__.write_record") as write_record,
            patch("log_collector.__main__.create_message_producer") as factory,
        ):
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=CollectorPublishingSettings(),
            )

        write_record.assert_called_once()
        factory.assert_not_called()
        self.transport.close.assert_called_once()
        self.tcp_server.close.assert_awaited_once()

    async def test_publishes_collected_records_to_the_selected_broker(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()
        settings = CollectorPublishingSettings(
            broker_enabled=True,
            worker_count=1,
        )

        async def start_tcp_with_records(**arguments: object) -> object:
            result = await self.start_tcp(**arguments)
            arguments["record_handler"](self.record(b"udp-\xff", "UDP"))
            arguments["record_handler"](self.record(b"tcp-\x00", "TCP"))
            return result

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.start_tcp_listener",
                new=AsyncMock(side_effect=start_tcp_with_records),
            ),
            patch("log_collector.__main__.write_record") as write_record,
        ):
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=settings,
                broker_settings=BrokerSettings(),
                message_producer=self.producer,
            )

        messages = [call.args[0] for call in self.producer.publish.await_args_list]

        self.assertEqual(
            {(message.protocol, message.raw_log) for message in messages},
            {("UDP", b"udp-\xff"), ("TCP", b"tcp-\x00")},
        )
        self.assertEqual(write_record.call_count, 2)
        self.producer.close.assert_awaited_once()

    async def test_can_disable_stdout_while_broker_publishing_is_enabled(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()
        settings = CollectorPublishingSettings(
            broker_enabled=True,
            stdout_enabled=False,
            worker_count=1,
        )

        async def start_tcp_with_record(**arguments: object) -> object:
            result = await self.start_tcp(**arguments)
            arguments["record_handler"](self.record(b"broker-only"))
            return result

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.start_tcp_listener",
                new=AsyncMock(side_effect=start_tcp_with_record),
            ),
            patch("log_collector.__main__.write_record") as write_record,
        ):
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=settings,
                broker_settings=BrokerSettings(),
                message_producer=self.producer,
            )

        write_record.assert_not_called()
        self.producer.publish.assert_awaited_once()

    async def test_uses_the_same_handler_for_udp_and_tcp(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()

        with shutdown_patch, udp_patch, tcp_patch:
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=CollectorPublishingSettings(),
            )

        self.assertIs(self.handlers["udp"], self.handlers["tcp"])

    async def test_closes_udp_listener_when_tcp_startup_fails(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.start_tcp_listener",
                new=AsyncMock(side_effect=RuntimeError("tcp unavailable")),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "tcp unavailable"):
                await run_collector(
                    "127.0.0.1",
                    5514,
                    5514,
                    publishing_settings=CollectorPublishingSettings(),
                )

        self.transport.close.assert_called_once()

    async def test_closes_broker_publisher_when_udp_startup_fails(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()
        settings = CollectorPublishingSettings(broker_enabled=True)

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.start_udp_listener",
                new=AsyncMock(side_effect=RuntimeError("udp unavailable")),
            ),
            patch("log_collector.__main__.LogBrokerPublisher") as publisher_class,
        ):
            publisher = publisher_class.return_value
            publisher.start = AsyncMock()
            publisher.close = AsyncMock()

            with self.assertRaisesRegex(RuntimeError, "udp unavailable"):
                await run_collector(
                    "127.0.0.1",
                    5514,
                    5514,
                    publishing_settings=settings,
                    broker_settings=BrokerSettings(),
                    message_producer=self.producer,
                )

        publisher.start.assert_awaited_once()
        publisher.close.assert_awaited_once()

    async def test_creates_configured_producer_when_no_instance_is_injected(self) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()
        settings = CollectorPublishingSettings(broker_enabled=True)
        broker_settings = BrokerSettings(provider=BrokerProvider.KAFKA)

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.create_message_producer",
                return_value=self.producer,
            ) as factory,
        ):
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=settings,
                broker_settings=broker_settings,
            )

        factory.assert_called_once_with(broker_settings)

    async def test_does_not_load_broker_configuration_when_publishing_is_disabled(
        self,
    ) -> None:
        shutdown_patch, udp_patch, tcp_patch = self.application_patches()

        with (
            shutdown_patch,
            udp_patch,
            tcp_patch,
            patch(
                "log_collector.__main__.BrokerSettings.from_environment"
            ) as load_broker_settings,
        ):
            await run_collector(
                "127.0.0.1",
                5514,
                5514,
                publishing_settings=CollectorPublishingSettings(),
            )

        load_broker_settings.assert_not_called()