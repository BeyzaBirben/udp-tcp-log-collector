import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aio_pika import DeliveryMode
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustQueue,
)
from pamqp.commands import Basic

from log_collector.broker import (
    BrokerDelivery,
    BrokerMessage,
    BrokerProvider,
    BrokerSettings,
    MessageConsumer,
    MessageProducer,
    RabbitMQConsumer,
    RabbitMQProducer,
)
from log_collector.models import LogRecord


class RabbitMQBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.settings = BrokerSettings.from_environment(
            {
                "BROKER_PROVIDER": "rabbitmq",
                "RABBITMQ_URL": "amqp://guest:guest@rabbitmq:5672/",
                "RABBITMQ_QUEUE": "raw-logs",
                "BROKER_RECONNECT_DELAY_SECONDS": "2.5",
            }
        )

        self.exchange = MagicMock()
        self.exchange.publish = AsyncMock(return_value=Basic.Ack())

        self.queue = MagicMock(spec=AbstractRobustQueue)
        self.queue.consume = AsyncMock(return_value="consumer-1")
        self.queue.cancel = AsyncMock()

        self.channel = MagicMock(spec=AbstractRobustChannel)
        self.channel.is_closed = False
        self.channel.default_exchange = self.exchange
        self.channel.declare_queue = AsyncMock(return_value=self.queue)
        self.channel.set_qos = AsyncMock()

        self.connection = MagicMock(spec=AbstractRobustConnection)
        self.connection.is_closed = False
        self.connection.channel = AsyncMock(return_value=self.channel)
        self.connection.close = AsyncMock()

        self.connect_mock = AsyncMock(return_value=self.connection)
        patcher = patch(
            "log_collector.broker.rabbitmq.connect_robust",
            new=self.connect_mock,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_message(self, raw_log: bytes = b"rabbitmq-test") -> BrokerMessage:
        record = LogRecord.capture(
            protocol="UDP",
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=raw_log,
        )

        return BrokerMessage.from_record(record)

    def make_incoming(self, message: BrokerMessage) -> MagicMock:
        incoming = MagicMock(spec=AbstractIncomingMessage)
        incoming.body = message.to_json_bytes()
        incoming.ack = AsyncMock()
        incoming.reject = AsyncMock()

        return incoming

    async def deliver(self, incoming: MagicMock) -> None:
        callback = self.queue.consume.await_args.args[0]
        await callback(incoming)

    async def test_producer_implements_message_producer_contract(self) -> None:
        producer = RabbitMQProducer(self.settings)

        self.assertIsInstance(producer, MessageProducer)

    async def test_producer_uses_robust_connection_and_durable_queue(self) -> None:
        producer = RabbitMQProducer(self.settings)

        await producer.connect()

        self.connect_mock.assert_awaited_once_with(
            "amqp://guest:guest@rabbitmq:5672/",
            reconnect_interval=2.5,
            client_properties={"connection_name": "log-collector-producer"},
        )
        self.connection.channel.assert_awaited_once_with(
            publisher_confirms=True,
            on_return_raises=True,
        )
        self.channel.declare_queue.assert_awaited_once_with(
            "raw-logs",
            durable=True,
            robust=True,
        )

    async def test_producer_connect_is_idempotent(self) -> None:
        producer = RabbitMQProducer(self.settings)

        await asyncio.gather(producer.connect(), producer.connect())

        self.connect_mock.assert_awaited_once()

    async def test_producer_publishes_persistent_confirmed_messages(self) -> None:
        producer = RabbitMQProducer(self.settings)
        broker_message = self.make_message(b"raw-\x00\xff")

        await producer.connect()
        await producer.publish(broker_message)

        publish_call = self.exchange.publish.await_args
        outgoing_message = publish_call.args[0]

        self.assertEqual(outgoing_message.body, broker_message.to_json_bytes())
        self.assertEqual(outgoing_message.delivery_mode, DeliveryMode.PERSISTENT)
        self.assertEqual(outgoing_message.content_type, "application/json")
        self.assertEqual(outgoing_message.content_encoding, "utf-8")
        self.assertEqual(outgoing_message.message_id, broker_message.message_id)
        self.assertEqual(
            outgoing_message.headers,
            {"schemaVersion": 1, "protocol": "UDP"},
        )
        self.assertEqual(publish_call.kwargs["routing_key"], "raw-logs")
        self.assertTrue(publish_call.kwargs["mandatory"])
        self.assertEqual(publish_call.kwargs["timeout"], 10.0)

    async def test_producer_rejects_missing_broker_confirmation(self) -> None:
        producer = RabbitMQProducer(self.settings)
        self.exchange.publish.return_value = Basic.Nack()

        await producer.connect()

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await producer.publish(self.make_message())

    async def test_producer_requires_an_active_connection(self) -> None:
        producer = RabbitMQProducer(self.settings)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await producer.publish(self.make_message())

    async def test_producer_closes_failed_connection_setup(self) -> None:
        producer = RabbitMQProducer(self.settings)
        self.channel.declare_queue.side_effect = RuntimeError("queue unavailable")

        with self.assertRaisesRegex(RuntimeError, "queue unavailable"):
            await producer.connect()

        self.connection.close.assert_awaited_once()

    async def test_producer_close_is_idempotent(self) -> None:
        producer = RabbitMQProducer(self.settings)

        await producer.connect()
        await producer.close()
        await producer.close()

        self.connection.close.assert_awaited_once()

    async def test_consumer_implements_message_consumer_contract(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        self.assertIsInstance(consumer, MessageConsumer)

    async def test_consumer_uses_manual_acknowledgement_and_prefetch(self) -> None:
        consumer = RabbitMQConsumer(self.settings, prefetch_count=250)

        await consumer.connect()

        self.connect_mock.assert_awaited_once_with(
            "amqp://guest:guest@rabbitmq:5672/",
            reconnect_interval=2.5,
            client_properties={"connection_name": "log-collector-consumer"},
        )
        self.connection.channel.assert_awaited_once_with(publisher_confirms=False)
        self.channel.set_qos.assert_awaited_once_with(prefetch_count=250)
        self.channel.declare_queue.assert_awaited_once_with(
            "raw-logs",
            durable=True,
            robust=True,
        )
        self.assertFalse(self.queue.consume.await_args.kwargs["no_ack"])
        self.assertTrue(self.queue.consume.await_args.kwargs["robust"])

    async def test_consumer_connect_is_idempotent(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        await asyncio.gather(consumer.connect(), consumer.connect())

        self.connect_mock.assert_awaited_once()

    async def test_consumer_receives_batches_without_data_loss(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        first_message = self.make_message(b"first-\x00\xff")
        second_message = self.make_message(b"second-\xfe")
        first_incoming = self.make_incoming(first_message)
        second_incoming = self.make_incoming(second_message)

        await consumer.connect()
        await self.deliver(first_incoming)
        await self.deliver(second_incoming)

        deliveries = await consumer.receive_batch(limit=10, timeout=1.0)

        self.assertEqual(len(deliveries), 2)
        self.assertEqual(deliveries[0].message.raw_log, b"first-\x00\xff")
        self.assertEqual(deliveries[1].message.raw_log, b"second-\xfe")
        self.assertIs(deliveries[0].receipt, first_incoming)
        self.assertIs(deliveries[1].receipt, second_incoming)
        first_incoming.ack.assert_not_awaited()
        second_incoming.ack.assert_not_awaited()

    async def test_consumer_respects_the_requested_batch_limit(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        first_incoming = self.make_incoming(self.make_message(b"first"))
        second_incoming = self.make_incoming(self.make_message(b"second"))

        await consumer.connect()
        await self.deliver(first_incoming)
        await self.deliver(second_incoming)

        first_batch = await consumer.receive_batch(limit=1, timeout=1.0)
        second_batch = await consumer.receive_batch(limit=1, timeout=1.0)

        self.assertEqual(first_batch[0].message.raw_log, b"first")
        self.assertEqual(second_batch[0].message.raw_log, b"second")

    async def test_consumer_returns_empty_batch_after_timeout(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        await consumer.connect()

        self.assertEqual(await consumer.receive_batch(limit=5, timeout=0), [])
        self.assertEqual(await consumer.receive_batch(limit=5, timeout=0.01), [])

    async def test_consumer_acknowledges_only_after_processing(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        incoming = self.make_incoming(self.make_message())

        await consumer.connect()
        await self.deliver(incoming)
        deliveries = await consumer.receive_batch(limit=1, timeout=1.0)

        incoming.ack.assert_not_awaited()

        await consumer.acknowledge(deliveries)

        incoming.ack.assert_awaited_once()

    async def test_consumer_requeues_rejected_messages_by_default(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        incoming = self.make_incoming(self.make_message())

        await consumer.connect()
        await self.deliver(incoming)
        deliveries = await consumer.receive_batch(limit=1, timeout=1.0)
        await consumer.reject(deliveries)

        incoming.reject.assert_awaited_once_with(requeue=True)

    async def test_consumer_can_reject_without_requeue(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        incoming = self.make_incoming(self.make_message())

        await consumer.connect()
        await self.deliver(incoming)
        deliveries = await consumer.receive_batch(limit=1, timeout=1.0)
        await consumer.reject(deliveries, requeue=False)

        incoming.reject.assert_awaited_once_with(requeue=False)

    async def test_consumer_requeues_invalid_message_payloads(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        incoming = self.make_incoming(self.make_message())
        incoming.body = b"invalid-json"

        await consumer.connect()
        await self.deliver(incoming)

        with self.assertRaisesRegex(ValueError, "valid JSON"):
            await consumer.receive_batch(limit=1, timeout=1.0)

        incoming.reject.assert_awaited_once_with(requeue=True)

    async def test_consumer_rejects_foreign_delivery_receipts(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        delivery = BrokerDelivery(message=self.make_message(), receipt=object())

        await consumer.connect()

        with self.assertRaisesRegex(TypeError, "incoming message"):
            await consumer.acknowledge([delivery])

    async def test_consumer_requires_an_active_connection(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await consumer.receive_batch(limit=1, timeout=1.0)

    async def test_consumer_validates_batch_arguments(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        await consumer.connect()

        for limit in (0, -1, True):
            with self.subTest(limit=limit):
                with self.assertRaisesRegex(ValueError, "Batch limit"):
                    await consumer.receive_batch(limit=limit, timeout=1.0)

        for timeout in (-1.0, float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "Batch timeout"):
                    await consumer.receive_batch(limit=1, timeout=timeout)

    async def test_consumer_closes_failed_connection_setup(self) -> None:
        consumer = RabbitMQConsumer(self.settings)
        self.queue.consume.side_effect = RuntimeError("consumer unavailable")

        with self.assertRaisesRegex(RuntimeError, "consumer unavailable"):
            await consumer.connect()

        self.connection.close.assert_awaited_once()

    async def test_consumer_stops_consuming_before_connection_close(self) -> None:
        consumer = RabbitMQConsumer(self.settings)

        await consumer.connect()
        await consumer.close()
        await consumer.close()

        self.queue.cancel.assert_awaited_once_with("consumer-1")
        self.connection.close.assert_awaited_once()

    async def test_rejects_unsupported_provider_and_invalid_options(self) -> None:
        kafka_settings = BrokerSettings(provider=BrokerProvider.KAFKA)

        with self.assertRaisesRegex(ValueError, "rabbitmq provider"):
            RabbitMQProducer(kafka_settings)

        with self.assertRaisesRegex(ValueError, "rabbitmq provider"):
            RabbitMQConsumer(kafka_settings)

        for timeout in (0, -1, float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "Publish timeout"):
                    RabbitMQProducer(self.settings, publish_timeout_seconds=timeout)

        for prefetch_count in (0, -1, True):
            with self.subTest(prefetch_count=prefetch_count):
                with self.assertRaisesRegex(ValueError, "Prefetch count"):
                    RabbitMQConsumer(self.settings, prefetch_count=prefetch_count)