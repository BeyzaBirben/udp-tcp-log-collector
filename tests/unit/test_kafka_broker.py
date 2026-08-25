import asyncio
import math
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition

from log_collector.broker import (
    BrokerDelivery,
    BrokerMessage,
    BrokerProvider,
    BrokerSettings,
    KafkaConsumer,
    KafkaProducer,
    KafkaReceipt,
    MessageConsumer,
    MessageProducer,
)
from log_collector.models import LogRecord


class KafkaBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.settings = BrokerSettings.from_environment(
            {
                "BROKER_PROVIDER": "kafka",
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
                "KAFKA_TOPIC": "raw-logs",
                "KAFKA_CONSUMER_GROUP": "log-consumers",
                "BROKER_RECONNECT_DELAY_SECONDS": "2.5",
            }
        )

        self.metadata = MagicMock()
        self.metadata.topic = "raw-logs"

        self.producer_client = MagicMock(spec=AIOKafkaProducer)
        self.producer_client.start = AsyncMock()
        self.producer_client.stop = AsyncMock()
        self.producer_client.send_and_wait = AsyncMock(return_value=self.metadata)

        self.consumer_client = MagicMock(spec=AIOKafkaConsumer)
        self.consumer_client.start = AsyncMock()
        self.consumer_client.stop = AsyncMock()
        self.consumer_client.getmany = AsyncMock(return_value={})
        self.consumer_client.commit = AsyncMock()
        self.consumer_client.seek = MagicMock()

        self.producer_factory = MagicMock(return_value=self.producer_client)
        self.consumer_factory = MagicMock(return_value=self.consumer_client)

        producer_patcher = patch(
            "log_collector.broker.kafka.AIOKafkaProducer",
            new=self.producer_factory,
        )
        consumer_patcher = patch(
            "log_collector.broker.kafka.AIOKafkaConsumer",
            new=self.consumer_factory,
        )

        producer_patcher.start()
        consumer_patcher.start()
        self.addCleanup(producer_patcher.stop)
        self.addCleanup(consumer_patcher.stop)

    def make_message(self, raw_log: bytes = b"kafka-test") -> BrokerMessage:
        record = LogRecord.capture(
            protocol="TCP",
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=raw_log,
        )

        return BrokerMessage.from_record(record)

    def make_record(
        self,
        message: BrokerMessage,
        partition: int = 0,
        offset: int = 0,
        topic: str = "raw-logs",
    ) -> MagicMock:
        record = MagicMock(spec=ConsumerRecord)
        record.topic = topic
        record.partition = partition
        record.offset = offset
        record.value = message.to_json_bytes()

        return record

    def make_delivery(
        self,
        partition: int = 0,
        offset: int = 0,
        topic: str = "raw-logs",
    ) -> BrokerDelivery:
        return BrokerDelivery(
            message=self.make_message(),
            receipt=KafkaReceipt(TopicPartition(topic, partition), offset),
        )

    async def test_producer_implements_message_producer_contract(self) -> None:
        producer = KafkaProducer(self.settings)

        self.assertIsInstance(producer, MessageProducer)

    async def test_producer_uses_confirmed_idempotent_delivery(self) -> None:
        producer = KafkaProducer(self.settings, linger_ms=10)

        await producer.connect()

        self.producer_factory.assert_called_once_with(
            bootstrap_servers="kafka:9092",
            client_id="log-collector-producer",
            acks="all",
            enable_idempotence=True,
            linger_ms=10,
            retry_backoff_ms=2500,
        )
        self.producer_client.start.assert_awaited_once()

    async def test_producer_connect_is_idempotent(self) -> None:
        producer = KafkaProducer(self.settings)

        await asyncio.gather(producer.connect(), producer.connect())

        self.producer_factory.assert_called_once()
        self.producer_client.start.assert_awaited_once()

    async def test_producer_publishes_lossless_confirmed_messages(self) -> None:
        producer = KafkaProducer(self.settings)
        broker_message = self.make_message(b"raw-\x00\xff")

        await producer.connect()
        await producer.publish(broker_message)

        self.producer_client.send_and_wait.assert_awaited_once_with(
            "raw-logs",
            value=broker_message.to_json_bytes(),
            key=broker_message.message_id.encode("ascii"),
            timestamp_ms=int(broker_message.received_at.timestamp() * 1000),
            headers=[
                ("schemaVersion", b"1"),
                ("protocol", b"TCP"),
            ],
        )

    async def test_producer_rejects_missing_broker_confirmation(self) -> None:
        producer = KafkaProducer(self.settings)
        self.producer_client.send_and_wait.return_value = None

        await producer.connect()

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await producer.publish(self.make_message())

    async def test_producer_rejects_confirmation_for_another_topic(self) -> None:
        producer = KafkaProducer(self.settings)
        self.metadata.topic = "another-topic"

        await producer.connect()

        with self.assertRaisesRegex(RuntimeError, "did not acknowledge"):
            await producer.publish(self.make_message())

    async def test_producer_propagates_publish_failures(self) -> None:
        producer = KafkaProducer(self.settings)
        self.producer_client.send_and_wait.side_effect = RuntimeError(
            "broker unavailable"
        )

        await producer.connect()

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await producer.publish(self.make_message())

    async def test_producer_requires_an_active_connection(self) -> None:
        producer = KafkaProducer(self.settings)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await producer.publish(self.make_message())

    async def test_producer_closes_failed_connection_setup(self) -> None:
        producer = KafkaProducer(self.settings)
        self.producer_client.start.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await producer.connect()

        self.producer_client.stop.assert_awaited_once()

    async def test_producer_close_is_idempotent(self) -> None:
        producer = KafkaProducer(self.settings)

        await producer.connect()
        await producer.close()
        await producer.close()

        self.producer_client.stop.assert_awaited_once()

    async def test_producer_can_reconnect_after_close(self) -> None:
        producer = KafkaProducer(self.settings)

        await producer.connect()
        await producer.close()
        await producer.connect()

        self.assertEqual(self.producer_factory.call_count, 2)
        self.assertEqual(self.producer_client.start.await_count, 2)

    async def test_consumer_implements_message_consumer_contract(self) -> None:
        consumer = KafkaConsumer(self.settings)

        self.assertIsInstance(consumer, MessageConsumer)

    async def test_consumer_uses_manual_commit_and_consumer_group(self) -> None:
        consumer = KafkaConsumer(self.settings, max_poll_records=250)

        await consumer.connect()

        self.consumer_factory.assert_called_once_with(
            "raw-logs",
            bootstrap_servers="kafka:9092",
            client_id="log-collector-consumer",
            group_id="log-consumers",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            isolation_level="read_committed",
            max_poll_records=250,
            retry_backoff_ms=2500,
        )
        self.consumer_client.start.assert_awaited_once()

    async def test_consumer_connect_is_idempotent(self) -> None:
        consumer = KafkaConsumer(self.settings)

        await asyncio.gather(consumer.connect(), consumer.connect())

        self.consumer_factory.assert_called_once()
        self.consumer_client.start.assert_awaited_once()

    async def test_consumer_receives_lossless_batches_across_partitions(self) -> None:
        consumer = KafkaConsumer(self.settings)
        first_message = self.make_message(b"first-\x00\xff")
        second_message = self.make_message(b"second-\xfe")
        first_partition = TopicPartition("raw-logs", 0)
        second_partition = TopicPartition("raw-logs", 1)
        first_record = self.make_record(first_message, partition=0, offset=7)
        second_record = self.make_record(second_message, partition=1, offset=12)
        self.consumer_client.getmany.return_value = {
            first_partition: [first_record],
            second_partition: [second_record],
        }

        await consumer.connect()
        deliveries = await consumer.receive_batch(limit=10, timeout=1.5)

        self.assertEqual(len(deliveries), 2)
        self.assertEqual(deliveries[0].message.raw_log, b"first-\x00\xff")
        self.assertEqual(deliveries[1].message.raw_log, b"second-\xfe")
        self.assertEqual(deliveries[0].receipt, KafkaReceipt(first_partition, 7))
        self.assertEqual(deliveries[1].receipt, KafkaReceipt(second_partition, 12))
        self.consumer_client.getmany.assert_awaited_once_with(
            timeout_ms=1500,
            max_records=10,
        )
        self.consumer_client.commit.assert_not_awaited()

    async def test_consumer_converts_short_timeouts_without_rounding_to_zero(
        self,
    ) -> None:
        consumer = KafkaConsumer(self.settings)

        await consumer.connect()
        await consumer.receive_batch(limit=3, timeout=0.0001)

        self.consumer_client.getmany.assert_awaited_once_with(
            timeout_ms=1,
            max_records=3,
        )

    async def test_consumer_returns_an_empty_batch_without_committing(self) -> None:
        consumer = KafkaConsumer(self.settings)

        await consumer.connect()
        deliveries = await consumer.receive_batch(limit=5, timeout=0)
        await consumer.acknowledge(deliveries)

        self.assertEqual(deliveries, [])
        self.consumer_client.commit.assert_not_awaited()

    async def test_consumer_commits_the_next_offset_for_each_partition(self) -> None:
        consumer = KafkaConsumer(self.settings)
        first_partition = TopicPartition("raw-logs", 0)
        second_partition = TopicPartition("raw-logs", 1)
        deliveries = [
            self.make_delivery(partition=0, offset=8),
            self.make_delivery(partition=1, offset=3),
            self.make_delivery(partition=0, offset=10),
        ]

        await consumer.connect()
        await consumer.acknowledge(deliveries)

        self.consumer_client.commit.assert_awaited_once_with(
            {
                first_partition: 11,
                second_partition: 4,
            }
        )

    async def test_consumer_propagates_offset_commit_failures(self) -> None:
        consumer = KafkaConsumer(self.settings)
        self.consumer_client.commit.side_effect = RuntimeError("commit failed")

        await consumer.connect()

        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            await consumer.acknowledge([self.make_delivery()])

    async def test_consumer_requeues_by_rewinding_each_partition(self) -> None:
        consumer = KafkaConsumer(self.settings)
        first_partition = TopicPartition("raw-logs", 0)
        second_partition = TopicPartition("raw-logs", 1)
        deliveries = [
            self.make_delivery(partition=0, offset=9),
            self.make_delivery(partition=1, offset=4),
            self.make_delivery(partition=0, offset=7),
        ]

        await consumer.connect()
        await consumer.reject(deliveries)

        self.consumer_client.seek.assert_has_calls(
            [
                call(first_partition, 7),
                call(second_partition, 4),
            ],
            any_order=True,
        )
        self.consumer_client.commit.assert_not_awaited()

    async def test_consumer_can_reject_without_requeue(self) -> None:
        consumer = KafkaConsumer(self.settings)
        partition = TopicPartition("raw-logs", 0)

        await consumer.connect()
        await consumer.reject([self.make_delivery(offset=5)], requeue=False)

        self.consumer_client.commit.assert_awaited_once_with({partition: 6})
        self.consumer_client.seek.assert_not_called()

    async def test_consumer_rewinds_all_partitions_for_invalid_payloads(
        self,
    ) -> None:
        consumer = KafkaConsumer(self.settings)
        first_partition = TopicPartition("raw-logs", 0)
        second_partition = TopicPartition("raw-logs", 1)
        first_record = self.make_record(self.make_message(), partition=0, offset=8)
        second_record = self.make_record(self.make_message(), partition=1, offset=3)
        second_record.value = b"invalid-json"
        self.consumer_client.getmany.return_value = {
            first_partition: [first_record],
            second_partition: [second_record],
        }

        await consumer.connect()

        with self.assertRaisesRegex(ValueError, "valid JSON"):
            await consumer.receive_batch(limit=10, timeout=1.0)

        self.consumer_client.seek.assert_has_calls(
            [
                call(first_partition, 8),
                call(second_partition, 3),
            ],
            any_order=True,
        )
        self.consumer_client.commit.assert_not_awaited()

    async def test_consumer_rejects_records_with_mismatched_topics(self) -> None:
        consumer = KafkaConsumer(self.settings)
        partition = TopicPartition("raw-logs", 0)
        record = self.make_record(self.make_message(), topic="another-topic")
        self.consumer_client.getmany.return_value = {partition: [record]}

        await consumer.connect()

        with self.assertRaisesRegex(ValueError, "topic does not match"):
            await consumer.receive_batch(limit=1, timeout=1.0)

        self.consumer_client.seek.assert_called_once_with(partition, 0)

    async def test_consumer_rejects_records_with_mismatched_partitions(self) -> None:
        consumer = KafkaConsumer(self.settings)
        partition = TopicPartition("raw-logs", 0)
        record = self.make_record(self.make_message(), partition=1)
        self.consumer_client.getmany.return_value = {partition: [record]}

        await consumer.connect()

        with self.assertRaisesRegex(ValueError, "partition does not match"):
            await consumer.receive_batch(limit=1, timeout=1.0)

        self.consumer_client.seek.assert_called_once_with(partition, 0)

    async def test_consumer_rejects_records_from_unexpected_topics(self) -> None:
        consumer = KafkaConsumer(self.settings)
        partition = TopicPartition("another-topic", 0)
        record = self.make_record(self.make_message(), topic="another-topic")
        self.consumer_client.getmany.return_value = {partition: [record]}

        await consumer.connect()

        with self.assertRaisesRegex(ValueError, "unexpected topic"):
            await consumer.receive_batch(limit=1, timeout=1.0)

        self.consumer_client.seek.assert_called_once_with(partition, 0)

    async def test_consumer_rejects_foreign_delivery_receipts(self) -> None:
        consumer = KafkaConsumer(self.settings)
        delivery = BrokerDelivery(message=self.make_message(), receipt=object())

        await consumer.connect()

        with self.assertRaisesRegex(TypeError, "does not belong"):
            await consumer.acknowledge([delivery])

        with self.assertRaisesRegex(TypeError, "does not belong"):
            await consumer.reject([delivery])

    async def test_consumer_rejects_receipts_from_unexpected_topics(self) -> None:
        consumer = KafkaConsumer(self.settings)
        delivery = self.make_delivery(topic="another-topic")

        await consumer.connect()

        with self.assertRaisesRegex(ValueError, "unexpected Kafka topic"):
            await consumer.acknowledge([delivery])

    async def test_consumer_requires_an_active_connection(self) -> None:
        consumer = KafkaConsumer(self.settings)
        delivery = self.make_delivery()

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await consumer.receive_batch(limit=1, timeout=1.0)

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await consumer.acknowledge([delivery])

        with self.assertRaisesRegex(RuntimeError, "not connected"):
            await consumer.reject([delivery])

    async def test_consumer_validates_batch_arguments(self) -> None:
        consumer = KafkaConsumer(self.settings)

        await consumer.connect()

        for limit in (0, -1, True):
            with self.subTest(limit=limit):
                with self.assertRaisesRegex(ValueError, "Batch limit"):
                    await consumer.receive_batch(limit=limit, timeout=1.0)

        for timeout in (-1.0, math.inf, -math.inf, math.nan):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "Batch timeout"):
                    await consumer.receive_batch(limit=1, timeout=timeout)

    async def test_consumer_closes_failed_connection_setup(self) -> None:
        consumer = KafkaConsumer(self.settings)
        self.consumer_client.start.side_effect = RuntimeError("broker unavailable")

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await consumer.connect()

        self.consumer_client.stop.assert_awaited_once()

    async def test_consumer_close_is_idempotent(self) -> None:
        consumer = KafkaConsumer(self.settings)

        await consumer.connect()
        await consumer.close()
        await consumer.close()

        self.consumer_client.stop.assert_awaited_once()

    async def test_rejects_unsupported_provider_and_invalid_options(self) -> None:
        rabbitmq_settings = BrokerSettings(provider=BrokerProvider.RABBITMQ)

        with self.assertRaisesRegex(ValueError, "kafka provider"):
            KafkaProducer(rabbitmq_settings)

        with self.assertRaisesRegex(ValueError, "kafka provider"):
            KafkaConsumer(rabbitmq_settings)

        for linger_ms in (-1, True):
            with self.subTest(linger_ms=linger_ms):
                with self.assertRaisesRegex(ValueError, "Linger interval"):
                    KafkaProducer(self.settings, linger_ms=linger_ms)

        for max_poll_records in (0, -1, True):
            with self.subTest(max_poll_records=max_poll_records):
                with self.assertRaisesRegex(ValueError, "Maximum poll records"):
                    KafkaConsumer(self.settings, max_poll_records=max_poll_records)

    async def test_receipt_validates_partition_and_offset(self) -> None:
        partition = TopicPartition("raw-logs", 0)

        with self.assertRaisesRegex(ValueError, "topic partition"):
            KafkaReceipt("raw-logs", 0)

        for offset in (-1, True):
            with self.subTest(offset=offset):
                with self.assertRaisesRegex(ValueError, "non-negative integer"):
                    KafkaReceipt(partition, offset)