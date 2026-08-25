import asyncio
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition

from log_collector.broker.config import BrokerProvider, BrokerSettings
from log_collector.broker.interfaces import BrokerDelivery
from log_collector.broker.message import BrokerMessage


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class KafkaReceipt:
    topic_partition: TopicPartition
    offset: int

    def __post_init__(self) -> None:
        if not isinstance(self.topic_partition, TopicPartition):
            raise ValueError("Kafka receipt requires a topic partition")

        if type(self.offset) is not int or self.offset < 0:
            raise ValueError("Kafka receipt offset must be a non-negative integer")


class KafkaProducer:
    def __init__(
        self,
        settings: BrokerSettings,
        linger_ms: int = 5,
    ) -> None:
        if settings.provider is not BrokerProvider.KAFKA:
            raise ValueError("Kafka producer requires the kafka provider")

        if type(linger_ms) is not int or linger_ms < 0:
            raise ValueError("Linger interval must be a non-negative integer")

        self._settings = settings
        self._linger_ms = linger_ms
        self._producer: AIOKafkaProducer | None = None
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._producer is not None:
                return

            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id="log-collector-producer",
                acks="all",
                enable_idempotence=True,
                linger_ms=self._linger_ms,
                retry_backoff_ms=_retry_backoff_ms(self._settings),
            )

            try:
                await producer.start()
            except Exception:
                await producer.stop()
                raise

            self._producer = producer

            logger.info(
                "Kafka producer connected to topic %s",
                self._settings.kafka_topic,
            )

    async def publish(self, message: BrokerMessage) -> None:
        producer = self._producer

        if producer is None:
            raise RuntimeError("Kafka producer is not connected")

        metadata = await producer.send_and_wait(
            self._settings.kafka_topic,
            value=message.to_json_bytes(),
            key=message.message_id.encode("ascii"),
            timestamp_ms=int(message.received_at.timestamp() * 1000),
            headers=[
                ("schemaVersion", str(message.schema_version).encode("ascii")),
                ("protocol", message.protocol.encode("ascii")),
            ],
        )

        if metadata is None or metadata.topic != self._settings.kafka_topic:
            raise RuntimeError(f"Kafka did not acknowledge message {message.message_id}")

    async def close(self) -> None:
        async with self._connection_lock:
            producer = self._producer
            self._producer = None

            if producer is None:
                return

            await producer.stop()

            logger.info("Kafka producer disconnected")


class KafkaConsumer:
    def __init__(
        self,
        settings: BrokerSettings,
        max_poll_records: int = 500,
    ) -> None:
        if settings.provider is not BrokerProvider.KAFKA:
            raise ValueError("Kafka consumer requires the kafka provider")

        if type(max_poll_records) is not int or max_poll_records <= 0:
            raise ValueError("Maximum poll records must be a positive integer")

        self._settings = settings
        self._max_poll_records = max_poll_records
        self._consumer: AIOKafkaConsumer | None = None
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._consumer is not None:
                return

            consumer = AIOKafkaConsumer(
                self._settings.kafka_topic,
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id="log-collector-consumer",
                group_id=self._settings.kafka_consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                isolation_level="read_committed",
                max_poll_records=self._max_poll_records,
                retry_backoff_ms=_retry_backoff_ms(self._settings),
            )

            try:
                await consumer.start()
            except Exception:
                await consumer.stop()
                raise

            self._consumer = consumer

            logger.info(
                "Kafka consumer connected to topic %s with group %s",
                self._settings.kafka_topic,
                self._settings.kafka_consumer_group,
            )

    async def receive_batch(
        self,
        limit: int,
        timeout: float,
    ) -> list[BrokerDelivery]:
        if type(limit) is not int or limit <= 0:
            raise ValueError("Batch limit must be a positive integer")

        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("Batch timeout must be a non-negative number")

        consumer = self._require_consumer()
        records_by_partition = await consumer.getmany(
            timeout_ms=math.ceil(timeout * 1000),
            max_records=limit,
        )

        deliveries: list[BrokerDelivery] = []

        try:
            for topic_partition, records in records_by_partition.items():
                for record in records:
                    deliveries.append(self._delivery(topic_partition, record))
        except ValueError:
            self._rewind_records(consumer, records_by_partition)
            raise

        return deliveries

    async def acknowledge(self, deliveries: Sequence[BrokerDelivery]) -> None:
        consumer = self._require_consumer()
        next_offsets = self._next_offsets(deliveries)

        if next_offsets:
            await consumer.commit(next_offsets)

    async def reject(
        self,
        deliveries: Sequence[BrokerDelivery],
        requeue: bool = True,
    ) -> None:
        consumer = self._require_consumer()

        if not requeue:
            await self.acknowledge(deliveries)
            return

        offsets: dict[TopicPartition, int] = {}

        for delivery in deliveries:
            receipt = self._receipt(delivery)
            current_offset = offsets.get(receipt.topic_partition)

            if current_offset is None or receipt.offset < current_offset:
                offsets[receipt.topic_partition] = receipt.offset

        for topic_partition, offset in offsets.items():
            consumer.seek(topic_partition, offset)

    async def close(self) -> None:
        async with self._connection_lock:
            consumer = self._consumer
            self._consumer = None

            if consumer is None:
                return

            await consumer.stop()

            logger.info("Kafka consumer disconnected")

    def _delivery(
        self,
        topic_partition: TopicPartition,
        record: ConsumerRecord,
    ) -> BrokerDelivery:
        if record.topic != topic_partition.topic:
            raise ValueError("Kafka record topic does not match its partition")

        if record.partition != topic_partition.partition:
            raise ValueError("Kafka record partition does not match its assignment")

        if topic_partition.topic != self._settings.kafka_topic:
            raise ValueError("Kafka record belongs to an unexpected topic")

        return BrokerDelivery(
            message=BrokerMessage.from_json_bytes(record.value),
            receipt=KafkaReceipt(topic_partition, record.offset),
        )

    def _next_offsets(
        self,
        deliveries: Sequence[BrokerDelivery],
    ) -> dict[TopicPartition, int]:
        next_offsets: dict[TopicPartition, int] = {}

        for delivery in deliveries:
            receipt = self._receipt(delivery)
            next_offset = receipt.offset + 1
            current_offset = next_offsets.get(receipt.topic_partition, 0)

            if next_offset > current_offset:
                next_offsets[receipt.topic_partition] = next_offset

        return next_offsets

    def _receipt(self, delivery: BrokerDelivery) -> KafkaReceipt:
        receipt = delivery.receipt

        if not isinstance(receipt, KafkaReceipt):
            raise TypeError("Delivery receipt does not belong to the Kafka consumer")

        if receipt.topic_partition.topic != self._settings.kafka_topic:
            raise ValueError("Delivery receipt belongs to an unexpected Kafka topic")

        return receipt

    def _require_consumer(self) -> AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not connected")

        return self._consumer

    @staticmethod
    def _rewind_records(
        consumer: AIOKafkaConsumer,
        records_by_partition: dict[TopicPartition, list[ConsumerRecord]],
    ) -> None:
        for topic_partition, records in records_by_partition.items():
            if records:
                consumer.seek(
                    topic_partition,
                    min(record.offset for record in records),
                )


def _retry_backoff_ms(settings: BrokerSettings) -> int:
    return max(1, math.ceil(settings.reconnect_delay_seconds * 1000))