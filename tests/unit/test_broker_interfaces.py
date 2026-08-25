import unittest
from collections import deque
from collections.abc import Sequence

from log_collector.broker import (
    BrokerDelivery,
    BrokerMessage,
    MessageConsumer,
    MessageProducer,
)
from log_collector.models import LogRecord


class InMemoryProducer:
    def __init__(self) -> None:
        self.connected = False
        self.messages: list[BrokerMessage] = []

    async def connect(self) -> None:
        self.connected = True

    async def publish(self, message: BrokerMessage) -> None:
        self.messages.append(message)

    async def close(self) -> None:
        self.connected = False


class InMemoryConsumer:
    def __init__(self, deliveries: list[BrokerDelivery]) -> None:
        self.connected = False
        self.pending = deque(deliveries)
        self.acknowledged: list[BrokerDelivery] = []
        self.rejected: list[BrokerDelivery] = []

    async def connect(self) -> None:
        self.connected = True

    async def receive_batch(
        self,
        limit: int,
        timeout: float,
    ) -> list[BrokerDelivery]:
        deliveries: list[BrokerDelivery] = []

        while self.pending and len(deliveries) < limit:
            deliveries.append(self.pending.popleft())

        return deliveries

    async def acknowledge(self, deliveries: Sequence[BrokerDelivery]) -> None:
        self.acknowledged.extend(deliveries)

    async def reject(
        self,
        deliveries: Sequence[BrokerDelivery],
        requeue: bool = True,
    ) -> None:
        self.rejected.extend(deliveries)

        if requeue:
            self.pending.extend(deliveries)

    async def close(self) -> None:
        self.connected = False


class BrokerInterfaceTests(unittest.IsolatedAsyncioTestCase):
    def make_message(self) -> BrokerMessage:
        record = LogRecord.capture(
            protocol="UDP",
            source_ip="127.0.0.1",
            source_port=55140,
            raw_log=b"interface-test",
        )

        return BrokerMessage.from_record(record)

    async def test_producer_implements_provider_independent_interface(self) -> None:
        producer = InMemoryProducer()
        message = self.make_message()

        self.assertIsInstance(producer, MessageProducer)

        await producer.connect()
        await producer.publish(message)
        await producer.close()

        self.assertEqual(producer.messages, [message])
        self.assertFalse(producer.connected)

    async def test_consumer_acknowledges_messages_after_processing(self) -> None:
        delivery = BrokerDelivery(message=self.make_message(), receipt="receipt-1")
        consumer = InMemoryConsumer([delivery])

        self.assertIsInstance(consumer, MessageConsumer)

        await consumer.connect()
        received = await consumer.receive_batch(limit=10, timeout=1.0)
        await consumer.acknowledge(received)
        await consumer.close()

        self.assertEqual(received, [delivery])
        self.assertEqual(consumer.acknowledged, [delivery])
        self.assertFalse(consumer.connected)

    async def test_consumer_can_requeue_unprocessed_messages(self) -> None:
        delivery = BrokerDelivery(message=self.make_message(), receipt="receipt-2")
        consumer = InMemoryConsumer([delivery])

        await consumer.connect()
        received = await consumer.receive_batch(limit=1, timeout=1.0)
        await consumer.reject(received, requeue=True)
        retried = await consumer.receive_batch(limit=1, timeout=1.0)

        self.assertEqual(consumer.rejected, [delivery])
        self.assertEqual(retried, [delivery])