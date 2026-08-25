import asyncio
import logging
import math
from collections.abc import Sequence

from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractRobustChannel,
    AbstractRobustConnection,
    AbstractRobustQueue,
)
from pamqp.commands import Basic

from log_collector.broker.config import BrokerProvider, BrokerSettings
from log_collector.broker.interfaces import BrokerDelivery
from log_collector.broker.message import BrokerMessage


logger = logging.getLogger(__name__)


class RabbitMQProducer:
    def __init__(
        self,
        settings: BrokerSettings,
        publish_timeout_seconds: float = 10.0,
    ) -> None:
        if settings.provider is not BrokerProvider.RABBITMQ:
            raise ValueError("RabbitMQ producer requires the rabbitmq provider")

        if not math.isfinite(publish_timeout_seconds) or publish_timeout_seconds <= 0:
            raise ValueError("Publish timeout must be a positive number")

        self._settings = settings
        self._publish_timeout_seconds = publish_timeout_seconds
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._is_connected():
                return

            connection = await connect_robust(
                self._settings.rabbitmq_url,
                reconnect_interval=self._settings.reconnect_delay_seconds,
                client_properties={"connection_name": "log-collector-producer"},
            )

            try:
                channel = await connection.channel(
                    publisher_confirms=True,
                    on_return_raises=True,
                )
                await channel.declare_queue(
                    self._settings.rabbitmq_queue,
                    durable=True,
                    robust=True,
                )
            except Exception:
                await connection.close()
                raise

            self._connection = connection
            self._channel = channel

            logger.info(
                "RabbitMQ producer connected to queue %s",
                self._settings.rabbitmq_queue,
            )

    async def publish(self, message: BrokerMessage) -> None:
        if not self._is_connected() or self._channel is None:
            raise RuntimeError("RabbitMQ producer is not connected")

        outgoing_message = Message(
            body=message.to_json_bytes(),
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
            content_encoding="utf-8",
            message_id=message.message_id,
            timestamp=message.received_at,
            headers={
                "schemaVersion": message.schema_version,
                "protocol": message.protocol,
            },
        )

        confirmation = await self._channel.default_exchange.publish(
            outgoing_message,
            routing_key=self._settings.rabbitmq_queue,
            mandatory=True,
            timeout=self._publish_timeout_seconds,
        )

        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError(
                f"RabbitMQ did not acknowledge message {message.message_id}"
            )

    async def close(self) -> None:
        async with self._connection_lock:
            connection = self._connection
            self._connection = None
            self._channel = None

            if connection is None or connection.is_closed:
                return

            await connection.close()

            logger.info("RabbitMQ producer disconnected")

    def _is_connected(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
        )


class RabbitMQConsumer:
    def __init__(
        self,
        settings: BrokerSettings,
        prefetch_count: int = 500,
    ) -> None:
        if settings.provider is not BrokerProvider.RABBITMQ:
            raise ValueError("RabbitMQ consumer requires the rabbitmq provider")

        if type(prefetch_count) is not int or prefetch_count <= 0:
            raise ValueError("Prefetch count must be a positive integer")

        self._settings = settings
        self._prefetch_count = prefetch_count
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._queue: AbstractRobustQueue | None = None
        self._consumer_tag: str | None = None
        self._pending: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue(
            maxsize=prefetch_count
        )
        self._connection_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._is_connected():
                return

            connection = await connect_robust(
                self._settings.rabbitmq_url,
                reconnect_interval=self._settings.reconnect_delay_seconds,
                client_properties={"connection_name": "log-collector-consumer"},
            )

            try:
                channel = await connection.channel(publisher_confirms=False)
                await channel.set_qos(prefetch_count=self._prefetch_count)

                queue = await channel.declare_queue(
                    self._settings.rabbitmq_queue,
                    durable=True,
                    robust=True,
                )

                consumer_tag = await queue.consume(
                    self._on_message,
                    no_ack=False,
                    robust=True,
                )
            except Exception:
                await connection.close()
                raise

            self._connection = connection
            self._channel = channel
            self._queue = queue
            self._consumer_tag = consumer_tag

            logger.info(
                "RabbitMQ consumer connected to queue %s with prefetch=%s",
                self._settings.rabbitmq_queue,
                self._prefetch_count,
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

        if not self._is_connected():
            raise RuntimeError("RabbitMQ consumer is not connected")

        if self._pending.empty():
            if timeout == 0:
                return []

            try:
                first_message = await asyncio.wait_for(
                    self._pending.get(),
                    timeout=timeout,
                )
            except TimeoutError:
                return []
        else:
            first_message = self._pending.get_nowait()

        incoming_messages = [first_message]

        while len(incoming_messages) < limit and not self._pending.empty():
            incoming_messages.append(self._pending.get_nowait())

        deliveries: list[BrokerDelivery] = []

        try:
            for incoming_message in incoming_messages:
                deliveries.append(
                    BrokerDelivery(
                        message=BrokerMessage.from_json_bytes(incoming_message.body),
                        receipt=incoming_message,
                    )
                )
        except ValueError:
            for incoming_message in incoming_messages:
                await incoming_message.reject(requeue=True)

            raise

        return deliveries

    async def acknowledge(self, deliveries: Sequence[BrokerDelivery]) -> None:
        if not self._is_connected():
            raise RuntimeError("RabbitMQ consumer is not connected")

        for delivery in deliveries:
            incoming_message = self._incoming_message(delivery)
            await incoming_message.ack()

    async def reject(
        self,
        deliveries: Sequence[BrokerDelivery],
        requeue: bool = True,
    ) -> None:
        if not self._is_connected():
            raise RuntimeError("RabbitMQ consumer is not connected")

        for delivery in deliveries:
            incoming_message = self._incoming_message(delivery)
            await incoming_message.reject(requeue=requeue)

    async def close(self) -> None:
        async with self._connection_lock:
            connection = self._connection
            channel = self._channel
            queue = self._queue
            consumer_tag = self._consumer_tag

            self._connection = None
            self._channel = None
            self._queue = None
            self._consumer_tag = None

            if connection is None:
                return

            try:
                if (
                    queue is not None
                    and consumer_tag is not None
                    and channel is not None
                    and not channel.is_closed
                ):
                    await queue.cancel(consumer_tag)
            finally:
                if not connection.is_closed:
                    await connection.close()

                while not self._pending.empty():
                    self._pending.get_nowait()

            logger.info("RabbitMQ consumer disconnected")

    async def _on_message(self, incoming_message: AbstractIncomingMessage) -> None:
        await self._pending.put(incoming_message)

    def _is_connected(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._consumer_tag is not None
        )

    @staticmethod
    def _incoming_message(delivery: BrokerDelivery) -> AbstractIncomingMessage:
        if not isinstance(delivery.receipt, AbstractIncomingMessage):
            raise TypeError("RabbitMQ delivery receipt must be an incoming message")

        return delivery.receipt