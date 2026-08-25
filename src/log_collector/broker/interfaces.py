from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from log_collector.broker.message import BrokerMessage


@dataclass(frozen=True, slots=True)
class BrokerDelivery:
    message: BrokerMessage
    receipt: object


@runtime_checkable
class MessageProducer(Protocol):
    async def connect(self) -> None: ...

    async def publish(self, message: BrokerMessage) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class MessageConsumer(Protocol):
    async def connect(self) -> None: ...

    async def receive_batch(
        self,
        limit: int,
        timeout: float,
    ) -> list[BrokerDelivery]: ...

    async def acknowledge(self, deliveries: Sequence[BrokerDelivery]) -> None: ...

    async def reject(
        self,
        deliveries: Sequence[BrokerDelivery],
        requeue: bool = True,
    ) -> None: ...

    async def close(self) -> None: ...