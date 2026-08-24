import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class BrokerProvider(StrEnum):
    RABBITMQ = "rabbitmq"
    KAFKA = "kafka"


@dataclass(frozen=True, slots=True)
class BrokerSettings:
    provider: BrokerProvider = BrokerProvider.RABBITMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_queue: str = "raw-logs"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "raw-logs"
    kafka_consumer_group: str = "log-consumers"
    reconnect_delay_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not isinstance(self.provider, BrokerProvider):
            raise ValueError("Broker provider must be rabbitmq or kafka")

        required_values = {
            "RABBITMQ_URL": self.rabbitmq_url,
            "RABBITMQ_QUEUE": self.rabbitmq_queue,
            "KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
            "KAFKA_TOPIC": self.kafka_topic,
            "KAFKA_CONSUMER_GROUP": self.kafka_consumer_group,
        }

        for variable_name, value in required_values.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{variable_name} must not be empty")

        if not math.isfinite(self.reconnect_delay_seconds):
            raise ValueError("BROKER_RECONNECT_DELAY_SECONDS must be positive")

        if self.reconnect_delay_seconds <= 0:
            raise ValueError("BROKER_RECONNECT_DELAY_SECONDS must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "BrokerSettings":
        environment = os.environ if values is None else values
        provider_value = environment.get("BROKER_PROVIDER", "rabbitmq")

        try:
            provider = BrokerProvider(provider_value.strip().lower())
        except ValueError as error:
            raise ValueError(
                "BROKER_PROVIDER must be rabbitmq or kafka"
            ) from error

        delay_value = environment.get("BROKER_RECONNECT_DELAY_SECONDS", "1.0")

        try:
            reconnect_delay_seconds = float(delay_value)
        except ValueError as error:
            raise ValueError(
                "BROKER_RECONNECT_DELAY_SECONDS must be a positive number"
            ) from error

        return cls(
            provider=provider,
            rabbitmq_url=environment.get(
                "RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"
            ).strip(),
            rabbitmq_queue=environment.get("RABBITMQ_QUEUE", "raw-logs").strip(),
            kafka_bootstrap_servers=environment.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ).strip(),
            kafka_topic=environment.get("KAFKA_TOPIC", "raw-logs").strip(),
            kafka_consumer_group=environment.get(
                "KAFKA_CONSUMER_GROUP", "log-consumers"
            ).strip(),
            reconnect_delay_seconds=reconnect_delay_seconds,
        )