import unittest

from log_collector.broker import BrokerProvider, BrokerSettings


class BrokerSettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = BrokerSettings.from_environment({})

        self.assertEqual(settings.provider, BrokerProvider.RABBITMQ)
        self.assertEqual(settings.rabbitmq_url, "amqp://guest:guest@localhost:5672/")
        self.assertEqual(settings.rabbitmq_queue, "raw-logs")
        self.assertEqual(settings.kafka_bootstrap_servers, "localhost:9092")
        self.assertEqual(settings.kafka_topic, "raw-logs")
        self.assertEqual(settings.kafka_consumer_group, "log-consumers")
        self.assertEqual(settings.reconnect_delay_seconds, 1.0)

    def test_accepts_provider_names_case_insensitively(self) -> None:
        examples = (
            ("RabbitMQ", BrokerProvider.RABBITMQ),
            (" Kafka ", BrokerProvider.KAFKA),
        )

        for value, expected_provider in examples:
            with self.subTest(value=value):
                settings = BrokerSettings.from_environment({"BROKER_PROVIDER": value})
                self.assertEqual(settings.provider, expected_provider)

    def test_loads_custom_configuration(self) -> None:
        settings = BrokerSettings.from_environment(
            {
                "BROKER_PROVIDER": "kafka",
                "RABBITMQ_URL": "amqp://collector:secret@rabbitmq:5672/",
                "RABBITMQ_QUEUE": "incoming-logs",
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
                "KAFKA_TOPIC": "incoming-logs",
                "KAFKA_CONSUMER_GROUP": "database-writers",
                "BROKER_RECONNECT_DELAY_SECONDS": "2.5",
            }
        )

        self.assertEqual(settings.provider, BrokerProvider.KAFKA)
        self.assertEqual(settings.rabbitmq_url, "amqp://collector:secret@rabbitmq:5672/")
        self.assertEqual(settings.rabbitmq_queue, "incoming-logs")
        self.assertEqual(settings.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(settings.kafka_topic, "incoming-logs")
        self.assertEqual(settings.kafka_consumer_group, "database-writers")
        self.assertEqual(settings.reconnect_delay_seconds, 2.5)

    def test_rejects_unsupported_provider(self) -> None:
        with self.assertRaisesRegex(ValueError, "BROKER_PROVIDER"):
            BrokerSettings.from_environment({"BROKER_PROVIDER": "redis"})

    def test_rejects_invalid_reconnect_delay(self) -> None:
        for value in ("0", "-1", "nan", "inf", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError, "BROKER_RECONNECT_DELAY_SECONDS"
                ):
                    BrokerSettings.from_environment(
                        {"BROKER_RECONNECT_DELAY_SECONDS": value}
                    )

    def test_rejects_empty_configuration_values(self) -> None:
        for variable_name in (
            "RABBITMQ_URL",
            "RABBITMQ_QUEUE",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC",
            "KAFKA_CONSUMER_GROUP",
        ):
            with self.subTest(variable_name=variable_name):
                with self.assertRaisesRegex(ValueError, variable_name):
                    BrokerSettings.from_environment({variable_name: "  "})