import unittest

from log_collector.api.config import (
    APISettings,
    AlarmStoreSettings,
    RuleStoreSettings,
)


class APISettingsTests(unittest.TestCase):
    def test_loads_default_configuration(self) -> None:
        settings = APISettings.from_environment({})

        self.assertEqual(settings.host, "0.0.0.0")
        self.assertEqual(settings.port, 8000)
        self.assertEqual(settings.default_page_size, 50)
        self.assertEqual(settings.max_page_size, 200)

    def test_loads_custom_configuration(self) -> None:
        settings = APISettings.from_environment(
            {
                "API_HOST": " 127.0.0.1 ",
                "API_PORT": "8080",
                "API_DEFAULT_PAGE_SIZE": "25",
                "API_MAX_PAGE_SIZE": "100",
            }
        )

        self.assertEqual(settings.host, "127.0.0.1")
        self.assertEqual(settings.port, 8080)
        self.assertEqual(settings.default_page_size, 25)
        self.assertEqual(settings.max_page_size, 100)

    def test_rejects_empty_host(self) -> None:
        with self.assertRaisesRegex(ValueError, "API_HOST"):
            APISettings.from_environment({"API_HOST": "  "})

    def test_rejects_invalid_port(self) -> None:
        for value in ("0", "65536", "invalid"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "API_PORT"):
                    APISettings.from_environment({"API_PORT": value})

    def test_rejects_invalid_page_sizes(self) -> None:
        for variable_name in (
            "API_DEFAULT_PAGE_SIZE",
            "API_MAX_PAGE_SIZE",
        ):
            for value in ("0", "-1", "invalid"):
                with self.subTest(variable_name=variable_name, value=value):
                    with self.assertRaisesRegex(ValueError, variable_name):
                        APISettings.from_environment({variable_name: value})

    def test_rejects_default_page_size_above_maximum(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            APISettings(default_page_size=201, max_page_size=200)


class RuleStoreSettingsTests(unittest.TestCase):
    def test_loads_default_collection(self) -> None:
        settings = RuleStoreSettings.from_environment({})

        self.assertEqual(settings.collection, "correlation_rules")

    def test_loads_custom_collection(self) -> None:
        settings = RuleStoreSettings.from_environment(
            {"MONGODB_RULES_COLLECTION": " security_rules "}
        )

        self.assertEqual(settings.collection, "security_rules")

    def test_rejects_empty_collection(self) -> None:
        with self.assertRaisesRegex(ValueError, "MONGODB_RULES_COLLECTION"):
            RuleStoreSettings.from_environment({"MONGODB_RULES_COLLECTION": "  "})


class AlarmStoreSettingsTests(unittest.TestCase):
    def test_loads_default_collection(self) -> None:
        settings = AlarmStoreSettings.from_environment({})

        self.assertEqual(settings.collection, "alarms")

    def test_loads_custom_collection(self) -> None:
        settings = AlarmStoreSettings.from_environment(
            {"MONGODB_ALARMS_COLLECTION": " security_alarms "}
        )

        self.assertEqual(settings.collection, "security_alarms")

    def test_rejects_empty_collection(self) -> None:
        with self.assertRaisesRegex(ValueError, "MONGODB_ALARMS_COLLECTION"):
            AlarmStoreSettings.from_environment(
                {"MONGODB_ALARMS_COLLECTION": "  "}
            )
