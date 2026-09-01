import unittest

from pydantic import ValidationError

from log_collector.api.rule_models import RuleCreateRequest, RuleUpdateRequest


def valid_rule_payload() -> dict[str, object]:
    return {
        "name": "SSH Login Failed",
        "enabled": True,
        "sourceIp": "192.168.1.15",
        "conditions": [
            {
                "field": "rawLog",
                "operator": "contains",
                "value": "login failed",
            }
        ],
        "actions": ["Alarm", "Email"],
        "emailTo": ["soc@example.com"],
    }


class RuleRequestValidationTests(unittest.TestCase):
    def test_accepts_documented_rule_and_normalizes_values(self) -> None:
        payload = valid_rule_payload()
        payload["name"] = "  SSH Login Failed  "
        payload["sourceIp"] = "2001:0db8:0:0:0:0:0:1"
        payload["emailTo"] = [" soc@example.com "]

        rule = RuleCreateRequest.model_validate(payload)

        self.assertEqual(rule.name, "SSH Login Failed")
        self.assertEqual(rule.sourceIp, "2001:db8::1")
        self.assertEqual(rule.emailTo, ["soc@example.com"])
        self.assertEqual(rule.conditions[0].field, "rawLog")
        self.assertEqual(rule.conditions[0].operator, "contains")

    def test_update_uses_the_same_contract_as_create(self) -> None:
        rule = RuleUpdateRequest.model_validate(valid_rule_payload())

        self.assertEqual(rule.name, "SSH Login Failed")
        self.assertTrue(rule.enabled)

    def test_accepts_every_documented_field_and_operator(self) -> None:
        fields = ("rawLog", "sourceIp")
        operators = ("contains", "equals", "startsWith", "regex")

        for field in fields:
            for operator in operators:
                with self.subTest(field=field, operator=operator):
                    payload = valid_rule_payload()
                    payload["conditions"] = [
                        {
                            "field": field,
                            "operator": operator,
                            "value": "^login" if operator == "regex" else "login",
                        }
                    ]

                    rule = RuleCreateRequest.model_validate(payload)

                    self.assertEqual(rule.conditions[0].field, field)
                    self.assertEqual(rule.conditions[0].operator, operator)

    def test_rejects_empty_name(self) -> None:
        payload = valid_rule_payload()
        payload["name"] = "  "

        with self.assertRaisesRegex(ValidationError, "rule name must not be empty"):
            RuleCreateRequest.model_validate(payload)

    def test_rejects_empty_condition_list(self) -> None:
        payload = valid_rule_payload()
        payload["conditions"] = []

        with self.assertRaises(ValidationError):
            RuleCreateRequest.model_validate(payload)

    def test_rejects_empty_condition_value(self) -> None:
        payload = valid_rule_payload()
        payload["conditions"] = [
            {"field": "rawLog", "operator": "contains", "value": "  "}
        ]

        with self.assertRaisesRegex(ValidationError, "condition value"):
            RuleCreateRequest.model_validate(payload)

    def test_rejects_invalid_regular_expression(self) -> None:
        payload = valid_rule_payload()
        payload["conditions"] = [
            {"field": "rawLog", "operator": "regex", "value": "[invalid"}
        ]

        with self.assertRaisesRegex(ValidationError, "invalid regular expression"):
            RuleCreateRequest.model_validate(payload)

    def test_rejects_invalid_source_ip(self) -> None:
        payload = valid_rule_payload()
        payload["sourceIp"] = "server.example.com"

        with self.assertRaisesRegex(ValidationError, "valid IPv4 or IPv6"):
            RuleCreateRequest.model_validate(payload)

    def test_requires_recipient_for_email_action(self) -> None:
        payload = valid_rule_payload()
        payload["emailTo"] = []

        with self.assertRaisesRegex(ValidationError, "emailTo is required"):
            RuleCreateRequest.model_validate(payload)

    def test_rejects_invalid_or_duplicate_recipients(self) -> None:
        for recipients in (
            ["not-an-email"],
            ["soc@example.com", "soc@example.com"],
        ):
            with self.subTest(recipients=recipients):
                payload = valid_rule_payload()
                payload["emailTo"] = recipients

                with self.assertRaises(ValidationError):
                    RuleCreateRequest.model_validate(payload)

    def test_rejects_unknown_fields_operators_actions_and_properties(self) -> None:
        examples = (
            ("conditions", [{"field": "host", "operator": "equals", "value": "a"}]),
            (
                "conditions",
                [{"field": "rawLog", "operator": "endsWith", "value": "a"}],
            ),
            ("actions", ["Webhook"]),
            ("unexpected", True),
        )

        for key, value in examples:
            with self.subTest(key=key, value=value):
                payload = valid_rule_payload()
                payload[key] = value

                with self.assertRaises(ValidationError):
                    RuleCreateRequest.model_validate(payload)
