import base64
import json
import unittest
from datetime import datetime, timedelta, timezone
from uuid import UUID

from log_collector.broker import BrokerMessage
from log_collector.models import LogRecord


class BrokerMessageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw_log = b"CEF:0|Vendor|Product|1.0|message=\x00\xff\xfe"
        self.record = LogRecord(
            received_at=datetime(
                2026,
                8,
                24,
                12,
                30,
                45,
                123456,
                tzinfo=timezone(timedelta(hours=3)),
            ),
            protocol="TCP",
            source_ip="192.168.1.15",
            source_port=55140,
            raw_log=self.raw_log,
        )

    def test_creates_documented_broker_message(self) -> None:
        message = BrokerMessage.from_record(
            self.record,
            message_id="12345678-1234-4234-8234-123456789abc",
        )
        payload = message.as_dict()

        self.assertEqual(payload["schemaVersion"], 1)
        self.assertEqual(payload["messageId"], "12345678-1234-4234-8234-123456789abc")
        self.assertEqual(payload["receivedAt"], "2026-08-24T09:30:45.123456Z")
        self.assertEqual(payload["protocol"], "TCP")
        self.assertEqual(payload["sourceIp"], "192.168.1.15")
        self.assertEqual(payload["sourcePort"], 55140)
        self.assertEqual(payload["rawLog"], self.raw_log.decode("utf-8", errors="replace"))
        self.assertEqual(
            payload["rawLogBase64"],
            base64.b64encode(self.raw_log).decode("ascii"),
        )
        self.assertEqual(payload["rawLogSize"], len(self.raw_log))

    def test_generates_unique_message_identifiers(self) -> None:
        first_message = BrokerMessage.from_record(self.record)
        second_message = BrokerMessage.from_record(self.record)

        self.assertEqual(str(UUID(first_message.message_id)), first_message.message_id)
        self.assertEqual(str(UUID(second_message.message_id)), second_message.message_id)
        self.assertNotEqual(first_message.message_id, second_message.message_id)

    def test_round_trips_non_utf8_payload_without_data_loss(self) -> None:
        original = BrokerMessage.from_record(self.record)

        restored = BrokerMessage.from_json_bytes(original.to_json_bytes())

        self.assertEqual(restored.message_id, original.message_id)
        self.assertEqual(restored.received_at, original.received_at)
        self.assertEqual(restored.protocol, original.protocol)
        self.assertEqual(restored.source_ip, original.source_ip)
        self.assertEqual(restored.source_port, original.source_port)
        self.assertEqual(restored.raw_log, self.raw_log)

    def test_rejects_missing_fields(self) -> None:
        payload = BrokerMessage.from_record(self.record).as_dict()
        payload.pop("rawLogBase64")

        with self.assertRaisesRegex(ValueError, "rawLogBase64"):
            BrokerMessage.from_dict(payload)

    def test_rejects_invalid_base64(self) -> None:
        payload = BrokerMessage.from_record(self.record).as_dict()
        payload["rawLogBase64"] = "invalid-base64!"

        with self.assertRaisesRegex(ValueError, "rawLogBase64"):
            BrokerMessage.from_dict(payload)

    def test_rejects_incorrect_raw_log_size(self) -> None:
        payload = BrokerMessage.from_record(self.record).as_dict()
        payload["rawLogSize"] = len(self.raw_log) + 1

        with self.assertRaisesRegex(ValueError, "rawLogSize"):
            BrokerMessage.from_dict(payload)

    def test_rejects_incorrect_raw_log_text(self) -> None:
        payload = BrokerMessage.from_record(self.record).as_dict()
        payload["rawLog"] = "different log"

        with self.assertRaisesRegex(ValueError, "rawLog does not match"):
            BrokerMessage.from_dict(payload)

    def test_rejects_invalid_metadata(self) -> None:
        invalid_values = (
            ("messageId", "not-a-uuid"),
            ("receivedAt", "2026-08-24T12:30:45"),
            ("protocol", "HTTP"),
            ("sourceIp", "invalid-ip"),
            ("sourcePort", 70000),
            ("schemaVersion", 2),
        )

        for field_name, value in invalid_values:
            with self.subTest(field_name=field_name):
                payload = BrokerMessage.from_record(self.record).as_dict()
                payload[field_name] = value

                with self.assertRaises(ValueError):
                    BrokerMessage.from_dict(payload)

    def test_rejects_invalid_json_payload(self) -> None:
        for payload in (b"not-json", json.dumps([]).encode("utf-8")):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    BrokerMessage.from_json_bytes(payload)