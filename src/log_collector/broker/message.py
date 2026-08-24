import base64
import binascii
import ipaddress
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast
from uuid import UUID, uuid4

from log_collector.models import LogRecord, TransportProtocol


@dataclass(frozen=True, slots=True)
class BrokerMessage:
    message_id: str
    received_at: datetime
    protocol: TransportProtocol
    source_ip: str
    source_port: int
    raw_log: bytes
    schema_version: int = 1

    def __post_init__(self) -> None:
        try:
            UUID(self.message_id)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError("messageId must be a valid UUID") from error

        if not isinstance(self.received_at, datetime):
            raise ValueError("receivedAt must be a timezone-aware datetime")

        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("receivedAt must be a timezone-aware datetime")

        if self.protocol not in ("UDP", "TCP"):
            raise ValueError("protocol must be UDP or TCP")

        try:
            ipaddress.ip_address(self.source_ip)
        except ValueError as error:
            raise ValueError("sourceIp must be a valid IP address") from error

        if type(self.source_port) is not int or not 1 <= self.source_port <= 65535:
            raise ValueError("sourcePort must be between 1 and 65535")

        if not isinstance(self.raw_log, bytes):
            raise ValueError("rawLog must contain bytes")

        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("schemaVersion must be 1")

    @classmethod
    def from_record(
        cls,
        record: LogRecord,
        message_id: str | None = None,
    ) -> "BrokerMessage":
        return cls(
            message_id=message_id or str(uuid4()),
            received_at=record.received_at,
            protocol=record.protocol,
            source_ip=record.source_ip,
            source_port=record.source_port,
            raw_log=record.raw_log,
        )

    def as_dict(self) -> dict[str, str | int]:
        received_at = self.received_at.astimezone(timezone.utc)

        return {
            "schemaVersion": self.schema_version,
            "messageId": self.message_id,
            "receivedAt": received_at.isoformat().replace("+00:00", "Z"),
            "protocol": self.protocol,
            "sourceIp": self.source_ip,
            "sourcePort": self.source_port,
            "rawLog": self.raw_log.decode("utf-8", errors="replace"),
            "rawLogBase64": base64.b64encode(self.raw_log).decode("ascii"),
            "rawLogSize": len(self.raw_log),
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "BrokerMessage":
        required_fields = (
            "schemaVersion",
            "messageId",
            "receivedAt",
            "protocol",
            "sourceIp",
            "sourcePort",
            "rawLog",
            "rawLogBase64",
            "rawLogSize",
        )

        missing_fields = [field for field in required_fields if field not in payload]

        if missing_fields:
            raise ValueError(
                f"Broker message is missing fields: {', '.join(missing_fields)}"
            )

        message_id = payload["messageId"]
        received_at_value = payload["receivedAt"]
        protocol = payload["protocol"]
        source_ip = payload["sourceIp"]
        source_port = payload["sourcePort"]
        raw_log_text = payload["rawLog"]
        encoded_raw_log = payload["rawLogBase64"]
        raw_log_size = payload["rawLogSize"]
        schema_version = payload["schemaVersion"]

        if not isinstance(message_id, str):
            raise ValueError("messageId must be a string")

        if not isinstance(received_at_value, str):
            raise ValueError("receivedAt must be an ISO 8601 timestamp")

        if not isinstance(protocol, str):
            raise ValueError("protocol must be a string")

        if not isinstance(source_ip, str):
            raise ValueError("sourceIp must be a string")

        if type(source_port) is not int:
            raise ValueError("sourcePort must be an integer")

        if not isinstance(raw_log_text, str):
            raise ValueError("rawLog must be a string")

        if not isinstance(encoded_raw_log, str):
            raise ValueError("rawLogBase64 must be a string")

        if type(raw_log_size) is not int:
            raise ValueError("rawLogSize must be an integer")

        if type(schema_version) is not int:
            raise ValueError("schemaVersion must be an integer")

        try:
            raw_log = base64.b64decode(encoded_raw_log, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("rawLogBase64 must contain valid Base64") from error

        if len(raw_log) != raw_log_size:
            raise ValueError("rawLogSize does not match the decoded raw log")

        if raw_log.decode("utf-8", errors="replace") != raw_log_text:
            raise ValueError("rawLog does not match the decoded raw log")

        try:
            received_at = datetime.fromisoformat(received_at_value)
        except ValueError as error:
            raise ValueError("receivedAt must be an ISO 8601 timestamp") from error

        return cls(
            message_id=message_id,
            received_at=received_at,
            protocol=cast(TransportProtocol, protocol),
            source_ip=source_ip,
            source_port=source_port,
            raw_log=raw_log,
            schema_version=schema_version,
        )

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "BrokerMessage":
        if not isinstance(payload, bytes):
            raise ValueError("Broker payload must contain bytes")

        try:
            decoded_payload = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Broker payload must contain valid JSON") from error

        if not isinstance(decoded_payload, dict):
            raise ValueError("Broker payload must contain a JSON object")

        return cls.from_dict(decoded_payload)