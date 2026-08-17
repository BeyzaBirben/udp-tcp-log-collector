import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal


TransportProtocol = Literal["UDP", "TCP"]


@dataclass(frozen=True, slots=True)
class LogRecord:
    received_at: datetime
    protocol: TransportProtocol
    source_ip: str
    source_port: int
    raw_log: bytes

    @classmethod
    def capture(
        cls,
        protocol: TransportProtocol,
        source_ip: str,
        source_port: int,
        raw_log: bytes,
    ) -> "LogRecord":
        return cls(
            received_at=datetime.now(timezone.utc),
            protocol=protocol,
            source_ip=source_ip,
            source_port=source_port,
            raw_log=raw_log,
        )

    def as_dict(self) -> dict[str, str | int]:
        return {
            "received_at": self.received_at.isoformat(),
            "protocol": self.protocol,
            "source_ip": self.source_ip,
            "source_port": self.source_port,
            "raw_log_text": self.raw_log.decode("utf-8", errors="replace"),
            "raw_log_base64": base64.b64encode(self.raw_log).decode("ascii"),
            "raw_log_size": len(self.raw_log),
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False)