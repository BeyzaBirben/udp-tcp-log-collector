import base64
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class LogResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    schemaVersion: int = Field(ge=1)
    messageId: str
    receivedAt: datetime
    protocol: Literal["UDP", "TCP"]
    sourceIp: str
    sourcePort: int = Field(ge=1, le=65535)
    rawLog: str
    rawLogBase64: str
    rawLogSize: int = Field(ge=0)
    rawLogBytes: str = Field(
        description="MongoDB rawLogBytes encoded as Base64 for JSON transport"
    )


class PaginatedLogsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[LogResponse]
    page: int = Field(ge=1)
    pageSize: int = Field(ge=1)
    total: int = Field(ge=0)


class SourceSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sourceIp: str
    logCount: int = Field(ge=0)
    firstSeen: datetime
    lastSeen: datetime


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]


def log_response_from_document(document: dict[str, Any]) -> LogResponse:
    raw_log_bytes = bytes(document["rawLogBytes"])

    return LogResponse(
        id=str(document["_id"]),
        schemaVersion=document["schemaVersion"],
        messageId=document["messageId"],
        receivedAt=document["receivedAt"],
        protocol=document["protocol"],
        sourceIp=document["sourceIp"],
        sourcePort=document["sourcePort"],
        rawLog=document["rawLog"],
        rawLogBase64=document["rawLogBase64"],
        rawLogSize=document["rawLogSize"],
        rawLogBytes=base64.b64encode(raw_log_bytes).decode("ascii"),
    )
