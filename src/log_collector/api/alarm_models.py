from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from log_collector.api.models import LogResponse, log_response_from_document


class AlarmResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    ruleId: str
    ruleName: str
    sourceIp: str
    logId: str
    rawLog: str
    createdAt: datetime
    emailSent: bool


class AlarmDetailResponse(AlarmResponse):
    log: LogResponse | None


class PaginatedAlarmsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[AlarmResponse]
    page: int = Field(ge=1)
    pageSize: int = Field(ge=1)
    total: int = Field(ge=0)


def alarm_response_from_document(document: dict[str, Any]) -> AlarmResponse:
    return AlarmResponse(
        id=str(document["_id"]),
        ruleId=document["ruleId"],
        ruleName=document["ruleName"],
        sourceIp=document["sourceIp"],
        logId=document["logId"],
        rawLog=document["rawLog"],
        createdAt=document["createdAt"],
        emailSent=document["emailSent"],
    )


def alarm_detail_response_from_documents(
    alarm_document: dict[str, Any],
    log_document: dict[str, Any] | None,
) -> AlarmDetailResponse:
    alarm = alarm_response_from_document(alarm_document)
    log = (
        None
        if log_document is None
        else log_response_from_document(log_document)
    )

    return AlarmDetailResponse(**alarm.model_dump(), log=log)
