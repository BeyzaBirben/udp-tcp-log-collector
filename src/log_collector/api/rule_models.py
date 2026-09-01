import ipaddress
import re
from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


RuleField = Literal["rawLog", "sourceIp"]
RuleOperator = Literal["contains", "equals", "startsWith", "regex"]
RuleAction = Literal["Alarm", "Email"]


class RuleCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: RuleField
    operator: RuleOperator
    value: str = Field(min_length=1, max_length=4096)

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("condition value must not be empty")

        return value

    @model_validator(mode="after")
    def validate_regex(self) -> Self:
        if self.operator == "regex":
            try:
                re.compile(self.value)
            except re.error as error:
                raise ValueError(f"invalid regular expression: {error}") from error

        return self


class RuleWriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    enabled: bool = True
    sourceIp: str
    conditions: list[RuleCondition] = Field(min_length=1)
    actions: list[RuleAction] = Field(min_length=1)
    emailTo: list[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()

        if not normalized:
            raise ValueError("rule name must not be empty")

        return normalized

    @field_validator("sourceIp")
    @classmethod
    def validate_source_ip(cls, value: str) -> str:
        try:
            return str(ipaddress.ip_address(value.strip()))
        except ValueError as error:
            raise ValueError(
                "sourceIp must be a valid IPv4 or IPv6 address"
            ) from error

    @field_validator("actions")
    @classmethod
    def validate_actions(cls, actions: list[RuleAction]) -> list[RuleAction]:
        if len(actions) != len(set(actions)):
            raise ValueError("rule actions must be unique")

        return actions

    @field_validator("emailTo")
    @classmethod
    def validate_email_recipients(cls, recipients: list[str]) -> list[str]:
        normalized: list[str] = []

        for recipient in recipients:
            address = recipient.strip()

            if not address or address.count("@") != 1:
                raise ValueError("emailTo must contain valid email addresses")

            local_part, domain = address.split("@", 1)

            has_whitespace = any(
                character.isspace() for character in address
            )

            if not local_part or not domain or has_whitespace:
                raise ValueError("emailTo must contain valid email addresses")

            normalized.append(address)

        if len(normalized) != len(set(normalized)):
            raise ValueError("emailTo addresses must be unique")

        return normalized

    @model_validator(mode="after")
    def validate_email_action(self) -> Self:
        if "Email" in self.actions and not self.emailTo:
            raise ValueError("emailTo is required when Email action is enabled")

        return self


class RuleCreateRequest(RuleWriteRequest):
    pass


class RuleUpdateRequest(RuleWriteRequest):
    pass


class RuleResponse(RuleWriteRequest):
    id: str
    createdAt: datetime
    updatedAt: datetime


def rule_response_from_document(document: dict[str, Any]) -> RuleResponse:
    return RuleResponse(
        id=str(document["_id"]),
        name=document["name"],
        enabled=document["enabled"],
        sourceIp=document["sourceIp"],
        conditions=document["conditions"],
        actions=document["actions"],
        emailTo=document["emailTo"],
        createdAt=document["createdAt"],
        updatedAt=document["updatedAt"],
    )
