import asyncio
import logging
import math
import os
import smtplib
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Protocol


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SMTPSettings:
    host: str = "mailhog"
    port: int = 1025
    from_address: str = "alerts@log-collector.local"
    username: str | None = None
    password: str | None = None
    use_tls: bool = False
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("SMTP_HOST must not be empty")

        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("SMTP_PORT must be between 1 and 65535")

        if not _valid_email_address(self.from_address):
            raise ValueError("SMTP_FROM must be a valid email address")

        credentials = {
            "SMTP_USERNAME": self.username,
            "SMTP_PASSWORD": self.password,
        }

        for variable_name, value in credentials.items():
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"{variable_name} must not be empty")

        if (self.username is None) != (self.password is None):
            raise ValueError(
                "SMTP_USERNAME and SMTP_PASSWORD must be configured together"
            )

        if not isinstance(self.use_tls, bool):
            raise ValueError("SMTP_USE_TLS must be a boolean")

        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("SMTP_TIMEOUT_SECONDS must be positive")

    @classmethod
    def from_environment(
        cls,
        values: Mapping[str, str] | None = None,
    ) -> "SMTPSettings":
        environment = os.environ if values is None else values

        return cls(
            host=environment.get("SMTP_HOST", "mailhog").strip(),
            port=_integer(environment, "SMTP_PORT", 1025),
            from_address=environment.get(
                "SMTP_FROM",
                "alerts@log-collector.local",
            ).strip(),
            username=_optional(environment.get("SMTP_USERNAME")),
            password=_optional(environment.get("SMTP_PASSWORD")),
            use_tls=_boolean(environment, "SMTP_USE_TLS", False),
            timeout_seconds=_positive_float(
                environment,
                "SMTP_TIMEOUT_SECONDS",
                5.0,
            ),
        )


@dataclass(frozen=True, slots=True)
class EmailNotification:
    alarm_id: str
    rule_id: str
    rule_name: str
    source_ip: str
    log_id: str
    raw_log: str
    recipients: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.recipients:
            raise ValueError("Email notification must have at least one recipient")


class EmailSender(Protocol):
    async def send(self, notification: EmailNotification) -> None:
        pass


class EmailStatusWriter(Protocol):
    async def mark_email_sent(self, alarm_id: str) -> bool:
        pass


class EmailDispatcher(Protocol):
    async def start(self) -> None:
        pass

    async def enqueue(self, notification: EmailNotification) -> None:
        pass

    async def close(self) -> None:
        pass


class SMTPEmailSender:
    def __init__(self, settings: SMTPSettings) -> None:
        self._settings = settings

    async def send(self, notification: EmailNotification) -> None:
        await asyncio.to_thread(self._send, notification)

    def _send(self, notification: EmailNotification) -> None:
        message = self._message(notification)

        with smtplib.SMTP(
            self._settings.host,
            self._settings.port,
            timeout=self._settings.timeout_seconds,
        ) as client:
            client.ehlo()

            if self._settings.use_tls:
                client.starttls(context=ssl.create_default_context())
                client.ehlo()

            if self._settings.username is not None:
                client.login(
                    self._settings.username,
                    self._settings.password,
                )

            client.send_message(message)

    def _message(self, notification: EmailNotification) -> EmailMessage:
        message = EmailMessage()
        subject_name = " ".join(notification.rule_name.splitlines()).strip()
        message["From"] = self._settings.from_address
        message["To"] = ", ".join(notification.recipients)
        message["Subject"] = f"[Log Collector] {subject_name}"
        message.set_content(
            "\n".join(
                (
                    "A correlation rule generated an alarm.",
                    "",
                    f"Alarm ID: {notification.alarm_id}",
                    f"Rule ID: {notification.rule_id}",
                    f"Rule: {notification.rule_name}",
                    f"Source IP: {notification.source_ip}",
                    f"Log ID: {notification.log_id}",
                    "",
                    "Raw log:",
                    notification.raw_log,
                )
            )
        )

        return message


class AsyncEmailDispatcher:
    def __init__(
        self,
        sender: EmailSender,
        status_writer: EmailStatusWriter,
    ) -> None:
        self._sender = sender
        self._status_writer = status_writer
        self._queue: asyncio.Queue[EmailNotification | None] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._worker_task is not None:
                return

            self._worker_task = asyncio.create_task(
                self._run(),
                name="alarm-email-delivery",
            )

    async def enqueue(self, notification: EmailNotification) -> None:
        await self.start()
        self._queue.put_nowait(notification)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            task = self._worker_task

            if task is None:
                return

            self._queue.put_nowait(None)
            self._worker_task = None
            await task

    async def _run(self) -> None:
        while True:
            notification = await self._queue.get()

            try:
                if notification is None:
                    return

                await self._deliver(notification)
            finally:
                self._queue.task_done()

    async def _deliver(self, notification: EmailNotification) -> None:
        try:
            await self._sender.send(notification)
        except Exception:
            logger.exception(
                "Alarm email delivery failed alarm_id=%s",
                notification.alarm_id,
            )
            return

        try:
            updated = await self._status_writer.mark_email_sent(
                notification.alarm_id
            )
        except Exception:
            logger.exception(
                "Failed to update alarm email status alarm_id=%s",
                notification.alarm_id,
            )
            return

        if not updated:
            logger.warning(
                "Alarm disappeared before email status update alarm_id=%s",
                notification.alarm_id,
            )
            return

        logger.info(
            "Alarm email delivered alarm_id=%s recipients=%s",
            notification.alarm_id,
            len(notification.recipients),
        )


def _valid_email_address(value: str) -> bool:
    if not isinstance(value, str) or value.count("@") != 1:
        return False

    local_part, domain = value.split("@", 1)

    return bool(
        local_part
        and domain
        and not any(character.isspace() for character in value)
    )


def _optional(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()

    return normalized or None


def _integer(
    environment: Mapping[str, str],
    variable_name: str,
    default: int,
) -> int:
    raw_value = environment.get(variable_name, str(default))

    try:
        return int(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be an integer") from error


def _boolean(
    environment: Mapping[str, str],
    variable_name: str,
    default: bool,
) -> bool:
    raw_value = environment.get(variable_name, str(default)).strip().lower()

    if raw_value in {"1", "true", "yes", "on"}:
        return True

    if raw_value in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"{variable_name} must be a boolean")


def _positive_float(
    environment: Mapping[str, str],
    variable_name: str,
    default: float,
) -> float:
    raw_value = environment.get(variable_name, str(default))

    try:
        value = float(raw_value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{variable_name} must be a positive number") from error

    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{variable_name} must be a positive number")

    return value
