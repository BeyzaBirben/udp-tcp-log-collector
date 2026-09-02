import logging
from typing import Protocol

from log_collector.api.alarm_repository import NewAlarm
from log_collector.correlation import CorrelationMatch
from log_collector.email_delivery import EmailDispatcher, EmailNotification


logger = logging.getLogger(__name__)


class AlarmWriter(Protocol):
    async def connect(self) -> None:
        pass

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, object]:
        pass

    async def close(self) -> None:
        pass


class AlarmCorrelationMatchHandler:
    def __init__(
        self,
        repository: AlarmWriter,
        email_dispatcher: EmailDispatcher | None = None,
    ) -> None:
        self._repository = repository
        self._email_dispatcher = email_dispatcher

    async def connect(self) -> None:
        await self._repository.connect()

        if self._email_dispatcher is not None:
            await self._email_dispatcher.start()

    async def handle(self, match: CorrelationMatch) -> None:
        await self.connect()
        document = await self._repository.create_alarm(
            NewAlarm(
                rule_id=match.rule.rule_id,
                rule_name=match.rule.name,
                source_ip=match.message.source_ip,
                log_id=match.message.message_id,
                raw_log=match.message.raw_log.decode(
                    "utf-8",
                    errors="replace",
                ),
            )
        )

        logger.info(
            "Alarm created alarm_id=%s rule_id=%s log_id=%s",
            document["_id"],
            match.rule.rule_id,
            match.message.message_id,
        )

        if (
            self._email_dispatcher is not None
            and "Email" in match.rule.actions
        ):
            await self._email_dispatcher.enqueue(
                EmailNotification(
                    alarm_id=str(document["_id"]),
                    rule_id=match.rule.rule_id,
                    rule_name=match.rule.name,
                    source_ip=match.message.source_ip,
                    log_id=match.message.message_id,
                    raw_log=match.message.raw_log.decode(
                        "utf-8",
                        errors="replace",
                    ),
                    recipients=match.rule.email_to,
                )
            )

    async def close(self) -> None:
        try:
            if self._email_dispatcher is not None:
                await self._email_dispatcher.close()
        finally:
            await self._repository.close()
