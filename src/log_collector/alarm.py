import logging
from typing import Protocol

from log_collector.api.alarm_repository import NewAlarm
from log_collector.correlation import CorrelationMatch


logger = logging.getLogger(__name__)


class AlarmWriter(Protocol):
    async def connect(self) -> None:
        pass

    async def create_alarm(self, alarm: NewAlarm) -> dict[str, object]:
        pass

    async def close(self) -> None:
        pass


class AlarmCorrelationMatchHandler:
    def __init__(self, repository: AlarmWriter) -> None:
        self._repository = repository

    async def connect(self) -> None:
        await self._repository.connect()

    async def handle(self, match: CorrelationMatch) -> None:
        await self._repository.connect()
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

    async def close(self) -> None:
        await self._repository.close()
