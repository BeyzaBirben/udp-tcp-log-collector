import ipaddress
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path as FilePath
from typing import Annotated, AsyncIterator
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, Path, Query, Response, status
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from log_collector.api.alarm_models import (
    AlarmDetailResponse,
    PaginatedAlarmsResponse,
    alarm_detail_response_from_documents,
    alarm_response_from_document,
)
from log_collector.api.alarm_repository import (
    AlarmPage,
    AlarmQuery,
    AlarmRepository,
    MongoAlarmRepository,
)
from log_collector.api.config import (
    APISettings,
    AlarmStoreSettings,
    RuleStoreSettings,
)
from log_collector.api.models import (
    HealthResponse,
    LogResponse,
    PaginatedLogsResponse,
    SourceSummaryResponse,
    log_response_from_document,
)
from log_collector.api.repository import (
    LogPage,
    LogQuery,
    LogQueryRepository,
    MongoLogQueryRepository,
)
from log_collector.api.rule_models import (
    RuleCreateRequest,
    RuleResponse,
    RuleUpdateRequest,
    rule_response_from_document,
)
from log_collector.api.rule_repository import MongoRuleRepository, RuleRepository
from log_collector.storage import MongoDBSettings


def create_app(
    settings: APISettings | None = None,
    repository: LogQueryRepository | None = None,
    rule_repository: RuleRepository | None = None,
    alarm_repository: AlarmRepository | None = None,
) -> FastAPI:
    active_settings = APISettings.from_environment() if settings is None else settings
    database_settings: MongoDBSettings | None = None

    if (
        repository is None
        or rule_repository is None
        or alarm_repository is None
    ):
        database_settings = MongoDBSettings.from_environment()

    if repository is None:
        if database_settings is None:
            raise RuntimeError("MongoDB settings are unavailable")

        active_repository = MongoLogQueryRepository(database_settings)
    else:
        active_repository = repository

    if rule_repository is None:
        if database_settings is None:
            raise RuntimeError("MongoDB settings are unavailable")

        active_rule_repository = MongoRuleRepository(
            database_settings,
            RuleStoreSettings.from_environment(),
        )
    else:
        active_rule_repository = rule_repository

    if alarm_repository is None:
        if database_settings is None:
            raise RuntimeError("MongoDB settings are unavailable")

        active_alarm_repository = MongoAlarmRepository(
            database_settings,
            AlarmStoreSettings.from_environment(),
        )
    else:
        active_alarm_repository = alarm_repository

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        await active_repository.connect()

        try:
            await active_rule_repository.connect()
        except Exception:
            await active_repository.close()
            raise

        try:
            await active_alarm_repository.connect()
        except Exception:
            try:
                await active_rule_repository.close()
            finally:
                await active_repository.close()
            raise

        application.state.log_repository = active_repository
        application.state.rule_repository = active_rule_repository
        application.state.alarm_repository = active_alarm_repository

        try:
            yield
        finally:
            try:
                await active_alarm_repository.close()
            finally:
                try:
                    await active_rule_repository.close()
                finally:
                    await active_repository.close()

    application = FastAPI(
        title="UDP/TCP Log API",
        summary="API for collected logs, correlation rules, and alarms",
        version="1.2.0",
        lifespan=lifespan,
    )

    static_directory = FilePath(__file__).with_name("static")
    application.mount(
        "/ui",
        StaticFiles(directory=static_directory, html=True),
        name="ui",
    )

    @application.get("/", include_in_schema=False)
    async def ui_root() -> RedirectResponse:
        return RedirectResponse(url="/ui/")

    @application.get(
        "/health",
        response_model=HealthResponse,
        tags=["Health"],
        summary="Check API and database health",
    )
    async def health() -> HealthResponse:
        try:
            await active_repository.ping()
            await active_rule_repository.ping()
            await active_alarm_repository.ping()
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database is unavailable",
            ) from error

        return HealthResponse(status="ok")

    @application.get(
        "/api/logs",
        response_model=PaginatedLogsResponse,
        tags=["Logs"],
        summary="Query collected logs",
    )
    async def list_logs(
        from_: Annotated[
            datetime,
            Query(alias="from", description="Inclusive range start with timezone"),
        ],
        to: Annotated[
            datetime,
            Query(description="Inclusive range end with timezone"),
        ],
        source_ip: Annotated[
            str | None,
            Query(alias="sourceIp", description="Exact source IP address"),
        ] = None,
        q: Annotated[
            str | None,
            Query(max_length=2048, description="Case-insensitive raw log search"),
        ] = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[
            int,
            Query(alias="pageSize", ge=1, le=active_settings.max_page_size),
        ] = active_settings.default_page_size,
    ) -> PaginatedLogsResponse:
        start, end = _validated_time_range(from_, to)
        query = LogQuery(
            start=start,
            end=end,
            source_ip=_validated_source_ip(source_ip),
            text=_normalized_text(q),
        )
        result = await active_repository.list_logs(query, page, page_size)

        return _paginated_response(result, page, page_size)

    @application.get(
        "/api/logs/{id}",
        response_model=LogResponse,
        tags=["Logs"],
        summary="Get one collected log",
    )
    async def get_log(
        log_id: Annotated[UUID, Path(alias="id", description="Log identifier")],
    ) -> LogResponse:
        document = await active_repository.get_log(str(log_id))

        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Log was not found",
            )

        return log_response_from_document(document)

    @application.get(
        "/api/sources",
        response_model=list[SourceSummaryResponse],
        tags=["Sources"],
        summary="List source IP summaries",
    )
    async def list_sources() -> list[SourceSummaryResponse]:
        sources = await active_repository.list_sources()

        return [SourceSummaryResponse.model_validate(source) for source in sources]

    @application.get(
        "/api/sources/{sourceIp}/logs",
        response_model=PaginatedLogsResponse,
        tags=["Sources"],
        summary="Query logs from one source IP",
    )
    async def list_source_logs(
        source_ip: Annotated[
            str,
            Path(alias="sourceIp", description="Source IP address"),
        ],
        from_: Annotated[
            datetime,
            Query(alias="from", description="Inclusive range start with timezone"),
        ],
        to: Annotated[
            datetime,
            Query(description="Inclusive range end with timezone"),
        ],
        q: Annotated[
            str | None,
            Query(max_length=2048, description="Case-insensitive raw log search"),
        ] = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[
            int,
            Query(alias="pageSize", ge=1, le=active_settings.max_page_size),
        ] = active_settings.default_page_size,
    ) -> PaginatedLogsResponse:
        start, end = _validated_time_range(from_, to)
        query = LogQuery(
            start=start,
            end=end,
            source_ip=_validated_source_ip(source_ip),
            text=_normalized_text(q),
        )
        result = await active_repository.list_logs(query, page, page_size)

        return _paginated_response(result, page, page_size)

    async def ensure_source_exists(source_ip: str) -> None:
        if not await active_repository.source_exists(source_ip):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="sourceIp must identify a source that exists in collected logs",
            )

    @application.get(
        "/api/rules",
        response_model=list[RuleResponse],
        tags=["Rules"],
        summary="List correlation rules",
    )
    async def list_rules() -> list[RuleResponse]:
        documents = await active_rule_repository.list_rules()

        return [rule_response_from_document(document) for document in documents]

    @application.post(
        "/api/rules",
        response_model=RuleResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["Rules"],
        summary="Create a correlation rule",
    )
    async def create_rule(request: RuleCreateRequest) -> RuleResponse:
        await ensure_source_exists(request.sourceIp)
        document = await active_rule_repository.create_rule(request)

        return rule_response_from_document(document)

    @application.get(
        "/api/rules/{id}",
        response_model=RuleResponse,
        tags=["Rules"],
        summary="Get one correlation rule",
    )
    async def get_rule(
        rule_id: Annotated[UUID, Path(alias="id", description="Rule identifier")],
    ) -> RuleResponse:
        document = await active_rule_repository.get_rule(str(rule_id))

        if document is None:
            raise _rule_not_found()

        return rule_response_from_document(document)

    @application.put(
        "/api/rules/{id}",
        response_model=RuleResponse,
        tags=["Rules"],
        summary="Replace a correlation rule",
    )
    async def update_rule(
        request: RuleUpdateRequest,
        rule_id: Annotated[UUID, Path(alias="id", description="Rule identifier")],
    ) -> RuleResponse:
        await ensure_source_exists(request.sourceIp)
        document = await active_rule_repository.update_rule(str(rule_id), request)

        if document is None:
            raise _rule_not_found()

        return rule_response_from_document(document)

    @application.delete(
        "/api/rules/{id}",
        status_code=status.HTTP_204_NO_CONTENT,
        response_class=Response,
        tags=["Rules"],
        summary="Delete a correlation rule",
    )
    async def delete_rule(
        rule_id: Annotated[UUID, Path(alias="id", description="Rule identifier")],
    ) -> Response:
        deleted = await active_rule_repository.delete_rule(str(rule_id))

        if not deleted:
            raise _rule_not_found()

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @application.get(
        "/api/alarms",
        response_model=PaginatedAlarmsResponse,
        tags=["Alarms"],
        summary="Query generated alarms",
    )
    async def list_alarms(
        from_: Annotated[
            datetime | None,
            Query(alias="from", description="Inclusive range start with timezone"),
        ] = None,
        to: Annotated[
            datetime | None,
            Query(description="Inclusive range end with timezone"),
        ] = None,
        rule_id: Annotated[
            UUID | None,
            Query(alias="ruleId", description="Exact rule identifier"),
        ] = None,
        source_ip: Annotated[
            str | None,
            Query(alias="sourceIp", description="Exact source IP address"),
        ] = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[
            int,
            Query(alias="pageSize", ge=1, le=active_settings.max_page_size),
        ] = active_settings.default_page_size,
    ) -> PaginatedAlarmsResponse:
        start, end = _validated_optional_time_range(from_, to)
        query = AlarmQuery(
            start=start,
            end=end,
            rule_id=None if rule_id is None else str(rule_id),
            source_ip=_validated_source_ip(source_ip),
        )
        result = await active_alarm_repository.list_alarms(
            query,
            page,
            page_size,
        )

        return _paginated_alarm_response(result, page, page_size)

    @application.get(
        "/api/alarms/{id}",
        response_model=AlarmDetailResponse,
        tags=["Alarms"],
        summary="Get an alarm and its triggering log",
    )
    async def get_alarm(
        alarm_id: Annotated[UUID, Path(alias="id", description="Alarm identifier")],
    ) -> AlarmDetailResponse:
        document = await active_alarm_repository.get_alarm(str(alarm_id))

        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Alarm was not found",
            )

        log_document = await active_repository.get_log(document["logId"])

        return alarm_detail_response_from_documents(document, log_document)

    return application


def _validated_time_range(
    start: datetime,
    end: datetime,
) -> tuple[datetime, datetime]:
    if start.tzinfo is None or start.utcoffset() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must include a timezone",
        )

    if end.tzinfo is None or end.utcoffset() is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="to must include a timezone",
        )

    normalized_start = start.astimezone(timezone.utc)
    normalized_end = end.astimezone(timezone.utc)

    if normalized_start > normalized_end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must be earlier than or equal to to",
        )

    return normalized_start, normalized_end


def _validated_optional_time_range(
    start: datetime | None,
    end: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    if start is not None and (start.tzinfo is None or start.utcoffset() is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must include a timezone",
        )

    if end is not None and (end.tzinfo is None or end.utcoffset() is None):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="to must include a timezone",
        )

    normalized_start = (
        None if start is None else start.astimezone(timezone.utc)
    )
    normalized_end = None if end is None else end.astimezone(timezone.utc)

    if (
        normalized_start is not None
        and normalized_end is not None
        and normalized_start > normalized_end
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must be earlier than or equal to to",
        )

    return normalized_start, normalized_end


def _validated_source_ip(source_ip: str | None) -> str | None:
    if source_ip is None:
        return None

    try:
        return str(ipaddress.ip_address(source_ip.strip()))
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="sourceIp must be a valid IPv4 or IPv6 address",
        ) from error


def _normalized_text(text: str | None) -> str | None:
    if text is None:
        return None

    normalized = text.strip()

    return normalized or None


def _paginated_response(
    result: LogPage,
    page: int,
    page_size: int,
) -> PaginatedLogsResponse:
    return PaginatedLogsResponse(
        items=[
            log_response_from_document(document) for document in result.documents
        ],
        page=page,
        pageSize=page_size,
        total=result.total,
    )


def _paginated_alarm_response(
    result: AlarmPage,
    page: int,
    page_size: int,
) -> PaginatedAlarmsResponse:
    return PaginatedAlarmsResponse(
        items=[
            alarm_response_from_document(document)
            for document in result.documents
        ],
        page=page,
        pageSize=page_size,
        total=result.total,
    )


def _rule_not_found() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Rule was not found",
    )


app = create_app()


def main() -> None:
    settings = APISettings.from_environment()
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
