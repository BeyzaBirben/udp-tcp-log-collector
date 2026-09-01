import ipaddress
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, AsyncIterator
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, Path, Query, status

from log_collector.api.config import APISettings
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
from log_collector.storage import MongoDBSettings


def create_app(
    settings: APISettings | None = None,
    repository: LogQueryRepository | None = None,
) -> FastAPI:
    active_settings = APISettings.from_environment() if settings is None else settings
    active_repository = (
        MongoLogQueryRepository(MongoDBSettings.from_environment())
        if repository is None
        else repository
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        await active_repository.connect()
        application.state.log_repository = active_repository

        try:
            yield
        finally:
            await active_repository.close()

    application = FastAPI(
        title="UDP/TCP Log API",
        summary="Read-only API for collected UDP and TCP logs",
        version="1.0.0",
        lifespan=lifespan,
    )

    @application.get(
        "/health",
        response_model=HealthResponse,
        tags=["Health"],
        summary="Check API and database health",
    )
    async def health() -> HealthResponse:
        try:
            await active_repository.ping()
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
