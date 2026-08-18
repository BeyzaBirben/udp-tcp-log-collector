FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .
RUN useradd --create-home --uid 10001 collector

USER collector

EXPOSE 5514/tcp
EXPOSE 5514/udp

STOPSIGNAL SIGTERM

CMD ["log-collector", "--host", "0.0.0.0", "--udp-port", "5514", "--tcp-port", "5514"]