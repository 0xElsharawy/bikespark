FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /opt/dbt

COPY . .

RUN uv sync

ENV PATH="/opt/dbt/.venv/bin:$PATH"

CMD ["dbt", "debug"]
