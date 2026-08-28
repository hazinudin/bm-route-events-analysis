FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED="1" \
    DBT_PROJECT_DIR="/app/events_analysis" \
    DBT_PROFILES_DIR="/run/secrets/dbt"

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev --no-install-project

COPY src/dbt_events_consumer ./src/dbt_events_consumer
COPY events_analysis ./events_analysis

ENTRYPOINT ["python", "-m", "dbt_events_consumer"]
