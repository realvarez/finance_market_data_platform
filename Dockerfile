FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies from uv.lock first for better layer caching.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY ingestion/ ./ingestion/
COPY entrypoint/ ./entrypoint/
COPY schemas/ ./schemas/
COPY utils/ ./utils/

ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=broker:29092

CMD ["uv", "run", "--no-dev", "python", "-m", "entrypoint.tick_service"]
