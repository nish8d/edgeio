FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# PACKAGE selects which workspace member (and its dependencies) to install.
ARG PACKAGE
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package "${PACKAGE}"
