.PHONY: up down logs test test-int lint fmt psql topics schema openapi dev-api dev-dashboard

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f $(s)

test:
	uv run pytest

test-int:
	uv run pytest tests/integration

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy contracts/edgeio_contracts simulator/edgeio_simulator worker/edgeio_worker api/edgeio_api

fmt:
	uv run ruff format .
	uv run ruff check --fix .

psql:
	docker compose exec timescaledb psql -U edgeio -d edgeio

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group edgeio-worker

schema:
	uv run python -m edgeio_contracts.export_schema > contracts/health.schema.json

openapi:
	uv run python -m edgeio_api.export_openapi > dashboard/openapi.json
	@if [ -d dashboard/node_modules ]; then npm --prefix dashboard run gen:api; fi

dev-api:
	uv run python -m edgeio_api

dev-dashboard:
	npm --prefix dashboard run dev
