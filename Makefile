.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose
DBT     := cd warehouse/dbt && dbt

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── local stack ───────────────────────────────────────────────────────────

.PHONY: up
up: ## Start the full local stack and wait for it to be healthy
	$(COMPOSE) up -d --build
	@echo "Waiting for services..."
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'
	@echo ""
	@echo "  Flink UI          http://localhost:8082"
	@echo "  Recommendations   http://localhost:8000/docs"
	@echo "  Schema Registry   http://localhost:8081"
	@echo "  MinIO console     http://localhost:9001"

.PHONY: down
down: ## Stop the stack, keeping volumes
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete all data
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs (make logs SERVICE=recommendations)
	$(COMPOSE) logs -f $(SERVICE)

# ── pipeline ──────────────────────────────────────────────────────────────

.PHONY: topics
topics: ## Create Kafka topics with production-shaped partitioning
	@for t in playback.events:12 qoe.windows:6 qoe.alerts:3 dlq.cdc.postgres:3; do \
		name=$${t%%:*}; parts=$${t##*:}; \
		$(COMPOSE) exec -T kafka kafka-topics --bootstrap-server localhost:29092 \
			--create --if-not-exists --topic $$name --partitions $$parts --replication-factor 1; \
	done

.PHONY: connectors
connectors: ## Register the Debezium source and the S3 sink
	curl -fsS -X POST -H "Content-Type: application/json" \
		--data @ingestion/debezium/postgres-source.json \
		http://localhost:8083/connectors | jq .
	@echo "Connector status:"
	@curl -fsS http://localhost:8083/connectors/streaming-postgres-source/status | jq .

.PHONY: simulate
simulate: ## Produce synthetic playback traffic (make simulate RATE=200)
	python ingestion/producers/playback_simulator.py \
		--brokers localhost:9092 --registry http://localhost:8081 \
		--rate $(or $(RATE),100) --sessions $(or $(SESSIONS),200)

.PHONY: simulate-incident
simulate-incident: ## Inject a CDN degradation to verify the QoE detector end to end
	python ingestion/producers/playback_simulator.py \
		--brokers localhost:9092 --registry http://localhost:8081 \
		--rate 300 --sessions 400 \
		--degrade-pop iad-3 --degrade-after 60 --degrade-severity 0.4

.PHONY: submit-jobs
submit-jobs: ## Submit both Flink jobs to the local cluster
	$(COMPOSE) exec flink-jobmanager flink run -py /opt/streaming/jobs/session_features.py -d
	$(COMPOSE) exec flink-jobmanager flink run -py /opt/streaming/jobs/qoe_anomaly.py -d

# ── warehouse ─────────────────────────────────────────────────────────────

.PHONY: dbt-build
dbt-build: ## Build all dbt models with their tests
	$(DBT) deps && $(DBT) build --target dev

.PHONY: dbt-test
dbt-test: ## Run dbt tests only
	$(DBT) test --target dev

.PHONY: dbt-docs
dbt-docs: ## Generate and serve the dbt documentation site
	$(DBT) docs generate --target dev && $(DBT) docs serve

.PHONY: dbt-fresh
dbt-fresh: ## Check source freshness
	$(DBT) source freshness --target dev

# ── quality ───────────────────────────────────────────────────────────────

.PHONY: lint
lint: ## Lint Python and SQL
	ruff check serving/ streaming/ ml/ orchestration/ ingestion/
	ruff format --check serving/ streaming/ ml/ orchestration/ ingestion/
	sqlfluff lint warehouse/dbt/models --dialect postgres

.PHONY: fmt
fmt: ## Auto-format everything
	ruff format serving/ streaming/ ml/ orchestration/ ingestion/
	ruff check --fix serving/ streaming/ ml/ orchestration/ ingestion/
	sqlfluff fix warehouse/dbt/models --dialect postgres
	terraform -chdir=infra/terraform fmt -recursive

.PHONY: test
test: ## Run the Python test suite with coverage
	cd serving && pytest tests/ -v --cov=app --cov-report=term-missing

.PHONY: load-test
load-test: ## Run the k6 load test against the local API
	k6 run load-tests/recommendations.js --env K6_TARGET=http://localhost:8000

# ── infrastructure ────────────────────────────────────────────────────────

.PHONY: tf-plan
tf-plan: ## Plan infrastructure changes (make tf-plan ENV=staging)
	terraform -chdir=infra/terraform init
	terraform -chdir=infra/terraform plan -var-file=environments/$(or $(ENV),staging).tfvars

.PHONY: tf-apply
tf-apply: ## Apply infrastructure changes
	terraform -chdir=infra/terraform apply -var-file=environments/$(or $(ENV),staging).tfvars

# ── end to end ────────────────────────────────────────────────────────────

.PHONY: demo
demo: up topics connectors submit-jobs ## Bring up everything and start generating traffic
	@echo ""
	@echo "Stack is running. Generating traffic for 60 seconds..."
	@python ingestion/producers/playback_simulator.py \
		--brokers localhost:9092 --registry http://localhost:8081 \
		--rate 150 --sessions 200 --duration 60
	@echo ""
	@echo "Try:  curl 'http://localhost:8000/recommendations?profile_id=101&region_code=us-east'"
