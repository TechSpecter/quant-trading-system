# ==========================================
# Quant Trading System - Makefile
# ==========================================

.PHONY: up down logs build backend worker frontend format test shell dev clean run-script run-strategy run-market token token-check

# -----------------------------
# Docker Commands
# -----------------------------
up:
	docker-compose up -d

down:
	docker-compose down -v

logs:
	docker-compose logs -f

build:
	docker-compose build

# -----------------------------
# Backend Commands
# -----------------------------
backend:
	cd backend && poetry run uvicorn app.main:app --reload

worker:
	cd backend && poetry run python -m app.workers.scheduler

shell:
	cd backend && poetry shell

# -----------------------------
# Frontend Commands
# -----------------------------
frontend:
	cd frontend && npm run dev

# -----------------------------
# Dev Utilities
# -----------------------------
format:
	cd backend && poetry run black .

lint:
	cd backend && poetry run isort .

test:
	cd backend && poetry run pytest

# -----------------------------
# Script Runners (from root)
# -----------------------------
run-script:
	cd backend && poetry run python tests/scripts/$(SCRIPT)

run-strategy:
	cd backend && poetry run python tests/scripts/test_strategy.py

run-market:
	cd backend && poetry run python tests/scripts/test_market_data.py

# -----------------------------
# Auth Helper
# -----------------------------
token:
	@echo "🚀 Starting backend (if not already running)..."
	@cd backend && poetry run uvicorn app.main:app --reload &

	@sleep 2

	@echo "🌐 Opening Fyers login in Brave..."
	@if command -v open >/dev/null 2>&1; then \
		open -a "Brave Browser" http://127.0.0.1:8000/auth/login; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open http://127.0.0.1:8000/auth/login; \
	else \
		echo "Please open http://127.0.0.1:8000/auth/login in your browser"; \
	fi

	@echo ""
	@echo "⏳ Waiting for access_token in Redis..."

	@COUNTER=0; \
	while [ $$COUNTER -lt 30 ]; do \
		TOKEN=$$(curl -s http://127.0.0.1:8000/auth/status | grep -o '"access_token":"[^"]*"' | cut -d':' -f2 | tr -d '"'); \
		if [ -n "$$TOKEN" ] && [ "$$TOKEN" != "null" ]; then \
			echo "✅ access_token detected"; \
			break; \
		fi; \
		echo "⏳ Waiting... ($$COUNTER)"; \
		sleep 2; \
		COUNTER=$$((COUNTER+1)); \
	done; \
	if [ $$COUNTER -eq 30 ]; then \
		echo "❌ Timeout waiting for access_token"; \
		exit 1; \
	fi

	@echo "✅ Access token already generated via callback"

	@echo ""
	@echo "✅ Access token should now be available in Redis (fyers_access_token)"

token-check:
	@echo "Fetching token from Redis..."
	docker exec -it algo-redis redis-cli GET fyers_access_token

# -----------------------------
# Full Dev Setup (ONE COMMAND)
# -----------------------------
dev:
	make up && \
	make backend & \
	make frontend

# -----------------------------
# Clean Everything
# -----------------------------
clean:
	@echo "🧹 Cleaning entire system..."
	@echo "⛔ Stopping and removing containers + volumes"
	docker compose down -v

	@echo "🗑️ Removing Python virtual environment"
	rm -rf backend/.venv

	@echo "🔥 Clearing Redis cache"
	@if docker ps -a | grep -q algo-redis; then \
		docker start algo-redis >/dev/null 2>&1 || true; \
		docker exec -it algo-redis redis-cli FLUSHALL; \
	else \
		echo "⚠️ Redis container not found"; \
	fi

	@echo "🗄️ Clearing DB candles table (optional)"
	@echo "👉 Run manually if needed: DELETE FROM candles;"

	@echo "✅ Full cleanup done"

# -----------------------------
# Clean Everything
# -----------------------------
reset:
	@echo "🧹 Cleaning entire system..."
	@echo "⛔ Stopping and removing containers + volumes"
	docker compose down -v
	
	@echo "🔥 Clearing Redis cache"
	@if docker ps -a | grep -q algo-redis; then \
		docker start algo-redis >/dev/null 2>&1 || true; \
		docker exec -it algo-redis redis-cli FLUSHALL; \
	else \
		echo "⚠️ Redis container not found"; \
	fi

	@echo "🗄️ Clearing DB candles table (optional)"
	@echo "👉 Run manually if needed: DELETE FROM candles;"

	@echo "✅ Full cleanup done"