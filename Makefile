

# ==========================================
# Quant Trading System - Makefile
# ==========================================

.PHONY: up down logs build backend worker frontend format test shell

# -----------------------------
# Docker Commands
# -----------------------------
up:
	docker-compose up -d

down:
	docker-compose down

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
	docker-compose down -v
	rm -rf backend/.venv