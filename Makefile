# Makefile for Stock Evaluation Pipeline
# Provides convenient shortcuts for common tasks

.PHONY: help build build-dev build-prod run run-dev run-test clean test docker-build docker-run docker-clean docker-test docker-dev compose-up compose-down compose-dev compose-test logs status

# Default target
help:
	@echo "Stock Evaluation Pipeline - Available Commands:"
	@echo ""
	@echo "Building:"
	@echo "  build        - Build production Docker image"
	@echo "  build-dev    - Build development Docker image"
	@echo "  build-prod   - Build production Docker image"
	@echo ""
	@echo "Running:"
	@echo "  run          - Run pipeline in test mode"
	@echo "  run-dev      - Run development environment"
	@echo "  run-test     - Run test suite"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build - Build Docker image using script"
	@echo "  docker-run   - Run Docker container using script"
	@echo "  docker-clean - Clean up Docker containers and images"
	@echo "  docker-test  - Run tests in Docker container"
	@echo "  docker-dev   - Run development environment in Docker"
	@echo ""
	@echo "Compose:"
	@echo "  compose-up   - Start services with docker-compose"
	@echo "  compose-down - Stop services with docker-compose"
	@echo "  compose-dev  - Start development environment"
	@echo "  compose-test - Run tests with docker-compose"
	@echo ""
	@echo "Utilities:"
	@echo "  logs         - Show container logs"
	@echo "  status       - Show container and image status"
	@echo "  clean        - Clean up build artifacts"
	@echo "  test         - Run tests locally"
	@echo ""

# Building targets
build: build-prod

build-prod:
	@echo "Building production Docker image..."
	./scripts/docker-build.sh production latest

build-dev:
	@echo "Building development Docker image..."
	./scripts/docker-build.sh development dev

# Running targets
run:
	@echo "Running pipeline in test mode..."
	./scripts/docker-run.sh pipeline test

run-dev:
	@echo "Starting development environment..."
	./scripts/docker-run.sh dev

run-test:
	@echo "Running test suite..."
	./scripts/docker-run.sh pipeline tests

# Docker script targets
docker-build:
	@echo "Building Docker image..."
	./scripts/docker-build.sh

docker-run:
	@echo "Running Docker container..."
	./scripts/docker-run.sh

docker-clean:
	@echo "Cleaning up Docker containers and images..."
	./scripts/docker-run.sh cleanup
	docker image prune -f
	docker system prune -f

docker-test:
	@echo "Running tests in Docker container..."
	./scripts/docker-run.sh pipeline tests

docker-dev:
	@echo "Starting development environment in Docker..."
	./scripts/docker-run.sh dev

# Docker Compose targets
compose-up:
	@echo "Starting services with docker-compose..."
	docker-compose up -d

compose-down:
	@echo "Stopping services with docker-compose..."
	docker-compose down

compose-dev:
	@echo "Starting development environment with docker-compose..."
	docker-compose --profile dev up -d pipeline-dev

compose-test:
	@echo "Running tests with docker-compose..."
	docker-compose --profile test up pipeline-test

compose-monitoring:
	@echo "Starting monitoring with docker-compose..."
	docker-compose --profile monitoring up -d monitoring

# Utility targets
logs:
	@echo "Showing container logs..."
	docker-compose logs -f

status:
	@echo "Showing container and image status..."
	./scripts/docker-run.sh status

clean:
	@echo "Cleaning up build artifacts..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf .coverage

test:
	@echo "Running tests locally..."
	python -m pytest tests/ -v

# Development targets
install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt

install-dev:
	@echo "Installing development dependencies..."
	pip install -r requirements.txt
	pip install ipython jupyter black flake8 mypy

format:
	@echo "Formatting code with black..."
	black .

lint:
	@echo "Running linter..."
	flake8 .

type-check:
	@echo "Running type checker..."
	mypy .

# Setup targets
setup:
	@echo "Setting up development environment..."
	python scripts/setup_environment.py

setup-docker:
	@echo "Setting up Docker environment..."
	@if [ ! -f .env ]; then \
		echo "Creating .env file from template..."; \
		cp .env.example .env 2>/dev/null || echo "# Add your environment variables here" > .env; \
		echo "Please edit .env file with your configuration."; \
	fi

# Production targets
deploy-staging:
	@echo "Deploying to staging..."
	# Add your staging deployment commands here

deploy-production:
	@echo "Deploying to production..."
	# Add your production deployment commands here

# Monitoring targets
monitor:
	@echo "Starting monitoring..."
	docker-compose --profile monitoring up -d

monitor-logs:
	@echo "Showing monitoring logs..."
	docker-compose --profile monitoring logs -f monitoring

# Backup and restore targets
backup:
	@echo "Creating backup..."
	tar -czf backup-$(date +%Y%m%d-%H%M%S).tar.gz data/ logs/ reports/

restore:
	@echo "Restoring from backup..."
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "Usage: make restore BACKUP_FILE=backup-file.tar.gz"; \
		exit 1; \
	fi
	tar -xzf $(BACKUP_FILE)

# Quick start target
quickstart: setup-docker build run
	@echo "Quick start completed!"
	@echo "Pipeline is running in test mode."
	@echo "Check logs with: make logs"
	@echo "Check status with: make status" 