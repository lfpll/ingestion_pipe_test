.PHONY: clean build up down uuid

clean: down
	@echo "Cleaning up  directories..."
	@find . -name .ipynb_checkpoints -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	@docker-compose rm -v
	@docker-compose down -v
	@echo "Clean up completed."

build:
	@echo "Building Docker containers..."
	@docker-compose build
	@docker build -t ingestion-data .
	@echo "Build completed."

up: check-stop
	@echo "Starting Docker containers..."
	@docker-compose up -d

down:
	@echo "Stopping Docker containers..."
	@docker-compose down
	@echo "Containers stopped."

check-stop:
	@echo "Checking if any Docker containers are up..."
	@if docker-compose ps -q | grep -q '.'; then \
		echo "Containers are running, stopping them now..."; \
		docker-compose down; \
	else \
		echo "No containers are currently running."; \
	fi
