# Makefile

# Colors for help messages
YELLOW := $(shell tput setaf 3)
RESET := $(shell tput sgr0)

# Default goal
.DEFAULT_GOAL := help

# Commands
.PHONY: help run clean

help: ## Show this help
	@echo ""
	@echo "${YELLOW}Available Commands:${RESET}"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

run: ## Start the services using docker-compose
	@echo "Running docker-compose up --build..."
	docker-compose up --build

clean: ## Clean up the environment (e.g., stop containers, remove images)
	@echo "Stopping and removing containers..."
	docker-compose down --volumes --remove-orphans
	@echo "Cleaning up images..."
	docker rmi $(shell docker images -q)
