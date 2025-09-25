.PHONY: up down build logs restart worker api migrate makemigrations

# Detect available container runtime
CONTAINER_RUNTIME := $(shell command -v podman 2> /dev/null || command -v docker 2> /dev/null)

# Detect available compose tool with better logic
COMPOSE_CMD := $(shell \
    if command -v docker-compose > /dev/null 2>&1 && command -v podman > /dev/null 2>&1; then \
        echo "podman compose"; \
    elif command -v podman-compose > /dev/null 2>&1; then \
        echo "podman-compose"; \
    elif command -v docker-compose > /dev/null 2>&1; then \
        echo "docker-compose"; \
    else \
        echo ""; \
    fi \
)

# If no compose tool found but podman is available, use podman with compose subcommand
ifeq ($(COMPOSE_CMD),)
    ifeq ($(shell command -v podman > /dev/null 2>&1 && echo yes),yes)
        COMPOSE_CMD = podman compose
    else
        $(warning Neither docker-compose nor podman-compose found. Please install one of them.)
        COMPOSE_CMD = docker-compose
    endif
endif

EXEC_CMD := $(shell if command -v podman > /dev/null 2>&1; then echo "podman exec"; else echo "docker exec"; fi)

DOCKERFILE := compose.yml

up:
	@echo "Using: $(COMPOSE_CMD)"
	$(COMPOSE_CMD) -f $(DOCKERFILE) up --build -d

down:
	$(COMPOSE_CMD) -f $(DOCKERFILE) down

build:
	$(COMPOSE_CMD) -f $(DOCKERFILE) build

logs:
	$(COMPOSE_CMD) -f $(DOCKERFILE) logs -f

migrate:
	$(COMPOSE_CMD) -f $(DOCKERFILE) exec api alembic upgrade head

makemigrations:
	$(COMPOSE_CMD) -f $(DOCKERFILE) exec api alembic revision --autogenerate -m "Migration"

# Additional useful commands
restart: down up

ps:
	$(COMPOSE_CMD) -f $(DOCKERFILE) ps

status: ps

clean: down
	$(CONTAINER_RUNTIME) system prune -f

# Help target to show available commands
help:
	@echo "Available commands:"
	@echo "  make up           - Start containers (detects Docker/Podman)"
	@echo "  make down         - Stop containers"
	@echo "  make build        - Build containers"
	@echo "  make logs         - Follow container logs"
	@echo "  make restart      - Restart containers"
	@echo "  make migrate      - Run database migrations"
	@echo "  make makemigrations - Create new migration"
	@echo "  make ps           - Show container status"
	@echo "  make clean        - Clean up containers and images"
	@echo ""
	@echo "Detected runtime: $(CONTAINER_RUNTIME)"
	@echo "Detected compose tool: $(COMPOSE_CMD)"
