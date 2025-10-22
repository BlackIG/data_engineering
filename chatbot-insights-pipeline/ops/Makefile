# ops/Makefile. #Fix: don’t put comments on the same line as variable assignments (Make treats # as part of the value there). Also strip spaces when expanding.
IMAGE?=salesiq-etl:dev
# container port (gunicorn binds to $PORT)
PORT?=8080
PORT_HOST?=8080
# host port; change PORT_HOST if 8080 is busy

# Build image
build:
	docker build -f ops/Dockerfile -t $(IMAGE) .

# Run server (HTTP /run) with a NAME so docker exec works
run:
	docker run --rm --name salesiq-etl \
		-p $(PORT_HOST):$(PORT) \
		--env-file .env \
		-e PORT=$(PORT) \
		-e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json \
		-v $$PWD/keyfile/etlsa_key.json:/secrets/key.json:ro \
		$(IMAGE)

# Run ETL once and exit
run-once:
	docker run --rm --name salesiq-etl-once \
		--env-file .env \
		-e PORT=$(PORT) \
		-e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json \
		-v $$PWD/keyfile/etlsa_key.json:/secrets/key.json:ro \
		$(IMAGE) python -u main.py

# Compose (server)
up:
	docker compose -f ops/docker-compose.yml up --build

# Compose (one-shot)
once:
	docker compose -f ops/docker-compose.yml run --rm run-once

# Tail logs (compose)
logs:
	docker compose -f ops/docker-compose.yml logs -f --tail=200

# Stop stack (compose)
down:
	docker compose -f ops/docker-compose.yml down

# Debug helpers
ps:
	docker ps --format 'table {{.ID}}\t{{.Names}}\t{{.Ports}}\t{{.Status}}'

sh:
	docker exec -it salesiq-etl sh || true
