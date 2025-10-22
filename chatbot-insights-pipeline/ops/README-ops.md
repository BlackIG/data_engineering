# Ops Guide (Docker / Compose / Make)

This document is **ops-only**: how to build and run the service using Docker, Docker Compose, and Make. (All ports set to **8080**.)

---

## Prerequisites
- Docker Desktop (or Docker Engine)
- Optional: Google ADC for local auth
  ```bash
  gcloud auth application-default login
  ```
- A `.env` at the repo root (not committed)

---

## Make targets (recommended)

From the **repo root**:

### Build the image
```bash
make -f ops/Makefile build
```

### Run HTTP server (Flask via Gunicorn on :8080)
```bash
make -f ops/Makefile run
# then trigger a run (in another terminal):
curl -X POST http://localhost:8080/run
```

### Run ETL once (no server; exits on completion)
```bash
make -f ops/Makefile run-once
```

### Docker Compose alternatives
```bash
make -f ops/Makefile up     # start server (compose)
make -f ops/Makefile once   # one-shot (compose)
make -f ops/Makefile logs   # tail logs
make -f ops/Makefile down   # stop & remove
```

---

## Docker (manual usage, if you don't want Make)

### Build
```bash
docker build -f ops/Dockerfile -t salesiq-etl:dev .
```

### Run server
```bash
docker run --rm -p 8080:8080   --env-file .env   -v "$HOME/.config/gcloud:/root/.config/gcloud:ro"   salesiq-etl:dev
# trigger:
curl -X POST http://localhost:8080/run
```

### Run once
```bash
docker run --rm   --env-file .env   -v "$HOME/.config/gcloud:/root/.config/gcloud:ro"   salesiq-etl:dev python -u main.py
```

> If using a service-account key instead of ADC, add:  
> `-e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json -v "$PWD/keyfile.json:/secrets/key.json:ro"`

---

## Notes
- **Ports**: container listens on **8080** (host mapping `-p HOST:8080`).  
- **Build context**: use `.dockerignore` to keep builds fast.  
- **Apple Silicon**: build for AMD64 if needed:
  ```bash
  docker build --platform=linux/amd64 -f ops/Dockerfile -t salesiq-etl:dev .
  ```

## Misc
- **Stop and remove container by name**: docker stop salesiq-etl && docker rm salesiq-etl
- **Force remove even if running**: docker rm -f salesiq-etl

---

## Expected layout
```
repo-root/
├── app.py
├── main.py
├── README.md
├── requirements.txt
├── salesiq_etl/...
├── keyfile/etlsa_key.json 
├── .env
├── .dockerignore
└── ops/
    ├── Dockerfile
    ├── docker-compose.yml
    ├── Makefile
    └── README-ops.md   <- this file
```
