# Docker Run Guide

## Prerequisite

`pipeline.yaml` must exist in the project root.

If you do not have one yet:

```bash
cp pipeline.yaml.example pipeline.yaml
```

Then update the MinIO and database-related values in `pipeline.yaml`.

## Build

```bash
docker build -t rockcapture .
```

## Run With Docker

```bash
docker run --rm \
  -p 8000:8000 \
  -v ${PWD}/pipeline.yaml:/app/pipeline.yaml:ro \
  -v ${PWD}/logs:/app/logs \
  -v ${PWD}/tmp:/app/tmp \
  --name rockcapture \
  rockcapture
```

On Windows PowerShell, `${PWD}` works as the current directory path.

## Run With Docker Compose

```bash
docker compose up --build
```

The compose file mounts:

- `./pipeline.yaml` to `/app/pipeline.yaml`
- `./logs` to `/app/logs`
- `./tmp` to `/app/tmp`

## Endpoints

- API: `http://localhost:8000`
- Swagger UI: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`

## Notes

- The image includes Java 17 because PySpark requires a JVM.
- JDBC driver JAR files under `drivers/` are copied into the image.
- `pipeline.yaml` is excluded from the Docker build context to avoid baking secrets into the image.
