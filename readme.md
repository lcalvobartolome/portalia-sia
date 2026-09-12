# ALIA-SIA

Intelligence and Analysis System for Public Procurement and Aid (from Spanish, *"Sistema de Inteligencia y Análisis de Contratación y Ayudas Públicas"*).

- [ALIA-SIA](#alia-sia)
  - [Documentation](#documentation)
  - [Services](#services)
  - [Instructions for deployment](#instructions-for-deployment)
    - [1. Create env file with the following structure](#1-create-env-file-with-the-following-structure)
    - [2. Point the compose files at your corpus data (and, optionally, a GPU)](#2-point-the-compose-files-at-your-corpus-data-and-optionally-a-gpu)
    - [3. Prepare host directory permissions](#3-prepare-host-directory-permissions)
    - [4. Initialize Solr storage and config (one-time)](#4-initialize-solr-storage-and-config-one-time)
    - [5. Build and start services](#5-build-and-start-services)
    - [6. Generate an API key](#6-generate-an-api-key)
  - [Docker → Podman deployment (moving pre-built images to another machine)](#docker--podman-deployment-moving-pre-built-images-to-another-machine)
    - [1. On the source machine: build and export the images](#1-on-the-source-machine-build-and-export-the-images)
    - [2. Copy what the target machine needs](#2-copy-what-the-target-machine-needs)
    - [3. Load the images and start the stack](#3-load-the-images-and-start-the-stack)
  - [API Authentication](#api-authentication)
    - [Configuration](#configuration)
    - [API Key Management](#api-key-management)
      - [Generate a new API key](#generate-a-new-api-key)
      - [List all API keys](#list-all-api-keys)
      - [Revoke an API key](#revoke-an-api-key)
      - [Delete an API key](#delete-an-api-key)
    - [Using API Keys](#using-api-keys)
      - [With Swagger UI](#with-swagger-ui)
      - [With curl](#with-curl)
    - [Public Endpoints (no authentication required)](#public-endpoints-no-authentication-required)
  - [Commands](#commands)
    - [To index a corpus](#to-index-a-corpus)
    - [To launch the extract pipeline](#to-launch-the-extract-pipeline)

## Documentation

- **Swagger UI**: `http://<host>:10083/docs`
- **ReDoc**: `http://<host>:10083/redoc`
- **OpenAPI JSON**: `http://<host>:10083/openapi.json`

## Services

| Service | Port | Description |
| --------- | ------ | ------------- |
| sia-core-api | 10083 | Main REST API |
| solr | 10085 | Apache Solr search engine |
| zoo | 10086/10087 | Zookeeper for Solr Cloud |

## Instructions for deployment

### 1. Create env file with the following structure

Create a `.env` file in the project root:

```
# Master key for API key management (admin operations)
SIA_MASTER_KEY=your-secure-master-key-here

# CORS allowed origins (comma-separated). Use "*" for development only.
CORS_ORIGINS=http://<host>:3000,https://your-frontend.com

# GitHub token to clone private pipeline repository during Docker build
GITHUB_TOKEN=your-github-token-here

# UID/GID the sia-core-api container runs as. It must be able to write the bind-mounted host dirs (./sia-config, ./db/data/sqlite3) and to read the external corpus data dirs set below (SIA_DATA_DIR, SIA_BDNS_DATA_DIR), mounted at /mnt/data_place and /mnt/data_bdns.
# If those dirs belong to your user, set these to the output of `id -u` / `id -g` and rebuild the image.
APP_UID=1000
APP_GID=1000

# Optional: pin the pipeline dependency to an immutable commit for reproducible builds
# Defaults to "main" when unset.
# PIPELINE_REF=<full-commit-sha>

# Host paths for the corpus data mounted into sia-core-api
SIA_DATA_DIR=/path/to/place/data
SIA_BDNS_DATA_DIR=/path/to/bdns/data
```

### 2. Point the compose files at your corpus data (and, optionally, a GPU)

`docker-compose.yaml` and `docker-compose.podman.yaml` both read `SIA_DATA_DIR` /
`SIA_BDNS_DATA_DIR` from `.env` and mount them read-write at `/mnt/data_place`
and `/mnt/data_bdns` respectively — set them in step 1 above, no need to edit
either compose file for this. The container-side targets must match
`path_source` in the `[place-config]` / `[bdns-config]` sections of
`sia-config/config.cf`. Only the host-side paths change per environment, via
`.env`.

If the machine has a GPU, uncomment the `reservations` block under
`sia-core-api.deploy.resources` in `docker-compose.yaml`:

```
    deploy:
      resources:
        limits:
          memory: 100GB
        reservations:
          devices:
            - driver: nvidia
              device_ids: ["2"]
              capabilities: [gpu]
```

Under Podman, use CDI instead (see the commented-out example in
`docker-compose.podman.yaml`):

```
        reservations:
          devices:
            - nvidia.com/gpu=all
```

### 3. Prepare host directory permissions

`sia-core-api` runs as an unprivileged user (`APP_UID:APP_GID`). Set `APP_UID`/`APP_GID`
in `.env` to **your own** `id -u`/`id -g` (the default in step 1 is just a placeholder) —
that way, when you create these paths yourself below, you already own them and none of
this needs `sudo`:

```
mkdir -p ./sia-config ./db/data/sqlite3
touch   ./db/data/sqlite3/pipeline_jobs.db

# Restrict access to other users on the host (api_keys.json holds hashed API keys)
chmod 750 ./sia-config
chmod 600 ./sia-config/api_keys.json 2>/dev/null || true
```

The `SIA_DATA_DIR` / `SIA_BDNS_DATA_DIR` directories from step 1 are external to this repo and usually managed separately — just confirm the `APP_UID:APP_GID` user can read them (e.g. `stat <path>`); don't `chown` them as part of this deployment unless you also own that data.

### 4. Initialize Solr storage and config (one-time)

```
# 1) Solr data dir must be owned by the solr user (UID 8983) inside the container
mkdir -p ./db/data/solr
sudo chown -R 8983:8983 ./db/data/solr

# 2) Bring up only Zookeeper + Solr first
docker compose up -d zoo solr

# 3) Upload the `sia_config` configset to Zookeeper.
docker compose exec solr bin/solr zk upconfig \
  -z zoo:2181 -n sia_config \
  -d /opt/solr/server/solr/configsets/sia_config

# (verify)
docker compose exec solr bin/solr zk ls /configs -z zoo:2181
```

### 5. Build and start services

```
docker compose up -d --build
```

To follow the logs:

```
docker compose logs -f sia-core-api
```

### 6. Generate an API key

Once the API is running, use the master key to generate an API key for regular access; see [API Authentication](#api-authentication) below.

## Docker → Podman deployment (moving pre-built images to another machine)

Use this when you build the images once (with Docker) and then need to run
the stack on a different machine that only has Podman, without rebuilding
anything there (e.g. no internet access, no `GITHUB_TOKEN`, no build tools).

A ready-to-use `docker-compose.podman.yaml` is kept at the project root next
to `docker-compose.yaml` specifically for this. The two files build the exact
same two images with the exact same tags by default (`sia-core-api:latest`,
`sia-solr:9.1.1`) and read the same `.env` (`SIA_DATA_DIR`, `SIA_BDNS_DATA_DIR`,
`APP_UID`/`APP_GID`, etc.); override `SIA_CORE_API_IMAGE` / `SOLR_IMAGE` /
`ZOOKEEPER_IMAGE` in `.env` instead if the target machine should use
differently-tagged precompiled images.

### 1. On the source machine: build and export the images

`zookeeper` is pulled from Docker Hub rather than built, so if the target
machine has internet access Podman will just pull it itself and you can drop it
from the command below. Include it only if the target machine is offline /
air-gapped:

```
docker compose build
docker save -o sia-images.tar \
  sia-core-api:latest sia-solr:9.1.1 \
  zookeeper@sha256:4c6f15fbd5491a3e01b0108c046891125553329a4956848ba3014cedff5386ee
```

### 2. Copy what the target machine needs

- `sia-images.tar`
- The whole project folder (needed for the bind-mounted config/data:
  `sia-config/`, `solr-config/`, `db/`, `docker-compose.podman.yaml`, `.env`)

### 3. Load the images and start the stack

```
podman load -i sia-images.tar
podman compose -f docker-compose.podman.yaml up -d
```

Podman finds `sia-core-api:latest` and `sia-solr:9.1.1`
already loaded and starts the containers directly.

> On the target machine, run the one-time Solr initialization from
> [4. Initialize Solr storage and config (one-time)](#4-initialize-solr-storage-and-config-one-time)
> (using `podman compose -f docker-compose.podman.yaml ...`) and apply the host
> directory permissions from
> [3. Prepare host directory permissions](#3-prepare-host-directory-permissions).

## API Authentication

The API uses a two-tier authentication system:

1. **Master Key**: For administrative operations (generating/managing API keys)
2. **API Keys**: For regular API access (all other endpoints)

### Configuration

Set the following environment variables (in `.env` file or docker-compose):

```
SIA_MASTER_KEY=your-secure-master-key-here
CORS_ORIGINS=http://<host>:3000,https://your-frontend.com
```

### API Key Management

Use the master key to manage API keys through the admin endpoints.

#### Generate a new API key

```
curl -X POST "http://<host>:10083/admin/api-keys" \
  -H "X-API-Key: your-master-key" \
  -H "Content-Type: application/json" \
  -d '{"name": "frontend-production"}'
```

Response:

```
{
  "key_id": "a1b2c3d4",
  "name": "frontend-production",
  "api_key": "abc123...xyz789",  // Save this! Only shown once
  "created_at": "2026-02-04T12:00:00Z"
}
```

#### List all API keys

```
curl -X GET "http://<host>:10083/admin/api-keys" \
  -H "X-API-Key: your-master-key"
```

#### Revoke an API key

```
curl -X POST "http://<host>:10083/admin/api-keys/{key_id}/revoke" \
  -H "X-API-Key: your-master-key"
```

#### Delete an API key

```
curl -X DELETE "http://<host>:10083/admin/api-keys/{key_id}" \
  -H "X-API-Key: your-master-key"
```

### Using API Keys

#### With Swagger UI

1. Open Swagger: `http://<host>:10083/docs`
2. Click the **"Authorize"** button (🔓 lock icon, top right)
3. Enter your API key in the `X-API-Key` field
4. Click **"Authorize"** then **"Close"**
5. Now all requests from Swagger will include the API key automatically

#### With curl

```
curl -X GET "http://<host>:10083/api/documents/search?query=test" \
  -H "X-API-Key: your-api-key"
```

### Public Endpoints (no authentication required)

- `GET /` - API info
- `GET /health` - Health check
- `GET /docs` - Swagger UI
- `GET /redoc` - ReDoc

## Commands

### To index a corpus

```
curl -X 'POST' \
  'http://<host>:<port>/processing/corpora' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "corpus_name": "<corpus_name>"
}'
```

### To launch the extract pipeline

```
curl -X 'POST' \
  'http://<host>:<port>/processing/alia-pipeline/extract' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "base_dir": "<data_dir>",
  "tipo": "<tipo>",
  "calculate_on": "<field_name>",
  "llm_model_gen": "<ollama_model>",
  "embed_model": "<huggingface_embed_model>",
  "file_workers": <num_file_workers>,
  "row_workers": <num_row_workers>,
  "semantic_threshold": <threshold>,
  "mallet": "<path_to_mallet>",
  "ollama_host": "<ollama_host_url>"
}'
```
