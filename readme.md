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
  - [Exploitation Services (search \& indicators)](#exploitation-services-search--indicators)
    - [Discover what a corpus supports](#discover-what-a-corpus-supports)
    - [Get document metadata](#get-document-metadata)
    - [Semantic search by text](#semantic-search-by-text)
    - [Semantic search by document(s)](#semantic-search-by-documents)
    - [Indicators (place only)](#indicators-place-only)

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

# UID/GID that sia-core-api AND solr run as. Both containers run as this
# user instead of a fixed built-in one, so every bind-mounted host dir
# (./sia-config, ./db/data/sqlite3, ./db/data/solr) just needs to be owned by
# whoever creates it — no `chown`/`sudo` step is ever required, on Mac or
# Linux. Also used to read the external corpus data dirs set below
# (SIA_DATA_DIR, SIA_BDNS_DATA_DIR), mounted at /mnt/data_place and /mnt/data_bdns.
# Set these to the output of `id -u` / `id -g` and rebuild the image.
APP_UID=1000
APP_GID=1000

# Optional: pin the pipeline dependency to an immutable commit for reproducible builds
# Defaults to "main" when unset.
# PIPELINE_REF=<full-commit-sha>

# Host paths for the corpus data mounted into sia-core-api
# Must be absolute, or start with "./" if relative — a bare "data/place" is
# parsed by Compose as a *named volume* reference (not a bind-mount path) and
# fails with "refers to undefined volume".
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

Both `sia-core-api` and `solr` run as an unprivileged user (`APP_UID:APP_GID`) instead
of any built-in container user. Set `APP_UID`/`APP_GID` in `.env` to **your own**
`id -u`/`id -g` (the default in step 1 is just a placeholder) — that way, when you
create these paths yourself below, you already own them and **none of this needs
`sudo` or a `chown` to a container-specific UID**, on Mac or Linux:

```
mkdir -p ./sia-config ./db/data/sqlite3 ./db/data/solr
touch   ./db/data/sqlite3/pipeline_jobs.db

# Restrict access to other users on the host (api_keys.json holds hashed API keys)
chmod 750 ./sia-config
chmod 600 ./sia-config/api_keys.json 2>/dev/null || true
```

The `SIA_DATA_DIR` / `SIA_BDNS_DATA_DIR` directories from step 1 are external to this repo and usually managed separately — just confirm the `APP_UID:APP_GID` user can read them (e.g. `stat <path>`); don't `chown` them as part of this deployment unless you also own that data.

> **macOS / Docker Desktop note**: if a bind-mounted dir was previously owned by a
> container's built-in UID (e.g. an old deployment that ran Solr as UID 8983), Docker
> Desktop's virtiofs bridge can leave it in a state where even a matching UID can't
> write to it. If you hit `Permission denied` on a directory that already looks
> correctly owned, `rm -rf` and `mkdir` it again as your own user rather than trying
> to `chmod` it in place.

### 4. Initialize Solr storage and config (one-time)

```
# 1) Bring up only Zookeeper + Solr first (./db/data/solr was already created
#    and owned by you in step 3 — no extra chown needed)
docker compose up -d zoo solr

# 2) Upload the `sia_config` configset to Zookeeper.
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

## Exploitation Services (search & indicators)

All endpoints below live under `/exploitation` and work against either corpus collection (`place` or `bdns`) via the `corpus_collection` path segment, unless noted otherwise. What each collection actually supports is declared per corpus in `sia-config/config.cf` (`capabilities=` key) — query the endpoint below instead of hardcoding assumptions about a collection in your client.

### Discover what a corpus supports

```
curl -X 'GET' \
  'http://<host>:<port>/exploitation/corpora/place/capabilities' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>'
```

```
{
  "success": true,
  "message": null,
  "data": {
    "corpus": "place",
    "capabilities": ["indicators", "metadata", "semantic_by_document", "semantic_by_text"],
    "secondary_id_fields": ["expediente"]
  }
}
```

The same call for `bdns` returns `"capabilities": ["metadata", "semantic_by_document", "semantic_by_text"]`
(no `indicators`, since indicators rely on procurement-specific fields — budget,
CPV, award data — that BDNS grants don't have) and `"secondary_id_fields": ["codigo_bdns"]`.
`secondary_id_fields` lists the alternate identifier(s) — besides the canonical
`id` — that a document can be looked up by in that corpus; none are hardcoded
in the API, they're declared per corpus in `sia-config/config.cf`
(`secondary_id_fields=`) and used as the `secondary_field` in
`GET .../documents` or as keys in `secondary_ids` in
`POST .../semantic/by-document` below.

### Get document metadata

```
curl -X 'GET' \
  'http://<host>:<port>/exploitation/corpora/place/documents?id=<place_doc_id>' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>'
```

Or look it up by the corpus' alternate identifier instead of `id`:

```
curl -X 'GET' \
  'http://<host>:<port>/exploitation/corpora/place/documents?secondary_field=expediente&secondary_value=2025/180' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>'
```

```
curl -X 'GET' \
  'http://<host>:<port>/exploitation/corpora/bdns/documents?secondary_field=codigo_bdns&secondary_value=<codigo_bdns_value>' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>'
```

### Semantic search by text

Same request shape for both collections — only `corpus_collection` and the
filters that make sense for that domain (CPV/procurement vs. grant-specific
metadata) change.

**PLACE example** (procurement):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/place/semantic/by-text' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "query_text": "inteligencia artificial en contratacion publica",
  "filters": {"date": "2025", "cpv": "72*"},
  "pagination": {"start": 0, "rows": 10}
}'
```

**BDNS example** (grants — no `cpv`, filter on grant-specific fields instead via `extra`):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/bdns/semantic/by-text' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "query_text": "ayudas para la transicion energetica en pymes",
  "filters": {"date": "2025", "extra": {"organo_entidad": "Industria y Energía"}},
  "pagination": {"start": 0, "rows": 10}
}'
```

### Semantic search by document(s)

Same request shape for both collections. Besides `doc_ids`, you can resolve
documents via `secondary_ids` — a dict keyed by the corpus' alternate
identifier field name (from `GET .../capabilities`), so each corpus passes
whatever field(s) it actually has instead of a one-size-fits-all name.

> **Backward compatibility**: the old `expediente` query param (on
> `GET .../documents`) and `expedientes` body field (on
> `POST .../semantic/by-document`) still work — they're deprecated aliases
> for `secondary_field='expediente'`/`secondary_value=...` and
> `secondary_ids={'expediente': [...]}` respectively, and only apply to
> corpora that declare `expediente` as a secondary id field (i.e. `place`).

**PLACE example** (by `doc_ids`):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/place/semantic/by-document' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "doc_ids": ["<place_doc_id>"],
  "filters": {"date": "2025"},
  "pagination": {"start": 0, "rows": 10}
}'
```

**PLACE example** (by `expediente`, place's secondary identifier field):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/place/semantic/by-document' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "secondary_ids": {"expediente": ["2025/180"]},
  "filters": {"date": "2025"},
  "pagination": {"start": 0, "rows": 10}
}'
```

**BDNS example** (by `doc_ids`):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/bdns/semantic/by-document' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "doc_ids": ["<bdns_doc_id>"],
  "filters": {"date": "2025"},
  "pagination": {"start": 0, "rows": 10}
}'
```

**BDNS example** (by `codigo_bdns`, bdns's secondary identifier field):

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/corpora/bdns/semantic/by-document' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "secondary_ids": {"codigo_bdns": ["<codigo_bdns_value>"]},
  "filters": {"date": "2025"},
  "pagination": {"start": 0, "rows": 10}
}'
```

For a reference document that isn't yet indexed with an embedding, the text used to compute one on the fly is read per corpus from `embedding_text_fields` in `sia-config/config.cf` (`generative_objective`/`objeto` for `place`, `descripcion` for `bdns`) — no client-side branching needed.

### Indicators (place only)

```
curl -X 'POST' \
  'http://<host>:<port>/exploitation/indicators/total-procurement' \
  -H 'accept: application/json' \
  -H 'X-API-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{
  "date_start": "2025-01-01T00:00:00Z",
  "date_end": "2026-01-01T00:00:00Z",
  "tender_type": "insiders",
  "cpv_prefixes": ["48", "72"]
}'
```

Indicators have no `corpus_collection` parameter since they only ever run against `place`. The endpoint `GET /exploitation/corpora/{corpus}/capabilities` can be checked to show indicator-related features for a given corpus.
