#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose_lt2.yaml"

# 3. Prepare host directory permissions
mkdir -p ./sia-config ./db/data/sqlite3 ./db/data/solr
touch   ./db/data/sqlite3/pipeline_jobs.db

chmod 750 ./sia-config
chmod 600 ./sia-config/api_keys.json 2>/dev/null || true

# 4. Initialize Solr storage and config (one-time)
docker compose -f "$COMPOSE_FILE" up -d zoo solr

docker compose -f "$COMPOSE_FILE" exec solr bin/solr zk upconfig \
  -z zoo:2181 -n sia_config \
  -d /opt/solr/server/solr/configsets/sia_config

docker compose -f "$COMPOSE_FILE" exec solr bin/solr zk ls /configs -z zoo:2181

# 5. Build and start services
docker compose -f "$COMPOSE_FILE" up -d --build
