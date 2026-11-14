#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .env.local ]]; then
  set -a
  source ./.env.local
  set +a
fi

container_name="${LOCAL_POSTGRES_CONTAINER:-jbot-local-pg}"

docker compose up -d postgres

echo "Waiting for ${container_name} to become ready..."
until docker exec "${container_name}" pg_isready -U "${LOCAL_POSTGRES_USER:-postgres}" -d "${LOCAL_POSTGRES_DB:-jbot_local}" >/dev/null 2>&1; do
  echo "  still waiting..."
done

echo "Local Postgres is ready."
pg_user="${LOCAL_POSTGRES_USER:-postgres}" pg_port="${LOCAL_POSTGRES_PORT:-5432}" pg_db="${LOCAL_POSTGRES_DB:-jbot_local}"
echo "DSN: ${LOCAL_POSTGRES_DSN:-postgresql://${pg_user}@localhost:${pg_port}/${pg_db}}"
echo "The DSN carries no password: add 'localhost:${pg_port}:${pg_db}:${pg_user}:<password>' to ~/.pgpass (chmod 600)."
