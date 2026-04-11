#!/usr/bin/env python3
"""Sync source job listings into the local Postgres cache without running LLM enrichment."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from run_job_enrichment import (
    DEFAULT_LOCAL_DSN,
    DEFAULT_SCHEMA,
    DEFAULT_SOURCE_DSN,
    connect_or_explain,
    ensure_tables,
    load_env_file,
    local_cache_count,
    sync_source_to_local,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync source jobs into local Postgres")
    parser.add_argument("--local-dsn", help="Local Postgres DSN")
    parser.add_argument("--source-dsn", help="Source Postgres DSN")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Local schema for cache/output tables")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for source-to-local sync")
    parser.add_argument("--force", action="store_true", help="Sync even if the local cache is already populated")
    return parser.parse_args()


def main() -> None:
    load_env_file(Path(__file__).with_name(".env.local"))
    args = parse_args()

    local_dsn = args.local_dsn or os.environ.get("LOCAL_POSTGRES_DSN", DEFAULT_LOCAL_DSN)
    source_dsn = args.source_dsn or os.environ.get("SOURCE_POSTGRES_DSN", DEFAULT_SOURCE_DSN)

    conn = connect_or_explain(local_dsn)
    try:
      ensure_tables(conn, args.schema)
      existing_rows = local_cache_count(conn, args.schema)
      if existing_rows > 0 and not args.force:
          print(f"Local cache already contains {existing_rows:,} rows. Skipping sync.")
          return

      sync_source_to_local(conn, source_dsn, args.schema, args.batch_size)
    finally:
      conn.close()


if __name__ == "__main__":
    main()