#!/usr/bin/env python3
"""Export enriched job records into Hugging Face-friendly JSONL shards."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_LOCAL_DSN = "postgresql://postgres@localhost:5432/jbot_local"
DEFAULT_SCHEMA = "training"


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return

    for line in file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def prompt_text(label: str, default: str | None = None, required: bool = True) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        if not required:
            return ""
        print("Value is required.")


def prompt_int(label: str, default: int, minimum: int = 1) -> int:
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            print("Enter a whole number.")
            continue
        if value < minimum:
            print(f"Enter a value >= {minimum}.")
            continue
        return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export job enrichment labels to JSONL shards")
    parser.add_argument("--local-dsn", help="Local Postgres DSN")
    parser.add_argument("--provider", help="Provider label")
    parser.add_argument("--model", help="Model label")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema where enrichment tables live")
    parser.add_argument("--output-dir", default="exports", help="Directory for JSONL output")
    parser.add_argument("--batch-size", type=int, default=10000, help="Rows per JSONL file")
    return parser.parse_args()


def sanitize_slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    cleaned = "-".join(filter(None, cleaned.split("-")))
    return cleaned or "unknown"


def export_jsonl(
    local_dsn: str,
    schema: str,
    provider: str,
    model: str,
    output_dir: Path,
    batch_size: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    provider_slug = sanitize_slug(provider)
    model_slug = sanitize_slug(model)

    query = f"""
        SELECT
            j.id,
            j.title,
            j.company,
            j.description,
            e.role_priorities,
            e.win_keys,
            e.reasoning,
            e.provider,
            e.model,
            e.updated_at
        FROM {schema}.job_enrichments e
        JOIN {schema}.job_listings_cache j
          ON j.id = e.job_id
        WHERE e.provider = %s
          AND e.model = %s
        ORDER BY j.id
    """

    with psycopg.connect(local_dsn, row_factory=dict_row) as conn:
        with conn.cursor(name="export_cursor") as cur:
            cur.execute(query, (provider, model))

            file_index = 0
            total_rows = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                file_index += 1
                file_name = (
                    f"hf_{provider_slug}_{model_slug}_part_{file_index:05d}.jsonl"
                )
                file_path = output_dir / file_name

                with file_path.open("w", encoding="utf-8") as f:
                    for row in rows:
                        target = {
                            "role_priorities": row["role_priorities"],
                            "win_keys": row["win_keys"],
                            "reasoning": row["reasoning"],
                        }
                        user_content = (
                            f"Title: {row['title'] or ''}\n"
                            f"Company: {row['company'] or ''}\n"
                            "Description:\n"
                            f"{row['description'] or ''}"
                        )

                        record = {
                            "id": row["id"],
                            "provider": row["provider"],
                            "model": row["model"],
                            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                            "messages": [
                                {
                                    "role": "system",
                                    "content": (
                                        "Extract the bottom line of a job description, "
                                        "list 3-5 keys to win the job, and explain your reasoning."
                                    ),
                                },
                                {"role": "user", "content": user_content},
                                {"role": "assistant", "content": json.dumps(target, ensure_ascii=False)},
                            ],
                            "target": target,
                        }
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")

                total_rows += len(rows)
                print(f"Wrote {len(rows):,} rows -> {file_path}")

    print(f"\nExport complete. Total rows: {total_rows:,}")


def main() -> None:
    load_env_file(Path(__file__).with_name(".env.local"))
    args = parse_args()

    local_dsn = args.local_dsn or prompt_text(
        "Local Postgres DSN",
        os.environ.get("LOCAL_POSTGRES_DSN", DEFAULT_LOCAL_DSN),
    )
    provider = args.provider or prompt_text("Provider label")
    model = args.model or prompt_text("Model label")
    batch_size = args.batch_size or prompt_int("Rows per output file", 10000)

    export_jsonl(
        local_dsn=local_dsn,
        schema=args.schema,
        provider=provider,
        model=model,
        output_dir=Path(args.output_dir),
        batch_size=batch_size,
    )


if __name__ == "__main__":
    main()
