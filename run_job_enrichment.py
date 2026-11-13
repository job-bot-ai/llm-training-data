#!/usr/bin/env python3
"""Generate LLM training labels for job descriptions and store them in Postgres.

Workflow:
1. Ask for local Postgres DSN (or provide setup guidance if not installed).
2. Ensure required local tables exist.
3. If local cache table is empty, copy source jobs from external Postgres.
4. Ask for provider/model/CLI command template, pace, and number of jobs to process.
5. Claim a batch of jobs safely (supports concurrent workers).
6. Call LLM CLI per job, parse strict JSON output, and upsert enrichment records.
7. Resume safely at any time.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

DEFAULT_SOURCE_DSN = (
    "postgresql://postgres@pmc-us-main.job-bot.ai:5432/"
    "jbot-pcm?sslmode=verify-full&sslrootcert=/etc/ssl/postgres/root.crt"
)
DEFAULT_LOCAL_DSN = "postgresql://postgres@localhost:5432/jbot_local"
DEFAULT_SCHEMA = "training"
DEFAULT_OMNIROUTE_BASE_URL = "http://127.0.0.1:20128/v1"
OMNIROUTE_SENTINEL_CLI = "__omniroute_native__"
PROVIDER_RPM_SUGGESTIONS = {
    "anthropic": 8.0,
    "google": 12.0,
    "gemini": 12.0,
    "openai": 20.0,
    "omniroute": 20.0,
}


@dataclass
class RunConfig:
    local_dsn: str
    source_dsn: str
    provider: str
    model: str
    cli_command_template: str
    omniroute_base_url: str | None
    omniroute_api_key: str | None
    max_jobs: int
    claim_batch_size: int
    requests_per_minute: float
    claim_ttl_seconds: int
    sync_batch_size: int


def provider_uses_omniroute(provider: str) -> bool:
    return provider.strip().lower() in {"omniroute", "omni-route", "omni route"}


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return

    for line in file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def suggest_requests_per_minute(provider: str, cli_command_template: str) -> float:
    provider_key = provider.strip().lower()
    cli_key = cli_command_template.strip().lower()

    for key, rpm in PROVIDER_RPM_SUGGESTIONS.items():
        if key in provider_key or key in cli_key:
            return rpm

    return 10.0


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


def prompt_float(label: str, default: float, minimum: float = 0.0) -> float:
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = float(raw)
        except ValueError:
            print("Enter a number.")
            continue
        if value < minimum:
            print(f"Enter a value >= {minimum}.")
            continue
        return value


def prompt_yes_no(label: str, default_yes: bool = True) -> bool:
    suffix = "Y/n" if default_yes else "y/N"
    while True:
        raw = input(f"{label} [{suffix}]: ").strip().lower()
        if not raw:
            return default_yes
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please answer y or n.")


def connect_or_explain(local_dsn: str) -> psycopg.Connection:
    try:
        return psycopg.connect(local_dsn, autocommit=False)
    except Exception as exc:
        print(f"\nCould not connect to local Postgres using DSN: {local_dsn}")
        print(f"Connection error: {exc}\n")
        wants_help = prompt_yes_no("Would you like install instructions for local Postgres?", True)
        if wants_help:
            print("\nQuick local Postgres via Docker:")
            print(
                "docker run --name jbot-local-pg -e POSTGRES_PASSWORD=postgres "
                "-e POSTGRES_DB=jbot_local -p 5432:5432 -d postgres:16"
            )
            print("Then retry this script with:")
            print(f"  {DEFAULT_LOCAL_DSN}\n")
        raise


def ensure_tables(conn: psycopg.Connection, schema: str) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.job_listings_cache (
        id TEXT PRIMARY KEY,
        title TEXT,
        company TEXT,
        description TEXT NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {schema}.job_enrichments (
        job_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        role_priorities TEXT NOT NULL,
        win_keys JSONB NOT NULL,
        reasoning TEXT NOT NULL,
        raw_response JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (job_id, provider, model),
        FOREIGN KEY (job_id) REFERENCES {schema}.job_listings_cache(id)
    );

    CREATE TABLE IF NOT EXISTS {schema}.job_enrichment_claims (
        job_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        worker_id TEXT NOT NULL,
        status TEXT NOT NULL,
        claim_expires_at TIMESTAMPTZ NOT NULL,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (job_id, provider, model)
    );

    CREATE INDEX IF NOT EXISTS idx_job_enrichment_claims_status
        ON {schema}.job_enrichment_claims(status, claim_expires_at);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def local_cache_count(conn: psycopg.Connection, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS total FROM {schema}.job_listings_cache")
        row = cur.fetchone()
        return int(row["total"])


def sync_source_to_local(
    local_conn: psycopg.Connection,
    source_dsn: str,
    schema: str,
    batch_size: int,
) -> None:
    print("Local cache is empty. Pulling source jobs from external Postgres...")
    inserted = 0

    source_query = """
        SELECT
            id::text AS id,
            title,
            company,
            description
        FROM orca2.job_listings
        WHERE description IS NOT NULL
    """

    upsert_sql = f"""
        INSERT INTO {schema}.job_listings_cache (id, title, company, description, synced_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET title = EXCLUDED.title,
            company = EXCLUDED.company,
            description = EXCLUDED.description,
            synced_at = NOW()
    """

    with psycopg.connect(source_dsn, autocommit=False, row_factory=dict_row) as source_conn:
        with source_conn.cursor(name="job_stream") as source_cur:
            source_cur.execute(source_query)
            while True:
                rows = source_cur.fetchmany(batch_size)
                if not rows:
                    break
                payload = [(r["id"], r["title"], r["company"], r["description"]) for r in rows]
                with local_conn.cursor() as local_cur:
                    local_cur.executemany(upsert_sql, payload)
                local_conn.commit()
                inserted += len(payload)
                print(f"  synced {inserted:,} rows")

    print(f"Sync complete. Cached {inserted:,} source rows.")


def get_peer_enrichments(
    conn: psycopg.Connection,
    schema: str,
    job_id: str,
    provider: str,
    model: str,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT provider, model, role_priorities, win_keys, reasoning
            FROM {schema}.job_enrichments
            WHERE job_id = %s
              AND NOT (provider = %s AND model = %s)
            ORDER BY provider, model
            """,
            (job_id, provider, model),
        )
        return cur.fetchall()


def build_prompt(row: dict[str, Any], peer_enrichments: list[dict[str, Any]]) -> str:
    description = row.get("description") or ""
    title = row.get("title") or "Unknown Title"
    company = row.get("company") or "Unknown Company"

    instruction = {
        "task": [
            "Summarize the role priorities and what the company is optimizing for.",
            "Generate 3 to 5 concrete keys on how a candidate can win this job.",
            "Document reasoning tied directly to the job description details.",
        ],
        "output_format": {
            "role_priorities": "string, 3-6 sentences",
            "win_keys": ["string", "string", "string"],
            "reasoning": "string, concise but specific evidence-based rationale",
        },
        "rules": [
            "Return strict JSON only. No markdown. No backticks.",
            "Do not hallucinate company facts beyond the provided job description.",
            "win_keys must contain 3 to 5 items.",
            "If peer outputs are provided, use them only as reference points to improve precision and coverage.",
            "Do not copy peer wording mechanically. Produce your own best answer from the job description.",
        ],
    }

    peer_block = "None"
    if peer_enrichments:
        peer_payload = []
        for peer in peer_enrichments:
            peer_payload.append(
                {
                    "provider": peer["provider"],
                    "model": peer["model"],
                    "role_priorities": peer["role_priorities"],
                    "win_keys": peer["win_keys"],
                    "reasoning": peer["reasoning"],
                }
            )
        peer_block = json.dumps(peer_payload, ensure_ascii=True)

    return (
        "You are creating training labels for job-understanding tasks.\n"
        f"Instruction JSON:\n{json.dumps(instruction, ensure_ascii=True)}\n\n"
        f"Job title: {title}\n"
        f"Company: {company}\n"
        "Job description:\n"
        f"{description}\n\n"
        "Peer outputs from other providers/models for this same job:\n"
        f"{peer_block}\n"
    )


def extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()

    # Codex --json mode emits JSONL events. Extract the final assistant message.
    if "\n" in text:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        json_lines: list[dict[str, Any]] = []
        for ln in lines:
            try:
                parsed_ln = json.loads(ln)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed_ln, dict):
                json_lines.append(parsed_ln)

        if json_lines:
            for event in reversed(json_lines):
                item = event.get("item")
                if isinstance(item, dict) and item.get("type") == "agent_message":
                    text_payload = item.get("text")
                    if isinstance(text_payload, str):
                        try:
                            parsed_payload = json.loads(text_payload)
                            if isinstance(parsed_payload, dict):
                                return parsed_payload
                        except json.JSONDecodeError:
                            pass

            # Claude --output-format json returns a top-level result object.
            top = json_lines[-1]
            if top.get("type") == "result":
                if bool(top.get("is_error")):
                    raise ValueError(f"Claude CLI returned error result: {top.get('result', '')}")
                result_payload = top.get("result")
                if isinstance(result_payload, str):
                    try:
                        parsed_payload = json.loads(result_payload)
                        if isinstance(parsed_payload, dict):
                            return parsed_payload
                    except json.JSONDecodeError:
                        pass

    # Fast path: output is pure JSON.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            if parsed.get("type") == "result" and "result" in parsed:
                if bool(parsed.get("is_error")):
                    raise ValueError(f"Claude CLI returned error result: {parsed.get('result', '')}")
                payload = parsed.get("result")
                if isinstance(payload, str):
                    parsed_payload = json.loads(payload)
                    if isinstance(parsed_payload, dict):
                        return parsed_payload
            return parsed
    except json.JSONDecodeError:
        pass

    # Fallback: scan for a balanced JSON object.
    start = text.find("{")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            ch = text[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start : i + 1]
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict):
                            return parsed
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)

    raise ValueError("Could not parse JSON object from LLM output")


def run_cli(cli_template: str, prompt: str) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".txt", delete=False) as f:
        prompt_path = Path(f.name)
        f.write(prompt)

    try:
        command = cli_template.replace("{prompt_file}", shlex.quote(str(prompt_path)))
        completed = subprocess.run(
            command,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"CLI failed (exit={completed.returncode})\n"
                f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        try:
            return extract_json_object(stdout)
        except Exception as exc:
            raise RuntimeError(
                "Could not parse model response into JSON label payload\n"
                f"parse_error={exc}\n"
                f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            ) from exc
    finally:
        try:
            prompt_path.unlink(missing_ok=True)
        except OSError:
            pass


def run_omniroute(
    model: str,
    prompt: str,
    base_url: str,
    api_key: str | None,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    if base.endswith("/chat/completions"):
        endpoint = base
    elif base.endswith("/v1"):
        endpoint = f"{base}/chat/completions"
    else:
        endpoint = f"{base}/v1/chat/completions"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return strict JSON only."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(endpoint, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            raw_body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"OmniRoute HTTP error (status={exc.code})\nURL: {endpoint}\nResponse:\n{body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OmniRoute request failed for {endpoint}: {exc}") from exc

    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"OmniRoute returned non-JSON response:\n{raw_body}") from exc

    choices = parsed.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError(f"OmniRoute response missing choices:\n{raw_body}")

    first = choices[0]
    if not isinstance(first, dict):
        raise RuntimeError(f"OmniRoute response has invalid choices format:\n{raw_body}")

    message = first.get("message")
    if not isinstance(message, dict):
        raise RuntimeError(f"OmniRoute response missing message block:\n{raw_body}")

    content = message.get("content")
    if not isinstance(content, str):
        raise RuntimeError(f"OmniRoute message missing text content:\n{raw_body}")

    return extract_json_object(content)


def validate_label(payload: dict[str, Any]) -> tuple[str, list[str], str]:
    role_priorities = str(payload.get("role_priorities", "")).strip()
    reasoning = str(payload.get("reasoning", "")).strip()
    win_keys = payload.get("win_keys")

    if not role_priorities:
        raise ValueError("Missing role_priorities")
    if not reasoning:
        raise ValueError("Missing reasoning")
    if not isinstance(win_keys, list):
        raise ValueError("win_keys must be a JSON array")

    normalized = [str(x).strip() for x in win_keys if str(x).strip()]
    if not (3 <= len(normalized) <= 5):
        raise ValueError("win_keys must contain 3 to 5 non-empty items")

    return role_priorities, normalized, reasoning


def claim_jobs(
    conn: psycopg.Connection,
    schema: str,
    provider: str,
    model: str,
    worker_id: str,
    claim_ttl_seconds: int,
    claim_batch_size: int,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            WITH candidates AS (
                SELECT jl.id
                FROM {schema}.job_listings_cache jl
                LEFT JOIN {schema}.job_enrichments je
                    ON je.job_id = jl.id
                   AND je.provider = %s
                   AND je.model = %s
                LEFT JOIN {schema}.job_enrichment_claims c
                    ON c.job_id = jl.id
                   AND c.provider = %s
                   AND c.model = %s
                WHERE je.job_id IS NULL
                  AND (
                        c.job_id IS NULL
                        OR c.status = 'failed'
                        OR c.claim_expires_at < NOW()
                  )
                ORDER BY jl.id
                LIMIT %s
            )
            INSERT INTO {schema}.job_enrichment_claims (
                job_id, provider, model, worker_id, status, claim_expires_at, updated_at
            )
            SELECT
                c.id,
                %s,
                %s,
                %s,
                'claimed',
                NOW() + (%s * INTERVAL '1 second'),
                NOW()
            FROM candidates c
            ON CONFLICT (job_id, provider, model)
            DO UPDATE SET
                worker_id = EXCLUDED.worker_id,
                status = 'claimed',
                claim_expires_at = EXCLUDED.claim_expires_at,
                updated_at = NOW()
            WHERE {schema}.job_enrichment_claims.status = 'failed'
               OR {schema}.job_enrichment_claims.claim_expires_at < NOW()
            RETURNING job_id
            """,
            (
                provider,
                model,
                provider,
                model,
                claim_batch_size,
                provider,
                model,
                worker_id,
                claim_ttl_seconds,
            ),
        )
        claimed = [r["job_id"] for r in cur.fetchall()]

        if not claimed:
            conn.commit()
            return []

        cur.execute(
            f"""
            SELECT id, title, company, description
            FROM {schema}.job_listings_cache
            WHERE id = ANY(%s)
            ORDER BY id
            """,
            (claimed,),
        )
        rows = cur.fetchall()
    conn.commit()
    return rows


def save_success(
    conn: psycopg.Connection,
    schema: str,
    row: dict[str, Any],
    provider: str,
    model: str,
    role_priorities: str,
    win_keys: list[str],
    reasoning: str,
    raw: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.job_enrichments (
                job_id, provider, model, role_priorities, win_keys, reasoning, raw_response, updated_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, NOW())
            ON CONFLICT (job_id, provider, model)
            DO UPDATE SET
                role_priorities = EXCLUDED.role_priorities,
                win_keys = EXCLUDED.win_keys,
                reasoning = EXCLUDED.reasoning,
                raw_response = EXCLUDED.raw_response,
                updated_at = NOW()
            """,
            (
                row["id"],
                provider,
                model,
                role_priorities,
                json.dumps(win_keys, ensure_ascii=True),
                reasoning,
                json.dumps(raw, ensure_ascii=True),
            ),
        )
        cur.execute(
            f"""
            UPDATE {schema}.job_enrichment_claims
            SET status = 'done',
                last_error = NULL,
                updated_at = NOW(),
                claim_expires_at = NOW() + INTERVAL '365 days'
            WHERE job_id = %s AND provider = %s AND model = %s
            """,
            (row["id"], provider, model),
        )
    conn.commit()


def save_failure(
    conn: psycopg.Connection,
    schema: str,
    row: dict[str, Any],
    provider: str,
    model: str,
    err: Exception,
) -> None:
    error_text = str(err)[:5000]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {schema}.job_enrichment_claims
            SET status = 'failed',
                last_error = %s,
                updated_at = NOW(),
                claim_expires_at = NOW() + INTERVAL '5 minutes'
            WHERE job_id = %s AND provider = %s AND model = %s
            """,
            (error_text, row["id"], provider, model),
        )
    conn.commit()


def gather_config(args: argparse.Namespace) -> RunConfig:
    env_local_dsn = os.environ.get("LOCAL_POSTGRES_DSN", DEFAULT_LOCAL_DSN)
    env_source_dsn = os.environ.get("SOURCE_POSTGRES_DSN", DEFAULT_SOURCE_DSN)

    local_dsn = args.local_dsn or prompt_text("Local Postgres DSN", env_local_dsn)
    source_dsn = args.source_dsn or prompt_text("Source Postgres DSN", env_source_dsn)
    provider = args.provider or prompt_text("LLM provider label (e.g., Google, OpenAI, Anthropic, OmniRoute)")
    model = args.model or prompt_text("Model label (e.g., gemini-2.5-pro)")

    omniroute_base_url: str | None = None
    omniroute_api_key: str | None = None

    if provider_uses_omniroute(provider):
        cli_command_template = OMNIROUTE_SENTINEL_CLI
        omniroute_base_url = (
            args.omniroute_base_url
            or os.environ.get("OMNIROUTE_BASE_URL")
            or DEFAULT_OMNIROUTE_BASE_URL
        )
        omniroute_api_key = (
            args.omniroute_api_key
            if args.omniroute_api_key is not None
            else os.environ.get("OMNIROUTE_API_KEY")
        )
    else:
        default_cli = "gemini --prompt \"\" < {prompt_file}"
        cli_command_template = args.cli_cmd or prompt_text(
            "CLI command template (must include {prompt_file})",
            default_cli,
        )
        if "{prompt_file}" not in cli_command_template:
            raise ValueError("CLI template must include {prompt_file}")

    suggested_rpm = suggest_requests_per_minute(provider, cli_command_template)
    max_jobs = args.max_jobs or prompt_int("How many jobs should this run process", 1000)
    claim_batch_size = args.batch_size or prompt_int("How many jobs to claim at a time", 25)
    requests_per_minute = args.rpm or prompt_float(
        "Pace: requests per minute",
        suggested_rpm,
        minimum=0.1,
    )
    claim_ttl_seconds = args.claim_ttl or prompt_int("Claim TTL in seconds", 3600)
    sync_batch_size = args.sync_batch_size or prompt_int("Source sync batch size", 5000)

    return RunConfig(
        local_dsn=local_dsn,
        source_dsn=source_dsn,
        provider=provider,
        model=model,
        cli_command_template=cli_command_template,
        omniroute_base_url=omniroute_base_url,
        omniroute_api_key=omniroute_api_key,
        max_jobs=max_jobs,
        claim_batch_size=claim_batch_size,
        requests_per_minute=requests_per_minute,
        claim_ttl_seconds=claim_ttl_seconds,
        sync_batch_size=sync_batch_size,
    )


def run(config: RunConfig, schema: str) -> None:
    worker_id = f"worker-{uuid.uuid4().hex[:12]}"
    sleep_seconds = 60.0 / config.requests_per_minute

    local_conn = connect_or_explain(config.local_dsn)
    local_conn.row_factory = dict_row

    try:
        ensure_tables(local_conn, schema)
        count = local_cache_count(local_conn, schema)

        if count == 0:
            sync_source_to_local(
                local_conn,
                config.source_dsn,
                schema,
                config.sync_batch_size,
            )
        else:
            print(f"Local cache already has {count:,} rows. Skipping source sync.")

        processed = 0
        succeeded = 0
        failed = 0

        print(
            f"\nStarting enrichment with provider={config.provider}, model={config.model}, "
            f"worker={worker_id}, target={config.max_jobs} rows"
        )

        while processed < config.max_jobs:
            remaining = config.max_jobs - processed
            claim_count = min(config.claim_batch_size, remaining)
            claimed_rows = claim_jobs(
                local_conn,
                schema,
                config.provider,
                config.model,
                worker_id,
                config.claim_ttl_seconds,
                claim_count,
            )

            if not claimed_rows:
                print("No more claimable rows found for this provider/model.")
                break

            for row in claimed_rows:
                if processed >= config.max_jobs:
                    break
                processed += 1

                try:
                    peer_enrichments = get_peer_enrichments(
                        local_conn,
                        schema,
                        row["id"],
                        config.provider,
                        config.model,
                    )
                    prompt = build_prompt(row, peer_enrichments)
                    if provider_uses_omniroute(config.provider):
                        if not config.omniroute_base_url:
                            raise ValueError("OmniRoute base URL is required")
                        raw = run_omniroute(
                            model=config.model,
                            prompt=prompt,
                            base_url=config.omniroute_base_url,
                            api_key=config.omniroute_api_key,
                        )
                    else:
                        raw = run_cli(config.cli_command_template, prompt)
                    role_priorities, win_keys, reasoning = validate_label(raw)
                    save_success(
                        local_conn,
                        schema,
                        row,
                        config.provider,
                        config.model,
                        role_priorities,
                        win_keys,
                        reasoning,
                        raw,
                    )
                    succeeded += 1
                    print(
                        f"[{processed}/{config.max_jobs}] ok job_id={row['id']} "
                        f"title={row.get('title') or 'N/A'} peers={len(peer_enrichments)}"
                    )
                except Exception as exc:
                    failed += 1
                    save_failure(local_conn, schema, row, config.provider, config.model, exc)
                    print(f"[{processed}/{config.max_jobs}] failed job_id={row['id']} error={exc}")

                time.sleep(sleep_seconds)

        print("\nRun complete")
        print(f"  Processed: {processed}")
        print(f"  Succeeded: {succeeded}")
        print(f"  Failed:    {failed}")
    finally:
        local_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate job enrichment labels with an LLM CLI")
    parser.add_argument("--local-dsn", help="Local Postgres DSN")
    parser.add_argument("--source-dsn", help="Source Postgres DSN")
    parser.add_argument("--provider", help="Provider label")
    parser.add_argument("--model", help="Model label")
    parser.add_argument("--cli-cmd", help="CLI command template with {prompt_file}")
    parser.add_argument(
        "--omniroute-base-url",
        help="OmniRoute base URL (OpenAI-compatible), e.g. http://127.0.0.1:20128/v1",
    )
    parser.add_argument(
        "--omniroute-api-key",
        help="Optional OmniRoute API key (sent as Bearer token).",
    )
    parser.add_argument("--max-jobs", type=int, help="Maximum jobs this run")
    parser.add_argument("--batch-size", type=int, help="Rows to claim per batch")
    parser.add_argument("--rpm", type=float, help="Requests per minute")
    parser.add_argument("--claim-ttl", type=int, help="Claim time-to-live seconds")
    parser.add_argument("--sync-batch-size", type=int, help="Batch size for source-to-local sync")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Local schema for cache/output tables")
    return parser.parse_args()


def main() -> None:
    load_env_file(Path(__file__).with_name(".env.local"))
    args = parse_args()
    config = gather_config(args)
    run(config, schema=args.schema)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nFatal error: {exc}")
        sys.exit(1)
