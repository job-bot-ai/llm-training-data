#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone

import psycopg

DSN = "postgresql://postgres:postgres@localhost:5432/jbot_local"


def main() -> None:
    now = datetime.now(timezone.utc).isoformat()
    print(f"=== {now} ===")

    cli_totals = {
        "gemini": 0,
        "codex": 0,
        "claude": 0,
    }

    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT provider, model, COUNT(*)
                FROM training.job_enrichments
                GROUP BY provider, model
                ORDER BY provider, model
                """
            )
            rows = cur.fetchall()
            print("enrichments:")
            if not rows:
                print("  none")
            else:
                for provider, model, count in rows:
                    print(f"  {provider}/{model}: {count}")
                    provider_l = provider.lower()
                    model_l = model.lower()
                    if "google" in provider_l or "gemini" in model_l:
                        cli_totals["gemini"] += count
                    elif "openai" in provider_l or "codex" in model_l:
                        cli_totals["codex"] += count
                    elif "anthropic" in provider_l or "claude" in model_l:
                        cli_totals["claude"] += count

            print("cli_summary:")
            print(f"  gemini: {cli_totals['gemini']}")
            print(f"  codex: {cli_totals['codex']}")
            print(f"  claude: {cli_totals['claude']}")
            print(f"  total: {sum(cli_totals.values())}")

            cur.execute(
                """
                SELECT provider, model, status, COUNT(*)
                FROM training.job_enrichment_claims
                GROUP BY provider, model, status
                ORDER BY provider, model, status
                """
            )
            claim_rows = cur.fetchall()
            print("claims:")
            if not claim_rows:
                print("  none")
            else:
                for provider, model, status, count in claim_rows:
                    print(f"  {provider}/{model} [{status}]: {count}")

    print()


if __name__ == "__main__":
    main()
