# Job Enrichment Pipeline

This workspace contains two scripts:

- `run_job_enrichment.py`: syncs source jobs into a local Postgres database, claims unprocessed jobs for a specific provider/model, calls either an LLM CLI or OmniRoute's OpenAI-compatible HTTP API, and stores structured labels.
- `export_hf_jsonl.py`: exports finished labels into Hugging Face-friendly JSONL shards, defaulting to 10,000 rows per file.

## What the pipeline stores

The local database uses three tables under the `training` schema:

- `job_listings_cache`: local copy of `id`, `title`, `company`, `description`
- `job_enrichments`: one row per `(job_id, provider, model)` with three target columns:
  - `role_priorities`
  - `win_keys`
  - `reasoning`
- `job_enrichment_claims`: worker claim state so multiple tools can run concurrently and resume safely

This is intentionally better than adding provider-specific columns directly to the job table, because it scales cleanly across multiple providers and models without schema churn.

## Setup

Install Python dependency:

```bash
pip install -r requirements.txt
```

Install and authenticate whichever CLI you want to use. Example command templates:

```bash
gemini --prompt "" < {prompt_file}
```

```bash
anthropic messages create --model claude-sonnet-4-20250514 --input-file {prompt_file}
```

Adjust the CLI template to match the exact command shape your installed CLI expects. The script requires `{prompt_file}` to be present in the template.

### OmniRoute (native API mode)

`run_job_enrichment.py` supports OmniRoute directly, without requiring a local CLI wrapper.

- default base URL: `http://127.0.0.1:20128/v1`
- endpoint used: `/chat/completions` (OpenAI-compatible)
- model is whatever OmniRoute exposes (pick any available model id)

Optional environment variables:

```bash
export OMNIROUTE_BASE_URL="http://127.0.0.1:20128/v1"
export OMNIROUTE_API_KEY=""
```

List available models from OmniRoute:

```bash
curl -sS http://127.0.0.1:20128/v1/models
```

Optional watchdog-managed OmniRoute worker (via `ensure_workers.py`):

- Set `OMNIROUTE_MODEL` (required to enable OmniRoute worker)
- Optional: `OMNIROUTE_BASE_URL`, `OMNIROUTE_API_KEY`, `OMNIROUTE_RPM`, `OMNIROUTE_BATCH_SIZE`, `OMNIROUTE_MAX_JOBS`

Example:

```bash
export OMNIROUTE_MODEL="openai/gpt-4.1-mini"
export OMNIROUTE_BASE_URL="http://127.0.0.1:20128/v1"
```

## Local Postgres

The script asks for a local DSN. If local Postgres is not installed, it offers a Docker-based setup path.

Default local DSN:

```text
postgresql://postgres:postgres@localhost:5432/jbot_local
```

Source data comes from:

```text
postgresql://postgres:kR7mQ2pXwN@pmc-us-main.job-bot.ai:5432/jbot-pcm?hostaddr=100.115.252.23&sslmode=verify-full&sslrootcert=/etc/ssl/postgres/root.crt
```

The `hostaddr` parameter forces traffic over the Tailscale IP while TLS hostname verification still uses `pmc-us-main.job-bot.ai`.

Source table:

- database: `jbot-pcm`
- schema: `orca2`
- table: `job_listings`
- columns copied locally: `id`, `title`, `company`, `description`

## Simplified local-first workflow

This workspace now supports a single local database as the source of truth for every CLI.

- `.env.local` stores the local DSN and the external source DSN
- `docker-compose.yml` starts a local Postgres container
- `setup_local_postgres.sh` starts the container and waits for readiness
- `sync_source_jobs.py` copies the source job listings into the local cache without running any LLM processing

If you want the CLIs to only work against one shared local store, do this once:

```bash
./setup_local_postgres.sh
python sync_source_jobs.py
```

After that, each CLI only needs to run `run_job_enrichment.py` against the local database.

## Suggested pace

Use a conservative starting pace and only increase after you confirm the CLI is stable and the provider is not rate-limiting.

- Google/Gemini CLI: start at `12` requests per minute
- OpenAI CLI/API wrapper: start at `20` requests per minute
- Anthropic CLI/API wrapper: start at `8` requests per minute
- OmniRoute (local): start at `20` requests per minute
- Unknown CLI/provider: start at `10` requests per minute

Those defaults are built into `run_job_enrichment.py`. They are intentionally conservative because each request carries a full job description and, once other providers have already labeled a row, peer outputs are also included in the prompt. That raises both token volume and the chance of provider throttling.

## Cross-provider review

When a provider/model processes a job, the script checks whether other providers/models have already produced labels for that same job. If so, those peer outputs are included in the prompt as reference material.

The prompt instructs the current model to:

- review peer outputs for coverage gaps or better phrasing
- still ground the answer in the original job description
- avoid copying peer wording mechanically

This gives you a form of iterative comparative labeling while still keeping every provider/model result separate in the database.

## Run the enrichment script

Interactive mode:

```bash
python run_job_enrichment.py
```

Example non-interactive invocation:

```bash
python run_job_enrichment.py \
  --local-dsn "postgresql://postgres:postgres@localhost:5432/jbot_local" \
  --provider "Google" \
  --model "gemini-2.5-pro" \
  --cli-cmd "gemini --prompt \"\" < {prompt_file}" \
  --max-jobs 5000 \
  --batch-size 25 \
  --rpm 12
```

OmniRoute example:

```bash
python run_job_enrichment.py \
  --local-dsn "postgresql://postgres:postgres@localhost:5432/jbot_local" \
  --provider "OmniRoute" \
  --model "openai/gpt-4.1-mini" \
  --omniroute-base-url "http://127.0.0.1:20128/v1" \
  --max-jobs 5000 \
  --batch-size 25 \
  --rpm 20
```

The first run will populate the local cache from the external source if the cache is empty.

If you already ran `sync_source_jobs.py`, the enrichment script will detect that the local cache exists and skip the source download.

## Resume behavior

You can stop and restart at any time.

- Completed rows are detected from `job_enrichments`
- In-flight work is tracked in `job_enrichment_claims`
- Expired or failed claims become eligible again
- Multiple workers can process different providers/models simultaneously

## Export Hugging Face JSONL

Run:

```bash
python export_hf_jsonl.py
```

Example:

```bash
python export_hf_jsonl.py \
  --local-dsn "postgresql://postgres:postgres@localhost:5432/jbot_local" \
  --provider "Google" \
  --model "gemini-2.5-pro" \
  --output-dir exports \
  --batch-size 10000
```

Each JSONL row includes:

- metadata: `id`, `provider`, `model`, `updated_at`
- `messages`: system/user/assistant chat format for instruction tuning
- `target`: structured JSON copy of the label

## Practical note

For 300,000 jobs, do not start with the full corpus. Run a pilot first, inspect output quality and failure rates, then scale out across providers.