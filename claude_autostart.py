#!/usr/bin/env python3
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

WORKDIR = Path('/j-bot/llm-training')
ENV_FILE = WORKDIR / '.env.local'
PYTHON = '/j-bot/llm-training/.venv/bin/python'
RUNNER = WORKDIR / 'run_job_enrichment.py'
OUT_LOG = WORKDIR / 'logs' / 'claude_worker.out'

LOCAL_DSN = 'postgresql://postgres@localhost:5432/jbot_local'
SOURCE_DSN = (
    'postgresql://postgres@pmc-us-main.job-bot.ai:5432/'
    'jbot-pcm?hostaddr=100.115.252.23&sslmode=verify-full&sslrootcert=/etc/ssl/postgres/root.crt'
)

CLAUDE_CMD_TEMPLATE = 'claude -p "$(cat {prompt_file})" --output-format json'
DEFAULT_PATH = '/usr/local/bin:/usr/bin:/bin:/root/.local/bin'


def ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_env_file() -> None:
    if not ENV_FILE.exists():
        return

    for line in ENV_FILE.read_text(encoding='utf-8').splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in stripped:
            continue
        key, value = stripped.split('=', 1)
        os.environ.setdefault(key.strip(), value.strip())


def worker_env() -> dict[str, str]:
    env = os.environ.copy()
    path_parts = [env.get('PATH', ''), DEFAULT_PATH]
    env['PATH'] = ':'.join(part for part in path_parts if part)
    return env


def claude_paused() -> bool:
    raw = os.environ.get('PAUSED_PROVIDERS', '')
    paused = {part.strip().lower() for part in raw.split(',') if part.strip()}
    return 'anthropic' in paused


def resolve_cli_command(template: str, env: dict[str, str]) -> str:
    parts = shlex.split(template, posix=True)
    if not parts:
        return template

    binary = parts[0]
    if '/' in binary:
        return template

    resolved = shutil.which(binary, path=env.get('PATH'))
    if not resolved:
        return template

    return template.replace(binary, resolved, 1)


def worker_running() -> bool:
    cmd = "ps -eo args"
    result = subprocess.run(cmd, shell=True, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return False

    for line in (result.stdout or "").splitlines():
        if "run_job_enrichment.py" not in line:
            continue
        if "--provider \"Anthropic\"" in line and "--model \"claude-sonnet\"" in line:
            return True
        if "--provider Anthropic" in line and "--model claude-sonnet" in line:
            return True
    return False


def claude_ready() -> bool:
    env = worker_env()
    claude_bin = shutil.which('claude', path=env.get('PATH')) or 'claude'
    test_cmd = (
        f"{shlex.quote(claude_bin)} -p 'Return strict JSON only: {{\"ok\":true}}' "
        "--output-format json"
    )
    result = subprocess.run(
        test_cmd,
        shell=True,
        check=False,
        capture_output=True,
        text=True,
        cwd=str(WORKDIR),
        env=env,
    )
    if result.returncode != 0:
        return False
    return '"is_error":true' not in (result.stdout or '')


def launch_worker() -> None:
    env = worker_env()
    cli_cmd = resolve_cli_command(CLAUDE_CMD_TEMPLATE, env)
    run_cmd = (
        f"nohup {PYTHON} {RUNNER} "
        f"--local-dsn \"{LOCAL_DSN}\" "
        f"--source-dsn \"{SOURCE_DSN}\" "
        "--provider \"Anthropic\" "
        "--model \"claude-sonnet\" "
        f"--cli-cmd {shlex.quote(cli_cmd)} "
        "--max-jobs 50000 "
        "--batch-size 50 "
        "--rpm 8 "
        "--claim-ttl 3600 "
        "--sync-batch-size 5000 "
        f">> \"{OUT_LOG}\" 2>&1 &"
    )
    subprocess.run(run_cmd, shell=True, check=False, cwd=str(WORKDIR), env=env)


def main() -> None:
    load_env_file()
    print(f"[{ts()}] claude_autostart check")
    if claude_paused():
        print('claude paused by configuration')
        return

    if worker_running():
        print('worker already running')
        return

    if not claude_ready():
        print('claude not ready (auth/inference still failing)')
        return

    launch_worker()
    print('launched claude worker')


if __name__ == '__main__':
    os.makedirs(WORKDIR / 'logs', exist_ok=True)
    main()
