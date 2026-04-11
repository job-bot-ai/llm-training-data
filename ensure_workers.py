#!/usr/bin/env python3
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

WORKDIR = Path('/j-bot/llm-training')
HEARTBEAT_LOG = WORKDIR / 'logs' / 'cron_heartbeat.log'
ENV_FILE = WORKDIR / '.env.local'
PYTHON = '/j-bot/llm-training/.venv/bin/python'
RUNNER = '/j-bot/llm-training/run_job_enrichment.py'
LOCAL_DSN = 'postgresql://postgres:postgres@localhost:5432/jbot_local'
SOURCE_DSN = (
    'postgresql://postgres:kR7mQ2pXwN@pmc-us-main.job-bot.ai:5432/'
    'jbot-pcm?hostaddr=100.115.252.23&sslmode=verify-full&sslrootcert=/etc/ssl/postgres/root.crt'
)
DEFAULT_PATH = '/usr/local/bin:/usr/bin:/bin:/root/.local/bin'

WORKERS = [
    {
        'provider': 'Google',
        'model': 'gemini-2.5-pro',
        'cli_cmd': 'gemini --prompt "" < {prompt_file}',
        'max_jobs': 49900,
        'batch_size': 50,
        'rpm': 12,
        'log_file': WORKDIR / 'logs' / 'google_worker.out',
    },
    {
        'provider': 'OpenAI',
        'model': 'codex-gpt-5.4',
        'cli_cmd': 'codex exec - --json < {prompt_file}',
        'max_jobs': 50000,
        'batch_size': 50,
        'rpm': 10,
        'log_file': WORKDIR / 'logs' / 'openai_worker.out',
    },
    {
        'provider': 'Anthropic',
        'model': 'claude-sonnet',
        'cli_cmd': 'claude -p "$(cat {prompt_file})" --output-format json',
        'max_jobs': 50000,
        'batch_size': 50,
        'rpm': 8,
        'log_file': WORKDIR / 'logs' / 'claude_worker.out',
    },
]


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


def paused_providers() -> set[str]:
    raw = os.environ.get('PAUSED_PROVIDERS', '')
    return {part.strip().lower() for part in raw.split(',') if part.strip()}


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


def cmd_ok(command: str, timeout: int = 60) -> bool:
    result = subprocess.run(
        command,
        shell=True,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(WORKDIR),
        env=worker_env(),
    )
    return result.returncode == 0


def claude_ready() -> bool:
    env = worker_env()
    claude_bin = shutil.which('claude', path=env.get('PATH')) or 'claude'
    test_cmd = f"{shlex.quote(claude_bin)} -p 'Return strict JSON only: {{\"ok\":true}}' --output-format json"
    try:
        result = subprocess.run(
            test_cmd,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(WORKDIR),
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False

    if result.returncode != 0:
        return False

    out = result.stdout or ''
    return '"is_error":true' not in out


def worker_running(provider: str, model: str) -> bool:
    result = subprocess.run('ps -eo args', shell=True, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return False

    for line in (result.stdout or '').splitlines():
        if 'run_job_enrichment.py' not in line:
            continue
        if f'--provider "{provider}"' in line and f'--model "{model}"' in line:
            return True
        if f'--provider {provider}' in line and f'--model {model}' in line:
            return True
    return False


def launch_worker(cfg: dict) -> None:
    env = worker_env()
    cli_cmd = resolve_cli_command(cfg['cli_cmd'], env)
    cfg['log_file'].parent.mkdir(parents=True, exist_ok=True)
    cmd = (
        f"nohup {PYTHON} {RUNNER} "
        f"--local-dsn \"{LOCAL_DSN}\" "
        f"--source-dsn \"{SOURCE_DSN}\" "
        f"--provider \"{cfg['provider']}\" "
        f"--model \"{cfg['model']}\" "
        f"--cli-cmd {shlex.quote(cli_cmd)} "
        f"--max-jobs {cfg['max_jobs']} "
        f"--batch-size {cfg['batch_size']} "
        f"--rpm {cfg['rpm']} "
        "--claim-ttl 3600 "
        "--sync-batch-size 5000 "
        f">> \"{cfg['log_file']}\" 2>&1 &"
    )
    subprocess.run(cmd, shell=True, check=False, cwd=str(WORKDIR), env=env)


def main() -> None:
    load_env_file()
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open('a', encoding='utf-8') as f:
        f.write(f"[{ts()}] ensure_workers heartbeat\n")

    print(f"[{ts()}] ensure_workers check")

    paused = paused_providers()

    for cfg in WORKERS:
        provider = cfg['provider']
        model = cfg['model']
        if provider.lower() in paused:
            print(f"{provider}/{model}: paused")
            continue
        if worker_running(provider, model):
            print(f"{provider}/{model}: running")
            continue

        if provider == 'Anthropic' and not claude_ready():
            print(f"{provider}/{model}: not running, auth not ready")
            continue

        launch_worker(cfg)
        print(f"{provider}/{model}: launched")


if __name__ == '__main__':
    main()
