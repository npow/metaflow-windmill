# metaflow-windmill

[![CI](https://github.com/npow/metaflow-windmill/actions/workflows/ux-tests.yml/badge.svg)](https://github.com/npow/metaflow-windmill/actions/workflows/ux-tests.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-windmill)](https://pypi.org/project/metaflow-windmill/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/metaflow-windmill)](https://pypi.org/project/metaflow-windmill/)

Run any Metaflow flow on Windmill without rewriting your pipeline.

## The problem

Windmill is a powerful open-source orchestrator, but its native scripting model has no concept of Metaflow steps, data artifacts, or retry semantics. You either maintain two separate codebases — one for Metaflow development and one for Windmill production — or you lose Metaflow's versioning, lineage, and `@retry` guarantees entirely. There is no off-the-shelf bridge that compiles a Metaflow flow into a Windmill workflow while preserving all of its runtime behavior.

## Quick start

```bash
pip install metaflow-windmill
python flow.py windmill create --windmill-host http://localhost:8000 --windmill-token $TOKEN
python flow.py windmill trigger --windmill-host http://localhost:8000 --windmill-token $TOKEN
# Deploying HelloFlow...
# Flow deployed successfully to Windmill.
# Job started: http://localhost:8000/run/01927f3a-...?workspace=admins
```

## Install

```bash
pip install metaflow-windmill
```

From source:

```bash
git clone https://github.com/npow/metaflow-windmill
cd metaflow-windmill
pip install -e .
```

## Usage

**Deploy and trigger in one step:**

```bash
python flow.py windmill run \
  --windmill-host http://localhost:8000 \
  --windmill-token $TOKEN \
  --windmill-workspace admins
# Compiling HelloFlow...
# Flow deployed successfully to Windmill.
# Triggering execution...
# Job started: http://localhost:8000/run/01927f3a-...?workspace=admins
# Job is running...
# Job 01927f3a-... completed successfully.
```

**Deploy once, trigger many times with parameters:**

```bash
python flow.py windmill create --windmill-token $TOKEN
python flow.py windmill trigger --windmill-token $TOKEN \
  --run-param message=hello --run-param iterations=5
```

**Programmatic API:**

```python
from metaflow import Deployer

with Deployer("flow.py") as d:
    df = d.windmill().create(
        windmill_host="http://localhost:8000",
        windmill_token="my-token",
    )
    run = df.trigger(message="hello")
    print(run.status)          # RUNNING / SUCCEEDED / FAILED
    print(run.windmill_ui)     # http://localhost:8000/run/<job-id>?workspace=admins
    print(run.run.successful)  # True
```

## How it works

Each Metaflow step becomes a Windmill flow module that runs a bash script. The bash script calls `python flow.py step <step_name>` with `--run-id`, `--task-id`, and `--retry-count` derived from Windmill's native `WM_FLOW_RETRY_COUNT` environment variable. Branch/join steps compile to Windmill `branchall` modules; foreach steps compile to `forloopflow` modules with a configurable `max_workers` concurrency limit. The Metaflow run ID is pre-computed before triggering and threaded through every step so Metaflow's local datastore and the Windmill job ID stay in sync.

Supported graph patterns:

| Metaflow pattern | Windmill module |
|---|---|
| Linear steps | `rawscript` sequence |
| Split / join (static branches) | `branchall` |
| ForEach | `forloopflow` |
| Nested ForEach | nested `forloopflow` |
| `@condition` (conditional split) | `branchone` |

For `@condition` splits, the split step emits `{"branch": "<step_name>"}` to stdout after running. The subsequent `branchone` module uses `results.<split_step>.branch === '<step_name>'` as the predicate so Windmill routes to the correct branch at runtime.

`@parallel` and `@batch` are not supported — Windmill runs each step as a local subprocess on the worker.

## Configuration

| CLI option | Environment variable | Default | Description |
|---|---|---|---|
| `--windmill-host` | `WINDMILL_HOST` | `http://localhost:8000` | Windmill server base URL |
| `--windmill-token` | `WINDMILL_TOKEN` | — | Windmill API token |
| `--windmill-workspace` | `WINDMILL_WORKSPACE` | `admins` | Workspace name |
| `--max-workers` | — | `10` | Max concurrent ForEach body tasks |
| `--branch` | — | — | `@project` branch name |
| `--production` | — | `false` | Deploy to the production project branch |
| `--name` | — | derived from class name | Override the Windmill flow path |

## Development

```bash
git clone https://github.com/npow/metaflow-windmill
cd metaflow-windmill
pip install -e ".[dev]"
pytest tests/
```

Integration tests require a running Windmill instance:

```bash
docker compose up -d
pytest tests/ -m integration
```

## License

Apache 2.0. See [LICENSE](LICENSE).
