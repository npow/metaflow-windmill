# metaflow-windmill

[Windmill](https://windmill.dev) scheduling and orchestration for [Metaflow](https://metaflow.org).

Windmill is an open-source workflow orchestrator that runs Python scripts as DAGs. This
extension lets you deploy and trigger Metaflow flows as Windmill workflows.

## Installation

```bash
pip install metaflow-windmill
```

## Quick Start

Start Windmill locally:

```bash
docker compose up -d  # use the docker-compose.yml from this repo's devtools/
```

Get your API token from the Windmill UI at http://localhost:8000.

Deploy and run your flow:

```bash
# Compile and deploy
python flow.py windmill create \
  --windmill-host http://localhost:8000 \
  --windmill-token <token> \
  --windmill-workspace admins

# Trigger a run
python flow.py windmill trigger \
  --windmill-host http://localhost:8000 \
  --windmill-token <token> \
  --windmill-workspace admins

# Or compile, deploy, and trigger in one step
python flow.py windmill run \
  --windmill-host http://localhost:8000 \
  --windmill-token <token>
```

## Programmatic API

```python
from metaflow import Deployer

with Deployer("flow.py") as d:
    df = d.windmill().create(
        windmill_host="http://localhost:8000",
        windmill_token="my-token",
        windmill_workspace="admins",
    )
    triggered = df.trigger()
    print(triggered.run.successful)
```

## Configuration

| CLI Option | Env Var | Default | Description |
|---|---|---|---|
| `--windmill-host` | `WINDMILL_HOST` | `http://localhost:8000` | Windmill server URL |
| `--windmill-token` | `WINDMILL_TOKEN` | — | API token |
| `--windmill-workspace` | `WINDMILL_WORKSPACE` | `admins` | Workspace name |
| `--max-workers` | — | `10` | Max parallel ForEach workers |
| `--branch` | — | — | @project branch name |
| `--production` | — | `false` | Deploy to production branch |

## Supported Graph Patterns

- Linear flows (sequential steps)
- Branch/join (parallel branches)
- ForEach (single level of fan-out)

Not supported:
- Nested foreach (foreach inside foreach)
- Conditional splits (`@condition`)
- `@batch` (Windmill runs steps as local processes)

## How It Works

Each Metaflow step becomes a Windmill flow module that runs a bash script. The bash
script invokes `python flow.py step <step_name>` with the correct `--run-id`,
`--task-id`, and `--retry-count` arguments.

The retry count is derived from Windmill's native `WM_FLOW_RETRY_COUNT` environment
variable so that Metaflow's `@retry` decorator works correctly.

## Implementation Contract

This extension satisfies all required capabilities:

- **Cap.RUN_PARAMS**: `run_params` is always a list, not a tuple.
- **Cap.PROJECT_BRANCH**: `--branch` is forwarded to every step subprocess.
- **Cap.CONFIG_EXPR**: `METAFLOW_FLOW_CONFIG_VALUE` is injected into every step.
- **Cap.RETRY**: retry count is derived from `WM_FLOW_RETRY_COUNT`.
- **Cap.FROM_DEPLOYMENT**: handles dotted identifiers (project.branch.FlowName).
