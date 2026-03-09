"""
Windmill CLI commands for Metaflow.

Registered via mfextinit_windmill.py as the `windmill` CLI group.

Commands
--------
  create   Compile and upload the flow to a running Windmill instance.
  trigger  Trigger a run for a previously deployed Windmill flow.
  run      Compile, deploy, trigger an execution and wait for it.
"""

import hashlib
import json
import os
import sys
import time

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.util import get_username

from .exception import WindmillException, NotSupportedException
from .windmill_compiler import WindmillCompiler, flow_name_to_path


def _effective_flow_name(obj, branch=None, production=False):
    """Return the project-decorated flow name used by Metaflow in the datastore.

    When @project is applied, the datastore stores runs under
    ``project.branch.FlowName`` instead of just ``FlowName``.  The pathspec
    written to the deployer attribute file must use this decorated name so that
    ``metaflow.Run(pathspec)`` resolves to the correct directory.
    """
    try:
        from metaflow.decorators import flow_decorators
        for deco in flow_decorators(obj.flow):
            if deco.name == "project":
                project_name = deco.attributes.get("name", "")
                if production:
                    branch_str = "prod"
                elif branch:
                    branch_str = "test.%s" % branch
                else:
                    branch_str = "user.%s" % (get_username() or "unknown")
                return "%s.%s.%s" % (project_name, branch_str, obj.graph.name)
    except Exception:
        pass
    return obj.graph.name


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_workflow(flow, graph):
    """Raise MetaflowException for unsupported features."""
    seen = set()
    for _, param in flow._get_parameters():
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException(
                "Parameter *%s* is specified twice. "
                "Parameter names are case-insensitive." % param.name
            )
        seen.add(norm)
        if "default" not in param.kwargs:
            raise MetaflowException(
                "Parameter *%s* does not have a default value. "
                "A default value is required when deploying to Windmill." % param.name
            )

    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                "Step *%s* uses @parallel which is not supported with Windmill."
                % node.name
            )
        if any(d.name == "batch" for d in node.decorators):
            raise NotSupportedException(
                "Step *%s* uses @batch which is not supported with Windmill. "
                "Windmill runs steps as local processes." % node.name
            )

    for bad in ("exit_hook",):
        decos = getattr(flow, "_flow_decorators", {}).get(bad)
        if decos:
            raise NotSupportedException(
                "@%s is not supported with Windmill." % bad
            )


# ---------------------------------------------------------------------------
# Click command group
# ---------------------------------------------------------------------------


@click.group()
def cli():
    pass


@cli.group(help="Commands related to Windmill orchestration.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Override the Windmill flow path (default: derived from flow class name).",
)
@click.pass_obj
def windmill(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.windmill_flow_name = name or obj.graph.name


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


@windmill.command(help="Compile and deploy this flow to a running Windmill instance.")
@click.option(
    "--windmill-host",
    default="http://localhost:8000",
    show_default=True,
    envvar="WINDMILL_HOST",
    help="Windmill server base URL.",
)
@click.option(
    "--windmill-token",
    default=None,
    envvar="WINDMILL_TOKEN",
    help="Windmill API token.",
)
@click.option(
    "--windmill-workspace",
    default="admins",
    show_default=True,
    envvar="WINDMILL_WORKSPACE",
    help="Windmill workspace.",
)
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option(
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="Max concurrent ForEach body tasks.",
)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def create(
    obj,
    windmill_host,
    windmill_token,
    windmill_workspace,
    tags,
    namespace,
    max_workers,
    with_decorators,
    branch,
    production,
    deployer_attribute_file,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo("Compiling *%s* to Windmill flow..." % obj.windmill_flow_name, bold=True)

    compiler = _build_compiler(
        obj,
        windmill_workspace,
        max_workers,
        with_decorators,
        branch,
        production,
        namespace=namespace,
        tags=tags,
    )
    flow_json = compiler.compile()
    flow_path = compiler.flow_path

    client = _make_client(windmill_host, windmill_token)
    _deploy_flow(client, windmill_workspace, flow_path, flow_json, obj)

    if deployer_attribute_file:
        # Use the project-decorated flow name for the Metaflow pathspec.
        # With @project, the datastore uses "project.branch.FlowName" not "FlowName".
        effective_name = compiler._flow_name
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": flow_path,
                    "flow_name": effective_name,
                    "metadata": "{}",
                    "additional_info": {
                        "flow_path": flow_path,
                        "windmill_host": windmill_host,
                        "windmill_token": windmill_token,
                        "windmill_workspace": windmill_workspace,
                        "mf_flow_class": effective_name,
                    },
                },
                f,
            )


# ---------------------------------------------------------------------------
# trigger
# ---------------------------------------------------------------------------


@windmill.command(help="Trigger a run for a previously deployed Windmill flow.")
@click.option(
    "--windmill-host",
    default="http://localhost:8000",
    show_default=True,
    envvar="WINDMILL_HOST",
)
@click.option("--windmill-token", default=None, envvar="WINDMILL_TOKEN")
@click.option(
    "--windmill-workspace",
    default="admins",
    show_default=True,
    envvar="WINDMILL_WORKSPACE",
)
@click.option(
    "--flow-path",
    default=None,
    hidden=True,
    help="Windmill flow path to trigger (overrides computed default).",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def trigger(
    obj,
    windmill_host,
    windmill_token,
    windmill_workspace,
    flow_path,
    deployer_attribute_file,
    run_params,
):
    if flow_path is None:
        flow_path = flow_name_to_path(obj.windmill_flow_name)

    # Parse run params into a dict
    params = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    client = _make_client(windmill_host, windmill_token)

    obj.echo(
        "Triggering execution of *%s* in workspace *%s*..."
        % (flow_path, windmill_workspace),
        bold=True,
    )
    job_id, metaflow_run_id = _trigger_job(client, windmill_workspace, flow_path, inputs=params or None)
    job_url = "%s/run/%s?workspace=%s" % (windmill_host, job_id, windmill_workspace)
    obj.echo("Job started: *%s*" % job_url)

    if deployer_attribute_file:
        # Use the project-decorated flow name for the Metaflow pathspec.
        # With @project, the datastore uses "project.branch.FlowName" not "FlowName".
        effective_name = _effective_flow_name(obj)
        pathspec = "%s/%s" % (effective_name, metaflow_run_id)
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": obj.windmill_flow_name,
                    "job_id": job_id,
                    "job_url": job_url,
                    "metadata": "{}",
                },
                f,
            )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@windmill.command(
    help="Compile, deploy, and trigger an execution on Windmill."
)
@click.option(
    "--windmill-host",
    default="http://localhost:8000",
    show_default=True,
    envvar="WINDMILL_HOST",
)
@click.option("--windmill-token", default=None, envvar="WINDMILL_TOKEN")
@click.option(
    "--windmill-workspace",
    default="admins",
    show_default=True,
    envvar="WINDMILL_WORKSPACE",
)
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option("--max-workers", default=10, show_default=True, type=int)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for the execution to complete.",
)
@click.pass_obj
def run(
    obj,
    windmill_host,
    windmill_token,
    windmill_workspace,
    tags,
    namespace,
    max_workers,
    with_decorators,
    branch,
    production,
    wait,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo("Compiling *%s*..." % obj.windmill_flow_name, bold=True)

    compiler = _build_compiler(
        obj,
        windmill_workspace,
        max_workers,
        with_decorators,
        branch,
        production,
        namespace=namespace,
        tags=tags,
    )
    flow_json = compiler.compile()
    flow_path = compiler.flow_path

    client = _make_client(windmill_host, windmill_token)
    _deploy_flow(client, windmill_workspace, flow_path, flow_json, obj)

    obj.echo("Triggering execution...", bold=True)
    job_id, _metaflow_run_id = _trigger_job(client, windmill_workspace, flow_path)
    job_url = "%s/run/%s?workspace=%s" % (windmill_host, job_id, windmill_workspace)
    obj.echo("Job started: *%s*" % job_url)

    if wait:
        obj.echo("Waiting for job to complete...")
        success, final_state = _wait_for_job(
            client, windmill_workspace, job_id, obj
        )
        if success:
            obj.echo("Job *%s* completed successfully." % job_id, bold=True)
        else:
            raise WindmillException(
                "Job %s finished with state: %s\nURL: %s"
                % (job_id, final_state, job_url)
            )
    else:
        obj.echo("Job ID: %s" % job_id)
        obj.echo("Track it at: %s" % job_url)


# ---------------------------------------------------------------------------
# Windmill API helpers
# ---------------------------------------------------------------------------


def _build_compiler(
    obj,
    windmill_workspace,
    max_workers,
    with_decorators,
    branch,
    production,
    namespace=None,
    tags=(),
) -> WindmillCompiler:
    """Construct a WindmillCompiler from a Metaflow CLI obj and shared options."""
    effective_branch = branch
    if effective_branch is None and not production:
        try:
            from metaflow import current as _current
            bn = getattr(_current, "branch_name", None)
            if bn and not bn.startswith("user.") and not bn.startswith("prod"):
                if bn.startswith("test."):
                    effective_branch = bn[len("test."):]
                else:
                    effective_branch = bn
        except Exception:
            pass

    return WindmillCompiler(
        name=obj.windmill_flow_name,
        graph=obj.graph,
        flow=obj.flow,
        flow_file=os.path.abspath(sys.argv[0]),
        metadata=obj.metadata,
        flow_datastore=obj.flow_datastore,
        environment=obj.environment,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        tags=list(tags),
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        with_decorators=list(with_decorators),
        windmill_workspace=windmill_workspace,
        branch=effective_branch,
        production=production,
    )


def _make_client(host: str, token: str):
    """Return a requests.Session configured for the given Windmill instance."""
    try:
        import requests
    except ImportError:
        raise WindmillException(
            "The `requests` package is required for deploy/run commands. "
            "Install it with: pip install requests"
        )
    session = requests.Session()
    if token:
        session.headers["Authorization"] = "Bearer %s" % token
    session.headers["Content-Type"] = "application/json"
    session._windmill_host = host
    return session


def _deploy_flow(
    client, workspace: str, flow_path: str, flow_json: dict, obj
):
    """Create or update a Windmill flow via the REST API."""
    host = client._windmill_host

    # Check if flow already exists
    check_url = "%s/api/w/%s/flows/get/%s" % (host, workspace, flow_path)
    try:
        check_resp = client.get(check_url, headers={"Content-Type": None})
        flow_exists = check_resp.status_code == 200
    except Exception:
        flow_exists = False

    try:
        if flow_exists:
            url = "%s/api/w/%s/flows/update/%s" % (host, workspace, flow_path)
            payload = dict(flow_json)
            payload["path"] = flow_path
            resp = client.post(url, json=payload)
        else:
            url = "%s/api/w/%s/flows/create" % (host, workspace)
            payload = dict(flow_json)
            payload["path"] = flow_path
            resp = client.post(url, json=payload)

        if resp.status_code in (200, 201):
            obj.echo("Flow deployed successfully to Windmill.")
            return
        raise WindmillException(
            "Failed to deploy flow to Windmill (HTTP %d): %s"
            % (resp.status_code, resp.text[:500])
        )
    except Exception as exc:
        if isinstance(exc, WindmillException):
            raise
        raise WindmillException(
            "Failed to connect to Windmill at %s: %s" % (host, exc)
        ) from exc


def _trigger_job(
    client, workspace: str, flow_path: str, inputs: dict = None
) -> tuple:
    """Trigger a Windmill flow job and return (job_id, metaflow_run_id).

    We pre-compute the Metaflow run_id before triggering and pass it as
    METAFLOW_RUN_ID flow input so the init module can use the same value.
    This ensures the pathspec in the deployer matches the actual run in the
    local datastore.
    """
    import uuid
    # Pre-compute a stable run_id using a UUID
    metaflow_run_id = "windmill-" + str(uuid.uuid4()).replace("-", "")[:16]

    host = client._windmill_host
    url = "%s/api/w/%s/jobs/run/f/%s" % (host, workspace, flow_path)
    try:
        payload = dict(inputs or {})
        # Pass the pre-computed run_id as a special flow input
        payload["METAFLOW_RUN_ID"] = metaflow_run_id
        resp = client.post(url, json=payload)
        if resp.status_code not in (200, 201):
            raise WindmillException(
                "Failed to trigger job (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )
        # Windmill returns the job UUID as a plain quoted string
        job_id = resp.text.strip().strip('"')
        return job_id, metaflow_run_id
    except Exception as exc:
        if isinstance(exc, WindmillException):
            raise
        raise WindmillException("Failed to trigger job: %s" % exc) from exc


def _wait_for_job(
    client, workspace: str, job_id: str, obj, poll_interval: int = 3
):
    """Poll until the job reaches a terminal state. Returns (success, state)."""
    host = client._windmill_host
    url = "%s/api/w/%s/jobs_u/get/%s" % (host, workspace, job_id)

    seen_running = False

    while True:
        try:
            resp = client.get(url, headers={"Content-Type": None})
            if resp.status_code == 200:
                data = resp.json()
                job_type = data.get("type", "")
                if job_type.lower() == "completedjob":
                    success = data.get("success", False)
                    return success, ("SUCCEEDED" if success else "FAILED")
                if not seen_running:
                    obj.echo("Job is running...")
                    seen_running = True
            else:
                obj.echo(
                    "Warning: could not poll job status (HTTP %d)" % resp.status_code
                )
        except Exception as exc:
            obj.echo("Warning: error polling job: %s" % exc)

        time.sleep(poll_interval)
