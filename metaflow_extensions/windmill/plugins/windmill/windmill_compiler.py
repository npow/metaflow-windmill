"""
Windmill flow compiler for Metaflow flows.

Converts a Metaflow FlowGraph into a Windmill OpenFlow JSON definition.
Each Metaflow step becomes a Windmill flow module that runs a bash script.

Supported Metaflow graph patterns:
  linear        - sequential modules
  split/join    - parallel branches (branchall modules)
  foreach       - forloopflow modules (single level)
  nested foreach - nested forloopflow modules (outer iterates, inner iterates per item)
  split-switch  - branchone modules (@condition decorator)

The compiled flow runs each Metaflow step via bash:
  python flow.py --no-pylint step <step_name>
      --run-id $RUN_ID --task-id 1 --retry-count ${WM_FLOW_RETRY_COUNT:-0}

where WM_FLOW_RETRY_COUNT comes from Windmill's native retry mechanism.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

from .exception import NotSupportedException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def flow_name_to_path(name: str) -> str:
    """Convert a Metaflow flow name to a Windmill flow path."""
    slug = name.lower().replace(".", "-").replace("_", "-")
    return "u/admin/" + slug


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------


class WindmillCompiler:
    """Compiles a Metaflow flow into a Windmill OpenFlow JSON definition."""

    def __init__(
        self,
        name: str,
        graph,
        flow,
        flow_file: str,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        username: Optional[str] = None,
        max_workers: int = 10,
        with_decorators: Optional[List[str]] = None,
        windmill_workspace: str = "admins",
        branch: Optional[str] = None,
        production: bool = False,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.flow_file = flow_file
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags or []
        self.namespace = namespace
        self.username = username or ""
        self.max_workers = max_workers
        self.with_decorators = with_decorators or []
        self.windmill_workspace = windmill_workspace
        self.branch = branch
        self.production = production

        self._project_info = self._get_project()
        self._flow_name = (
            self._project_info["flow_name"] if self._project_info else name
        )

        self._metadata_type = metadata.TYPE
        self._datastore_type = flow_datastore.TYPE
        # Capture the sysroot from the environment at compile time (deploy time).
        # IMPORTANT: METAFLOW_DATASTORE_SYSROOT_LOCAL is the PARENT of .metaflow,
        # not the full datastore_root. flow_datastore.datastore_root already has
        # .metaflow appended, so we must use the env var directly.
        # Using flow_datastore.datastore_root would cause double .metaflow nesting.
        self._datastore_root = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "") or ""
        self._environment_type = environment.TYPE
        self._event_logger_type = event_logger.TYPE
        self._monitor_type = monitor.TYPE

        # Capture compile-time config values for METAFLOW_FLOW_CONFIG_VALUE
        self._flow_config_value = self._extract_flow_config_value(flow)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_flow_config_value(flow) -> Optional[str]:
        """Serialize compile-time config values to JSON string."""
        try:
            from metaflow.flowspec import FlowStateItems
            flow_configs = flow._flow_state[FlowStateItems.CONFIGS]
            config_env = {
                name: value
                for name, (value, _is_plain) in flow_configs.items()
                if value is not None
            }
            if config_env:
                return json.dumps(config_env)
        except Exception:
            pass
        return None

    def _get_project(self):
        """Return project info dict if @project is applied, else None."""
        try:
            from metaflow.decorators import flow_decorators
            for deco in flow_decorators(self.flow):
                if deco.name == "project":
                    project_name = deco.attributes.get("name", "")
                    if self.production:
                        branch = "prod"
                    elif self.branch:
                        branch = "test.%s" % self.branch
                    else:
                        branch = "user.%s" % (self.username or "unknown")
                    flow_name = "%s.%s.%s" % (project_name, branch, self.name)
                    return {
                        "name": project_name,
                        "branch": branch,
                        "flow_name": flow_name,
                    }
        except Exception:
            pass
        return None

    def _get_parameters(self) -> Dict:
        """Return dict of {param_name: {default, type, help}} for flow parameters."""
        params = {}
        for _, param in self.flow._get_parameters():
            if param.name.lower() == "config-value":
                continue
            default = None
            if "default" in param.kwargs:
                raw = param.kwargs["default"]
                default = raw() if callable(raw) else raw
            params[param.name] = {
                "default": default,
                "type": param.kwargs.get("type", str),
                "help": param.kwargs.get("help", ""),
            }
        return params

    def _get_retry_config(self, node):
        """Return (retries, delay_seconds) for a node."""
        for deco in node.decorators:
            if deco.name == "retry":
                retries = int(deco.attributes.get("times", 0))
                delay = int(deco.attributes.get("minutes_between_retries", 2)) * 60
                return retries, delay
        return 0, 0

    def _get_timeout(self, node):
        """Return timeout in seconds for a node, or None."""
        try:
            from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
            limit = get_run_time_limit_for_task(node.decorators)
            return limit
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def flow_path(self) -> str:
        """Windmill flow path derived from the flow name."""
        return flow_name_to_path(self._flow_name)

    def compile(self) -> dict:
        """Return the complete Windmill OpenFlow JSON as a Python dict."""
        params = self._get_parameters()
        schema = self._build_schema(params)
        modules = self._build_modules(params)
        flow_value = {
            "modules": modules,
            # same_worker=True forces all modules to run on the SAME worker process.
            # This allows modules to share /tmp files (e.g. for passing the run ID).
            "same_worker": True,
        }
        return {
            "summary": "Metaflow flow: %s" % self._flow_name,
            "description": (
                "Generated by metaflow-windmill on %s.\nFlow: %s\nFlow file: %s"
                % (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    self._flow_name,
                    os.path.basename(self.flow_file),
                )
            ),
            "value": flow_value,
            "schema": schema,
        }

    # ------------------------------------------------------------------
    # Schema builder
    # ------------------------------------------------------------------

    def _build_schema(self, params: Dict) -> dict:
        """Build the JSON Schema for flow inputs (Metaflow parameters)."""
        properties = {}
        required = []
        for pname, pinfo in params.items():
            default = pinfo.get("default")
            ptype = pinfo.get("type", str)
            json_type = "string"
            if ptype in (int, "int"):
                json_type = "integer"
            elif ptype in (float, "float"):
                json_type = "number"
            elif ptype in (bool, "bool"):
                json_type = "boolean"
            prop = {
                "type": json_type,
                "description": pinfo.get("help", ""),
            }
            if default is not None:
                prop["default"] = default
            properties[pname] = prop
            if default is None:
                required.append(pname)
        # Always add ORIGIN_RUN_ID for resume support
        properties["ORIGIN_RUN_ID"] = {
            "type": "string",
            "description": (
                "Leave empty for a normal run. "
                "Set to a previous Metaflow run ID to resume."
            ),
            "default": "",
        }
        # METAFLOW_RUN_ID is set by the deployer at trigger time to a pre-computed UUID.
        # The init module uses this value as the Metaflow run ID, ensuring the pathspec
        # in the deployer matches the actual run written to the local datastore.
        properties["METAFLOW_RUN_ID"] = {
            "type": "string",
            "description": (
                "Pre-computed Metaflow run ID set by the deployer. "
                "Do not set manually."
            ),
            "default": "",
        }
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": properties,
            "required": required,
        }

    # ------------------------------------------------------------------
    # Module builder
    # ------------------------------------------------------------------

    def _build_modules(self, params: Dict) -> list:
        """Walk the graph and produce a list of Windmill flow modules."""
        modules = []
        modules.append(self._build_init_module(params))
        visited = set()
        self._visit_node("start", modules, visited)
        return modules

    def _visit_node(self, step_name: str, out: list, visited: set):
        """Recursively visit graph nodes and emit Windmill modules."""
        if step_name in visited:
            return
        visited.add(step_name)

        node = self.graph[step_name]
        ntype = node.type

        if ntype == "end":
            out.append(self._build_step_module(node))
            return

        if ntype in ("start", "linear"):
            out.append(self._build_step_module(node))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited)

        elif ntype == "foreach":
            out.append(self._build_step_module(node))
            out.append(self._build_foreach_module(node, visited))
            join_step = self._find_foreach_join(node.name)
            if join_step:
                self._visit_node(join_step, out, visited)

        elif ntype == "split":
            out.append(self._build_step_module(node))
            out.append(self._build_parallel_module(node, visited))
            join_step = self._find_join_step(step_name)
            if join_step:
                self._visit_node(join_step, out, visited)

        elif ntype == "split-switch":
            out.append(self._build_switch_step_module(node))
            out.append(self._build_branchone_module(node, visited))
            join_step = self._find_join_step(step_name)
            if join_step:
                self._visit_node(join_step, out, visited)

        elif ntype == "join":
            out.append(self._build_step_module(node))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited)

        else:
            raise NotSupportedException(
                "Graph node *%s* has unsupported type %r." % (step_name, ntype)
            )

    # ------------------------------------------------------------------
    # Environment helpers
    # ------------------------------------------------------------------

    def _build_env_dict(self) -> Dict[str, str]:
        """Return env vars to inject into every step script."""
        env = {
            "METAFLOW_DEFAULT_METADATA": self._metadata_type,
            "METAFLOW_DEFAULT_DATASTORE": self._datastore_type,
            "METAFLOW_DEFAULT_EVENT_LOGGER": self._event_logger_type,
            "METAFLOW_DEFAULT_MONITOR": self._monitor_type,
        }
        # Always set sysroot - use compile-time value or env fallback
        sysroot = self._datastore_root or os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")
        if sysroot:
            env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
        if self._flow_config_value:
            # REQUIRED (Cap.CONFIG_EXPR): inject compile-time config values
            env["METAFLOW_FLOW_CONFIG_VALUE"] = self._flow_config_value
        if self.username:
            env["USERNAME"] = self.username
        # Forward METAFLOW_SERVICE_* and METAFLOW_DEFAULT_* from compile-time env
        for key, val in os.environ.items():
            if key.startswith("METAFLOW_SERVICE") or (
                key.startswith("METAFLOW_DEFAULT") and key not in env
            ):
                env[key] = val
        # DO NOT forward PYTHONPATH to Windmill worker step scripts.
        # If PYTHONPATH points to a metaflow source tree (e.g., npow/corral-devstack),
        # it may contain internal mfextinit_*.py files that register plugins
        # (like 'service' metadata provider) not available in the worker container.
        # This causes ValueError: Cannot locate metadata_provider plugin 'service'.
        # The worker uses pip-installed metaflow (from PyPI) which has only OSS plugins.
        # Pitfall #34: never forward internal metadata types or source paths to containers.
        return env

    def _env_export_lines(self) -> str:
        """Return shell export statements for all env vars."""
        lines = []
        for key, val in self._build_env_dict().items():
            # Escape single quotes in values
            safe_val = val.replace("'", "'\"'\"'")
            lines.append("export %s='%s'" % (key, safe_val))
        return "\n".join(lines)

    def _step_cmd(self, step_name: str) -> str:
        """Build the base step command (without retry-count, run-id, task-id)."""
        parts = [
            "python3", self.flow_file,
            "--no-pylint",
            "--quiet",
            "--metadata", self._metadata_type,
            "--datastore", self._datastore_type,
            # REQUIRED (Cap.ENVIRONMENT): --environment for @conda support
            "--environment", self._environment_type,
            "--event-logger", self._event_logger_type,
            "--monitor", self._monitor_type,
        ]
        if self._datastore_root:
            # The CLI --datastore-root expects the full path including .metaflow.
            # _datastore_root is the SYSROOT (parent of .metaflow).
            parts += ["--datastore-root", self._datastore_root + "/.metaflow"]

        # REQUIRED (Cap.PROJECT_BRANCH): --branch MUST be forwarded
        if self.branch:
            parts += ["--branch", self.branch]

        if self.namespace:
            parts += ["--namespace", self.namespace]

        for deco in self.with_decorators:
            parts += ["--with", deco]

        # NOTE: --tag goes AFTER the 'step' subcommand (not as a top-level flag)
        # in OSS Metaflow.
        parts += ["step", step_name]
        for tag in self.tags:
            parts += ["--tag", tag]
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Module renderers
    # ------------------------------------------------------------------

    def _build_init_module(self, params: Dict) -> dict:
        """Build the init module that computes mf_run_id and runs metaflow init."""
        # Windmill passes input_transforms values to bash scripts as positional
        # arguments ($1, $2, ...), NOT as environment variables.  We assign them
        # to named shell variables at the top of the script so the rest of the
        # script can reference them by name.
        #
        # The order MUST match the insertion order of keys in input_transforms
        # (built at the bottom of this method): params first, then
        # ORIGIN_RUN_ID, then METAFLOW_RUN_ID.
        arg_idx = 1
        positional_assigns = []
        for pname in params:
            positional_assigns.append('%s="${%d:-}"' % (pname, arg_idx))
            arg_idx += 1
        positional_assigns.append('ORIGIN_RUN_ID="${%d:-}"' % arg_idx)
        arg_idx += 1
        positional_assigns.append('METAFLOW_RUN_ID="${%d:-}"' % arg_idx)
        positional_args_block = "\n".join(positional_assigns)

        param_collection = "\n".join(
            'MF_PARAM_%s="$%s"' % (pname.upper(), pname)
            for pname in params
        )

        base_init_cmd = " ".join([
            "python3", self.flow_file,
            "--no-pylint",
            "--quiet",
            "--metadata", self._metadata_type,
            "--datastore", self._datastore_type,
            "--environment", self._environment_type,
            "--event-logger", self._event_logger_type,
            "--monitor", self._monitor_type,
        ])
        if self._datastore_root:
            base_init_cmd += " --datastore-root " + self._datastore_root + "/.metaflow"
        if self.branch:
            base_init_cmd += " --branch " + self.branch
        if self.namespace:
            base_init_cmd += " --namespace " + self.namespace
        for deco in self.with_decorators:
            base_init_cmd += " --with " + deco
        # NOTE: --tag goes AFTER the 'init' subcommand (not as a top-level flag)
        # in OSS Metaflow; the NFLX extension moves it to top-level.
        # --task-id is required for 'init' in OSS Metaflow.
        # Parameters are passed as METAFLOW_PARAMETER_* env vars to the start step,
        # not via --run-param (which is a NFLX-only init option).
        base_init_cmd += " init --run-id $RUN_ID --task-id windmill-params"
        for tag in self.tags:
            base_init_cmd += " --tag " + tag

        env_exports = self._env_export_lines()

        script = '''\
#!/bin/bash
set -e

# Set up Metaflow environment (set PYTHONPATH first so bootstrap check uses it)
{env_exports}

# Windmill passes input_transforms values as positional arguments to bash
# scripts.  Assign them to named variables so the rest of the script can
# reference them by name.
{positional_args}

# Bootstrap: install metaflow if not already available.
if ! python3 -c "import metaflow" 2>/dev/null; then
    python3 -m pip install --quiet "metaflow>=2.9" 2>&1 || true
fi

# Collect parameters into MF_PARAM_* variables for the init step
{param_collection}

# Use the pre-computed run ID passed as METAFLOW_RUN_ID flow input.
# This ensures the pathspec in the deployer matches the actual Metaflow run.
if [ -n "$METAFLOW_RUN_ID" ]; then
  RUN_ID="$METAFLOW_RUN_ID"
else
  # Fallback: derive from timestamp (manual trigger without deployer)
  RUN_ID="windmill-$(date +%s%N | md5sum | head -c 16)"
fi
export RUN_ID

# Store run ID for downstream steps to use via /tmp (same_worker=True shares /tmp)
echo "$RUN_ID" > /tmp/mf_windmill_run_id.txt

# Initialize Metaflow run (creates _parameters artifact)
if [ -n "$ORIGIN_RUN_ID" ]; then
  {init_cmd} --clone-run-id "$ORIGIN_RUN_ID"
else
  {init_cmd}
fi

echo "Initialized Metaflow run: $RUN_ID"
'''.format(
            env_exports=env_exports,
            positional_args=positional_args_block,
            param_collection=param_collection,
            init_cmd=base_init_cmd,
        )

        input_transforms = {}
        for pname in params:
            input_transforms[pname] = {
                "type": "javascript",
                "expr": "flow_input.%s !== undefined ? String(flow_input.%s) : ''" % (
                    pname, pname
                ),
            }
        input_transforms["ORIGIN_RUN_ID"] = {
            "type": "javascript",
            "expr": "flow_input.ORIGIN_RUN_ID || ''",
        }
        input_transforms["METAFLOW_RUN_ID"] = {
            "type": "javascript",
            "expr": "flow_input.METAFLOW_RUN_ID || ''",
        }

        return {
            "id": "metaflow_init",
            "summary": "Metaflow init (compute mf_run_id, init parameters)",
            "value": {
                "type": "rawscript",
                "content": script,
                "language": "bash",
                "input_transforms": input_transforms,
            },
        }

    def _build_step_module(self, node, extra_args: str = "") -> dict:
        """Build a Windmill module for a single Metaflow step."""
        env_exports = self._env_export_lines()
        step_base_cmd = self._step_cmd(node.name)

        # Compute --input-paths for this step.
        # In OSS Metaflow local storage:
        #   start step input: $RUN_ID/_parameters/1  (from init)
        #   other steps:      $RUN_ID/{parent_step}/1
        #   join steps:       comma-separated list of $RUN_ID/{parent}/1 for each parent
        in_funcs = list(node.in_funcs)
        if node.name == "start":
            input_paths_expr = '"$RUN_ID/_parameters/windmill-params"'
        elif len(in_funcs) == 1:
            parent = in_funcs[0]
            input_paths_expr = '"$RUN_ID/%s/1"' % parent
        else:
            # Multiple parents (join step)
            parent_paths = ",".join("$RUN_ID/%s/1" % p for p in in_funcs)
            input_paths_expr = '"%s"' % parent_paths

        # The RUN_ID is shared via /tmp file since same_worker=True ensures
        # all modules run in the same worker process/container.
        script = '''\
#!/bin/bash
set -e

# Set up Metaflow environment (set PYTHONPATH first so bootstrap check uses it)
{env_exports}

# Bootstrap: install pip + metaflow if not importable.
# System python3 in Windmill container may lack pip — install it first.
# Use --break-system-packages for Ubuntu 22.04+ / Debian 12+ (PEP 668).
if ! python3 -c "import metaflow" 2>/dev/null; then
    python3 -c "import pip" 2>/dev/null || (apt-get update -q 2>/dev/null; apt-get install -y -q python3-pip 2>/dev/null) || true
    python3 -m pip install --break-system-packages --quiet "metaflow>=2.9" 2>&1 || \
    python3 -m pip install --quiet "metaflow>=2.9" 2>&1 || true
fi

# Restore run ID written by the metaflow_init module (same_worker=True shares /tmp)
RUN_ID=$(cat /tmp/mf_windmill_run_id.txt 2>/dev/null || echo "")
if [ -z "$RUN_ID" ]; then
  echo "ERROR: RUN_ID not set. Init step may have failed or same_worker is not enabled."
  exit 1
fi

# REQUIRED (Cap.RETRY): retry_count from Windmill native attempt counter
RETRY_COUNT="${{WM_FLOW_RETRY_COUNT:-0}}"

INPUT_PATHS={input_paths_expr}

{step_cmd} --run-id "$RUN_ID" --task-id 1 --retry-count "$RETRY_COUNT" --input-paths "$INPUT_PATHS" {extra_args}
'''.format(
            env_exports=env_exports,
            step_cmd=step_base_cmd,
            extra_args=extra_args,
            input_paths_expr=input_paths_expr,
        )

        retries, retry_delay = self._get_retry_config(node)
        timeout = self._get_timeout(node)

        module = {
            "id": node.name,
            "summary": "Step: %s" % node.name,
            "value": {
                "type": "rawscript",
                "content": script,
                "language": "bash",
                "input_transforms": {},
            },
        }

        if retries > 0:
            module["retry"] = {
                "constant": {
                    "attempts": retries,
                    "seconds": retry_delay,
                },
            }

        if timeout:
            module["timeout"] = timeout

        return module

    def _build_foreach_body_modules(self, foreach_node, visited: set) -> list:
        """Build the list of modules for the body of a forloopflow.

        For a simple (non-nested) foreach the body is a single rawscript module.

        For a nested foreach (body step is itself a foreach), the returned list is:
          [body_foreach_step_module, inner_forloopflow_module, inner_join_module]

        The inner_join runs once per outer iteration and is therefore INSIDE the
        outer forloopflow body.  It is also added to ``visited`` so the top-level
        walker does not re-emit it.

        The outer join (the join for ``foreach_node`` itself) is NOT marked
        visited here — the caller (_visit_node) is responsible for emitting it at
        the top level after the outer forloopflow module.
        """
        body_name = foreach_node.out_funcs[0]
        body_node = self.graph[body_name]
        visited.add(body_name)

        if body_node.type == "foreach":
            # Nested foreach: the body step is itself a foreach step.
            # Layout: [body_step, inner_forloopflow, inner_join]
            body_step_module = self._build_foreach_body_step_module(
                body_node, foreach_node.name
            )
            # Recursively build the inner forloopflow body modules.
            inner_body_modules = self._build_foreach_body_modules(body_node, visited)
            inner_foreach_module = {
                "id": "foreach_%s" % body_node.name,
                "summary": "ForEach (inner): %s" % body_node.name,
                "value": {
                    "type": "forloopflow",
                    "modules": inner_body_modules,
                    "iterator": {
                        "type": "javascript",
                        "expr": (
                            "results.%s !== undefined && results.%s.foreach_values "
                            "? results.%s.foreach_values : []"
                        ) % (body_node.name, body_node.name, body_node.name),
                    },
                    "parallel": True,
                    "parallelism": self.max_workers,
                    "skip_failures": False,
                },
            }
            # The join for the inner foreach runs once per outer iteration —
            # include it inside the outer body and mark it visited so the
            # top-level walker doesn't re-emit it.
            inner_join_name = self._find_foreach_join(body_node.name)
            result = [body_step_module, inner_foreach_module]
            if inner_join_name:
                visited.add(inner_join_name)
                result.append(self._build_step_module(self.graph[inner_join_name]))
            return result
        else:
            # Simple (leaf) foreach body: one rawscript module.
            return [self._build_foreach_body_step_module(body_node, foreach_node.name)]

    def _build_foreach_body_step_module(self, body_node, parent_foreach_name: str) -> dict:
        """Build the rawscript module for a single foreach body step.

        Uses WM_ITERATION_INDEX as the split-index so each iteration runs a
        separate Metaflow task.
        """
        env_exports = self._env_export_lines()
        step_base_cmd = self._step_cmd(body_node.name)

        body_in_funcs = list(body_node.in_funcs)
        if len(body_in_funcs) == 1:
            body_input_paths_expr = '"$RUN_ID/%s/$SPLIT_INDEX"' % body_in_funcs[0]
        else:
            # Multiple parents (join of multiple foreachs) — rare but handle it.
            # Use SPLIT_INDEX for task ID of each parent.
            body_input_paths_expr = '"' + ",".join(
                "$RUN_ID/%s/$SPLIT_INDEX" % p for p in body_in_funcs
            ) + '"'

        body_script = '''\
#!/bin/bash
set -e

# Bootstrap: install metaflow using python3's own pip to ensure same Python
# Use python3 -m pip (not pip3) to avoid version mismatch between pip3 and python3
if ! python3 -c "import metaflow" 2>/dev/null; then
    python3 -m pip install --quiet "metaflow>=2.9" 2>&1 || true
fi

{env_exports}

# Restore run ID from init module (same_worker=True shares /tmp)
RUN_ID=$(cat /tmp/mf_windmill_run_id.txt 2>/dev/null || echo "")
if [ -z "$RUN_ID" ]; then
  echo "ERROR: RUN_ID not set."
  exit 1
fi

RETRY_COUNT="${{WM_FLOW_RETRY_COUNT:-0}}"
# WM_ITERATION_INDEX is set by Windmill's forloopflow for each iteration
SPLIT_INDEX="${{WM_ITERATION_INDEX:-0}}"
INPUT_PATHS={body_input_paths_expr}

{step_cmd} --run-id "$RUN_ID" --task-id "$SPLIT_INDEX" --retry-count "$RETRY_COUNT" --split-index "$SPLIT_INDEX" --input-paths "$INPUT_PATHS"
'''.format(
            env_exports=env_exports,
            step_cmd=step_base_cmd,
            body_input_paths_expr=body_input_paths_expr,
        )

        return {
            "id": body_node.name,
            "summary": "Step: %s (foreach body)" % body_node.name,
            "value": {
                "type": "rawscript",
                "content": body_script,
                "language": "bash",
                "input_transforms": {},
            },
        }

    def _build_foreach_module(self, foreach_node, visited: set) -> dict:
        """Build a Windmill forloopflow module for a Metaflow foreach step.

        Supports single-level and nested foreach (foreach inside foreach).
        For nested foreach the body modules themselves contain a forloopflow.
        """
        body_modules = self._build_foreach_body_modules(foreach_node, visited)

        # Iterator: use foreach_values output from the foreach step script.
        foreach_module = {
            "id": "foreach_%s" % foreach_node.name,
            "summary": "ForEach: %s" % foreach_node.name,
            "value": {
                "type": "forloopflow",
                "modules": body_modules,
                "iterator": {
                    "type": "javascript",
                    "expr": (
                        "results.%s !== undefined && results.%s.foreach_values "
                        "? results.%s.foreach_values : []"
                    ) % (foreach_node.name, foreach_node.name, foreach_node.name),
                },
                "parallel": True,
                "parallelism": self.max_workers,
                "skip_failures": False,
            },
        }
        return foreach_module

    def _build_parallel_module(self, split_node, visited: set) -> dict:
        """Build a Windmill parallel branchall module for a Metaflow split."""
        branches = []
        for branch_name in split_node.out_funcs:
            branch_modules = []
            self._collect_branch_modules(branch_name, branch_modules, visited)
            branches.append({
                "summary": "Branch: %s" % branch_name,
                "modules": branch_modules,
                "skip_failure": False,
                "label": branch_name,
            })

        return {
            "id": "parallel_%s" % split_node.name,
            "summary": "Parallel branches from: %s" % split_node.name,
            "value": {
                "type": "branchall",
                "branches": branches,
                "parallel": True,
            },
        }

    def _build_switch_step_module(self, node) -> dict:
        """Build a step module for a split-switch node.

        The script runs the Metaflow step normally, then reads the chosen branch
        from the _transition artifact in the datastore and outputs a JSON object
        ``{"branch": "<chosen_step_name>"}`` to stdout.  Windmill captures this
        as ``results.<step_name>`` so that the subsequent branchone predicates can
        reference ``results.<step_name>.branch``.
        """
        env_exports = self._env_export_lines()
        step_base_cmd = self._step_cmd(node.name)

        in_funcs = list(node.in_funcs)
        if node.name == "start":
            input_paths_expr = '"$RUN_ID/_parameters/windmill-params"'
        elif len(in_funcs) == 1:
            parent = in_funcs[0]
            input_paths_expr = '"$RUN_ID/%s/1"' % parent
        else:
            parent_paths = ",".join("$RUN_ID/%s/1" % p for p in in_funcs)
            input_paths_expr = '"%s"' % parent_paths

        # After the step runs, read the _transition artifact to find the chosen branch.
        # _transition is stored as a pickled Python tuple ([step_name], None).
        # We use Python inline to unpickle it and emit JSON.
        sysroot = self._datastore_root or ""
        read_transition_py = (
            "import pickle, json, sys, os; "
            "root = os.environ.get('METAFLOW_DATASTORE_SYSROOT_LOCAL', '%s'); "
            "run_id = open('/tmp/mf_windmill_run_id.txt').read().strip(); "
            "p = os.path.join(root, '.metaflow', '%s', 'data'); "
            "# read _transition from task datastore (attempt 0); "
            "import metaflow; "
            "from metaflow.datastore import FlowDataStore; "
            "from metaflow.plugins import DATASTORES; "
            "impl = next(d for d in DATASTORES if d.TYPE == 'local'); "
            "fds = FlowDataStore('%s', None, storage_impl=impl, "
            "ds_root=os.path.join(root, '.metaflow')); "
            "tds = fds.get_task_datastore(run_id, '%s', '1', attempt=0, mode='r'); "
            "tr = tds['_transition']; "
            "branch = tr[0][0] if tr and tr[0] else 'unknown'; "
            "print(json.dumps({'branch': branch}))"
        ) % (sysroot, self.name, self.name, node.name)

        script = '''\
#!/bin/bash
set -e

# Set up Metaflow environment (set PYTHONPATH first so bootstrap check uses it)
{env_exports}

# Bootstrap: install pip + metaflow if not importable.
# System python3 in Windmill container may lack pip — install it first.
# Use --break-system-packages for Ubuntu 22.04+ / Debian 12+ (PEP 668).
if ! python3 -c "import metaflow" 2>/dev/null; then
    python3 -c "import pip" 2>/dev/null || (apt-get update -q 2>/dev/null; apt-get install -y -q python3-pip 2>/dev/null) || true
    python3 -m pip install --break-system-packages --quiet "metaflow>=2.9" 2>&1 || \
    python3 -m pip install --quiet "metaflow>=2.9" 2>&1 || true
fi

# Restore run ID written by the metaflow_init module (same_worker=True shares /tmp)
RUN_ID=$(cat /tmp/mf_windmill_run_id.txt 2>/dev/null || echo "")
if [ -z "$RUN_ID" ]; then
  echo "ERROR: RUN_ID not set. Init step may have failed or same_worker is not enabled."
  exit 1
fi

# REQUIRED (Cap.RETRY): retry_count from Windmill native attempt counter
RETRY_COUNT="${{WM_FLOW_RETRY_COUNT:-0}}"

INPUT_PATHS={input_paths_expr}

{step_cmd} --run-id "$RUN_ID" --task-id 1 --retry-count "$RETRY_COUNT" --input-paths "$INPUT_PATHS"

# Read the chosen branch from the Metaflow datastore and output as JSON
# so that the branchone module can pick the correct branch at runtime.
python3 -c "{read_transition_py}"
'''.format(
            env_exports=env_exports,
            step_cmd=step_base_cmd,
            input_paths_expr=input_paths_expr,
            read_transition_py=read_transition_py,
        )

        retries, retry_delay = self._get_retry_config(node)
        timeout = self._get_timeout(node)

        module = {
            "id": node.name,
            "summary": "Step: %s (conditional split)" % node.name,
            "value": {
                "type": "rawscript",
                "content": script,
                "language": "bash",
                "input_transforms": {},
            },
        }

        if retries > 0:
            module["retry"] = {
                "constant": {
                    "attempts": retries,
                    "seconds": retry_delay,
                },
            }

        if timeout:
            module["timeout"] = timeout

        return module

    def _build_branchone_module(self, switch_node, visited: set) -> dict:
        """Build a Windmill branchone module for a Metaflow split-switch node.

        Each case in ``switch_node.switch_cases`` becomes a branch with a JS
        predicate ``results.<switch_step>.branch == '<step_name>'``.  Branch
        steps are collected until the join.  The join step is NOT marked visited
        here; the caller (_visit_node) is responsible for visiting it after this
        module so it gets emitted at the top level.
        """

        branches = []
        # switch_cases maps case_label -> step_name
        # We build one branch per case.  The last case goes into the branchone
        # "default" so we need at least one non-default branch.
        case_items = list(switch_node.switch_cases.items())
        # All branches except the last become explicit predicates;
        # the last becomes the default (Windmill requires a default).
        predicate_cases = case_items[:-1]
        default_case = case_items[-1] if case_items else None

        for _case_label, step_name in predicate_cases:
            branch_modules = []
            self._collect_branch_modules(step_name, branch_modules, visited)
            branches.append({
                "summary": step_name,
                "expr": "results.%s.branch === '%s'" % (switch_node.name, step_name),
                "modules": branch_modules,
            })

        default_modules = []
        if default_case:
            _default_label, default_step = default_case
            self._collect_branch_modules(default_step, default_modules, visited)

        return {
            "id": "switch_%s" % switch_node.name,
            "summary": "Conditional split from: %s" % switch_node.name,
            "value": {
                "type": "branchone",
                "branches": branches,
                "default": default_modules,
            },
        }

    def _collect_branch_modules(self, step_name: str, out: list, visited: set):
        """Collect modules for one branch of a parallel split until a join."""
        if step_name in visited:
            return
        node = self.graph[step_name]
        if node.type == "join":
            return
        visited.add(step_name)
        out.append(self._build_step_module(node))
        for next_step in node.out_funcs:
            self._collect_branch_modules(next_step, out, visited)

    # ------------------------------------------------------------------
    # Graph helpers
    # ------------------------------------------------------------------

    def _find_foreach_join(self, foreach_step_name: str) -> Optional[str]:
        """Return the join step name for the given foreach step."""
        for node in self.graph:
            if node.type == "join":
                parents = list(getattr(node, "split_parents", []))
                if parents and parents[-1] == foreach_step_name:
                    return node.name
        # Fallback: body's out_func
        body_name = self.graph[foreach_step_name].out_funcs[0]
        return self.graph[body_name].out_funcs[0]

    def _find_join_step(self, split_step_name: str) -> Optional[str]:
        """Return the join step for a given split step.

        For regular split nodes, the join node's split_parents[-1] equals the
        split step name.  For split-switch nodes the graph traversal does not
        push the switch onto split_parents, so we fall back to tracing through
        the branch steps to find the convergent join.
        """
        split_node = self.graph[split_step_name]

        # Fast path: regular split uses split_parents
        if split_node.type == "split":
            for node in self.graph:
                if node.type == "join":
                    parents = list(getattr(node, "split_parents", []))
                    if parents and parents[-1] == split_step_name:
                        return node.name

        # For split-switch (and as a fallback for split), follow branch out_funcs
        # until we hit a join node — that's the convergence point.
        for branch_name in split_node.out_funcs:
            branch_node = self.graph[branch_name]
            # Walk forward until we find the join
            current = branch_node
            while current is not None:
                for next_name in current.out_funcs:
                    next_node = self.graph[next_name]
                    if next_node.type == "join":
                        return next_name
                    current = next_node
                    break  # follow the first out_func
                else:
                    break

        return None
