"""
Windmill flow compiler for Metaflow flows.

Converts a Metaflow FlowGraph into a Windmill OpenFlow JSON definition.
Each Metaflow step becomes a Windmill flow module that runs a bash script.

Supported Metaflow graph patterns:
  linear       - sequential modules
  split/join   - parallel branches (branchall modules)
  foreach      - forloopflow modules (single level)
  nested foreach - NOT supported (raises NotSupportedException)
  split-switch - NOT supported (raises NotSupportedException)

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
            raise NotSupportedException(
                "Step *%s* uses a conditional split (@condition) which is not "
                "supported with Windmill." % step_name
            )

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
        # Forward PYTHONPATH so Windmill workers can find metaflow source.
        # When running locally, PYTHONPATH may contain the metaflow source path.
        # We filter to only include paths that contain a 'metaflow' package
        # (the OSS source) and exclude paths that would load NFLX-internal
        # extensions that aren't available in the Windmill worker container.
        current_pythonpath = os.environ.get("PYTHONPATH", "")
        if current_pythonpath:
            import sys
            filtered_paths = []
            for p in current_pythonpath.split(os.pathsep):
                if not p:
                    continue
                # Include paths that contain the metaflow package itself
                mf_pkg = os.path.join(p, "metaflow", "__init__.py")
                if os.path.isfile(mf_pkg):
                    filtered_paths.append(p)
            if filtered_paths:
                env["PYTHONPATH"] = os.pathsep.join(filtered_paths)
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
        param_collection = "\n".join(
            'MF_PARAM_%s="${%s:-}"' % (pname.upper(), pname)
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
        base_init_cmd += " init --run-id $RUN_ID --task-id 1"
        for tag in self.tags:
            base_init_cmd += " --tag " + tag

        env_exports = self._env_export_lines()

        script = '''\
#!/bin/bash
set -e

# Set up Metaflow environment
{env_exports}

# Collect parameters from Windmill flow inputs
{param_collection}
ORIGIN_RUN_ID="${{ORIGIN_RUN_ID:-}}"

# Use the pre-computed run ID passed as METAFLOW_RUN_ID flow input.
# This ensures the pathspec in the deployer matches the actual Metaflow run.
if [ -n "${{METAFLOW_RUN_ID:-}}" ]; then
  RUN_ID="$METAFLOW_RUN_ID"
else
  # Fallback: derive from timestamp (should not normally reach here)
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
            input_paths_expr = '"$RUN_ID/_parameters/1"'
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

# Set up Metaflow environment
{env_exports}

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

    def _build_foreach_module(self, foreach_node, visited: set) -> dict:
        """Build a Windmill forloopflow module for a Metaflow foreach step."""
        body_name = foreach_node.out_funcs[0]
        body_node = self.graph[body_name]

        if body_node.type == "foreach":
            raise NotSupportedException(
                "Nested foreach (foreach inside foreach) is not supported with Windmill. "
                "Step *%s* contains a nested foreach step *%s*." % (
                    foreach_node.name, body_name
                )
            )

        visited.add(body_name)
        join_name = self._find_foreach_join(foreach_node.name)
        if join_name:
            visited.add(join_name)

        env_exports = self._env_export_lines()
        step_base_cmd = self._step_cmd(body_name)

        # Body script: uses WM_ITERATION_INDEX for split-index
        # RUN_ID is shared via /tmp file since same_worker=True.
        # The foreach parent step is the input for the body step.
        body_node = self.graph[body_name]
        body_in_funcs = list(body_node.in_funcs)
        if len(body_in_funcs) == 1:
            body_input_paths_expr = '"$RUN_ID/%s/1"' % body_in_funcs[0]
        else:
            body_input_paths_expr = '"' + ",".join(
                "$RUN_ID/%s/1" % p for p in body_in_funcs
            ) + '"'

        body_script = '''\
#!/bin/bash
set -e

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

        body_module = {
            "id": body_name,
            "summary": "Step: %s (foreach body)" % body_name,
            "value": {
                "type": "rawscript",
                "content": body_script,
                "language": "bash",
                "input_transforms": {},
            },
        }

        # Iterator: use values from foreach parent step's foreach_values output
        # The foreach parent step (bash) must print JSON array of values to stdout
        foreach_module = {
            "id": "foreach_%s" % foreach_node.name,
            "summary": "ForEach: %s" % foreach_node.name,
            "value": {
                "type": "forloopflow",
                "modules": [body_module],
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
        """Return the join step for a given split step."""
        for node in self.graph:
            if node.type == "join":
                parents = list(getattr(node, "split_parents", []))
                if parents and parents[-1] == split_step_name:
                    return node.name
        return None
