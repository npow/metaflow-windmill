"""DeployedFlow and TriggeredRun objects for the Windmill Deployer plugin.

WindmillDeployedFlow handles deploying and triggering Windmill flows.
WindmillTriggeredRun tracks a running Windmill job and bridges it back
to the Metaflow client (metaflow.Run) so callers can check status/results.

Key design decisions:
  - Environment variables for metadata/datastore are set permanently (not
    saved/restored) because metaflow.Run() uses lazy evaluation — reads
    happen later when the caller accesses run.finished or run.successful.
  - Task IDs use non-integer format (windmill-N) so that the local metadata
    provider's register_task_id() calls _new_task(), which creates the
    step-level _meta/_self.json required by metaflow.Run().
  - The Metaflow run_id is pre-computed by the deployer and passed to the
    Windmill flow as METAFLOW_RUN_ID, so the deployer's pathspec matches
    the actual run in the local datastore.
"""

from __future__ import annotations

import json
import os
import sys
from typing import ClassVar, Optional

import metaflow
from metaflow.exception import MetaflowNotFound
from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo


def _find_flow_for_run_id(sysroot: str, run_id: str) -> Optional[str]:
    """Scan ``<sysroot>/.metaflow/*/`` to find which flow directory contains *run_id*."""
    mf_root = os.path.join(sysroot, ".metaflow")
    if not os.path.isdir(mf_root):
        return None
    for entry in os.listdir(mf_root):
        if os.path.isdir(os.path.join(mf_root, entry, run_id)):
            return entry
    return None


class WindmillTriggeredRun(TriggeredRun):
    """A Windmill job that was triggered via the Deployer API."""

    def __init__(self, deployer, content: str):
        super().__init__(deployer, content)
        self._content = json.loads(content)
        self._metadata_configured = False

    @property
    def _metadata(self) -> dict:
        return self._content

    @property
    def windmill_ui(self) -> Optional[str]:
        """URL to the Windmill UI for this job."""
        return self._metadata.get("job_url")

    # ------------------------------------------------------------------
    # Metadata setup
    # ------------------------------------------------------------------

    def _ensure_metadata(self):
        """Configure local metadata/datastore so metaflow.Run() can find data.

        Called on every access to ``run`` / ``status``.

        First call: sets METAFLOW_DATASTORE_SYSROOT_LOCAL and
        METAFLOW_DEFAULT_METADATA as *permanent* env vars.  They must stay
        set because metaflow.Run() is lazy — actual datastore reads happen
        later (e.g. when the caller checks ``run.finished``).

        Every call: if sysroot/.metaflow exists, calls
        ``metaflow.metadata("local@<sysroot>")`` to set
        ``LocalStorage.datastore_root`` (deferred until the directory appears
        because ``compute_info()`` would fail without it).
        """
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")

        if meta_type == "local" and not sysroot:
            sysroot = os.path.expanduser("~")

        # Env vars — set once on the first call.
        if not self._metadata_configured:
            self._metadata_configured = True
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
            if meta_type and meta_type != "local":
                metaflow.metadata(meta_type)
            metaflow.namespace(None)

        # For the local provider we need to call metadata("local@<path>") to
        # configure LocalStorage.datastore_root.  This only works once the
        # .metaflow directory exists, so we retry on every call until it does.
        if sysroot and meta_type == "local":
            mf_dir = os.path.join(sysroot, ".metaflow")
            if os.path.isdir(mf_dir):
                metaflow.metadata("local@%s" % sysroot)

    def _resolve_pathspec(self) -> Optional[str]:
        """Return a corrected pathspec, or None if the run directory doesn't exist.

        The flow name in the pathspec may not match the actual datastore
        directory (e.g. "UNKNOWN" from _trigger_direct).  If the expected
        path doesn't exist, scans the sysroot to find the real flow name.
        """
        pathspec = self.pathspec
        if not pathspec or "/" not in pathspec:
            return None

        flow_name, run_id = pathspec.split("/", 1)
        sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")

        # If the pathspec already points to a real directory, use it.
        if sysroot:
            run_dir = os.path.join(sysroot, ".metaflow", flow_name, run_id)
            if os.path.isdir(run_dir):
                return pathspec

        # Flow name might be wrong (e.g. "UNKNOWN"). Scan the sysroot.
        if sysroot and run_id:
            actual_flow = _find_flow_for_run_id(sysroot, run_id)
            if actual_flow:
                corrected = "%s/%s" % (actual_flow, run_id)
                self.pathspec = corrected
                return corrected

        return pathspec

    @property
    def run(self):
        """Return a ``metaflow.Run`` for this triggered run, or None if not ready."""
        self._ensure_metadata()
        pathspec = self._resolve_pathspec()
        if not pathspec:
            return None
        try:
            return metaflow.Run(pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None

    def _check_sysroot_completion(self) -> Optional[str]:
        """Check the local datastore directly for completion (fallback).

        Used when ``metaflow.Run()`` returns None (data hasn't landed yet).
        Looks for the ``_task_ok`` artifact in the ``end`` step directory
        as evidence that the flow finished successfully.
        """
        sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")
        if not sysroot:
            return None

        pathspec = self.pathspec
        if not pathspec or "/" not in pathspec:
            return None

        flow_name, run_id = pathspec.split("/", 1)
        run_dir = os.path.join(sysroot, ".metaflow", flow_name, run_id)
        if not os.path.isdir(run_dir):
            return None

        # Look for completion markers in end/<task_id>/
        end_dir = os.path.join(run_dir, "end")
        if not os.path.isdir(end_dir):
            return "RUNNING"
        for task_dir in os.listdir(end_dir):
            task_path = os.path.join(end_dir, task_dir)
            if not os.path.isdir(task_path):
                continue
            # Any of these files indicate the end step completed
            for marker in ("0.DONE.lock", "0.task_end"):
                if os.path.exists(os.path.join(task_path, marker)):
                    return "SUCCEEDED"
            meta_marker = os.path.join(task_path, "_meta", "0_artifact__task_ok.json")
            if os.path.exists(meta_marker):
                return "SUCCEEDED"

        return "RUNNING"

    @property
    def status(self) -> Optional[str]:
        """Return a status string for this run."""
        self._ensure_metadata()
        run = self.run
        if run is None:
            return self._check_sysroot_completion() or "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class WindmillDeployedFlow(DeployedFlow):
    """A Metaflow flow deployed as a Windmill flow."""

    TYPE: ClassVar[Optional[str]] = "windmill"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for from_deployment."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            **additional_info,
        })

    def run(self, **kwargs) -> WindmillTriggeredRun:
        """Trigger a new execution of this deployed Windmill flow."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        flow_file = getattr(self.deployer, "flow_file", "") or ""

        # When recovered via from_deployment() with no flow file, trigger directly.
        if not flow_file:
            return self._trigger_direct(**kwargs)

        # REQUIRED (Cap.RUN_PARAMS): must be list, not tuple.
        run_params = list("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = {"deployer_attribute_file": attribute_file_path}
            if run_params:
                trigger_kwargs["run_params"] = run_params
            for key in (
                "flow_path",
                "windmill_host",
                "windmill_token",
                "windmill_workspace",
            ):
                val = additional_info.get(key)
                if val:
                    trigger_kwargs[key] = val

            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return WindmillTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            "Error triggering Windmill flow %r" % self.deployer.flow_file
        )

    def trigger(self, run_params=None, **kwargs) -> "WindmillTriggeredRun":
        """Trigger a new execution; alias for run() that also accepts run_params.

        REQUIRED (Cap.RUN_PARAMS): run_params must be a list, not a tuple.
        """
        run_params = list(run_params) if run_params else []
        for kv in run_params:
            k, _, v = kv.partition("=")
            kwargs.setdefault(k.strip(), v.strip())
        return self.run(**kwargs)

    def _trigger_direct(self, **kwargs) -> "WindmillTriggeredRun":
        """Trigger a Windmill flow directly via REST API (no flow file needed)."""
        from .windmill_cli import trigger_windmill_flow

        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        windmill_host = additional_info.get("windmill_host", "http://localhost:8000")
        windmill_token = additional_info.get("windmill_token", "")
        windmill_workspace = additional_info.get("windmill_workspace", "admins")
        flow_path = additional_info.get("flow_path")

        if not flow_path:
            from .windmill_compiler import flow_name_to_path
            flow_path = flow_name_to_path(self.name)

        inputs = {k: str(v) for k, v in kwargs.items()}
        job_id, run_id = trigger_windmill_flow(
            host=windmill_host,
            token=windmill_token,
            workspace=windmill_workspace,
            flow_path=flow_path,
            inputs=inputs,
        )

        pathspec = "%s/%s" % (self.deployer.flow_name or "UNKNOWN", run_id)
        content_dict = {
            "pathspec": pathspec,
            "name": self.name,
            "job_id": job_id,
            "job_url": "%s/run/%s?workspace=%s" % (
                windmill_host, job_id, windmill_workspace
            ),
            "metadata": "{}",
        }
        return WindmillTriggeredRun(
            deployer=self.deployer, content=json.dumps(content_dict)
        )

    @classmethod
    def from_deployment(
        cls, identifier: str, metadata: Optional[str] = None
    ) -> "WindmillDeployedFlow":
        """Recover a WindmillDeployedFlow from a deployment identifier.

        identifier can be:
        - A JSON string produced by :attr:`id` (preferred).
        - A plain flow path or name string.

        REQUIRED (Cap.FROM_DEPLOYMENT): handles dotted names (project.branch.FlowName).
        Uses only the last component as the Python class name.
        """
        from .windmill_deployer import WindmillDeployer
        from .windmill_compiler import flow_name_to_path

        info = None
        if identifier.startswith("{"):
            try:
                info = json.loads(identifier)
            except (ValueError, TypeError):
                pass

        if info is not None:
            name = info["name"]
            flow_name = info["flow_name"]
            additional_info = {
                k: v
                for k, v in info.items()
                if k not in ("name", "flow_name", "flow_file")
            }
        else:
            # REQUIRED (Cap.FROM_DEPLOYMENT): handle dotted names.
            # If identifier already contains '/' it's a Windmill path — use as-is.
            if "/" in identifier:
                name = identifier.split("/")[-1]
                flow_path = identifier
            else:
                name = identifier.split(".")[-1]
                flow_path = flow_name_to_path(name)

            flow_name = name
            additional_info = {
                "flow_path": flow_path,
                "windmill_host": os.environ.get("WINDMILL_HOST", "http://localhost:8000"),
                "windmill_token": os.environ.get("WINDMILL_TOKEN", ""),
                "windmill_workspace": os.environ.get("WINDMILL_WORKSPACE", "admins"),
            }

        deployer = WindmillDeployer.create_stub(name, additional_info)
        deployer.flow_name = flow_name
        deployer.metadata = metadata or "{}"
        return cls(deployer=deployer)
