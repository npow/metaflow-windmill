"""DeployedFlow and TriggeredRun objects for the Windmill Deployer plugin."""

from __future__ import annotations

import json
import os
import sys
from typing import TYPE_CHECKING, ClassVar, Optional

import metaflow
from metaflow.exception import MetaflowNotFound
from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo
from metaflow.runner.subprocess_manager import SubprocessManager

if TYPE_CHECKING:
    import metaflow.runner.deployer_impl


def _find_flow_for_run_id(sysroot: str, run_id: str) -> Optional[str]:
    """Scan the local datastore to find which flow owns *run_id*."""
    mf_root = os.path.join(sysroot, ".metaflow")
    if not os.path.isdir(mf_root):
        return None
    for entry in os.listdir(mf_root):
        if os.path.isdir(os.path.join(mf_root, entry, run_id)):
            return entry
    return None


def _make_stub_deployer(name: str):
    """Return a minimal deployer stub for recovery without a flow file."""
    from .windmill_deployer import WindmillDeployer

    stub = object.__new__(WindmillDeployer)
    stub._deployer_kwargs = {}
    stub.flow_file = ""
    stub.show_output = False
    stub.profile = None
    stub.env = None
    stub.cwd = os.getcwd()
    stub.file_read_timeout = 3600
    stub.env_vars = os.environ.copy()
    stub.spm = SubprocessManager()
    stub.top_level_kwargs = {}
    stub.api = None
    stub.name = name
    stub.flow_name = name
    stub.metadata = "{}"
    stub.additional_info = {}
    return stub


class WindmillTriggeredRun(TriggeredRun):
    """A Windmill job that was triggered via the Deployer API."""

    def __init__(self, deployer, content: str):
        super().__init__(deployer, content)
        self._content = json.loads(content)
        self._metadata_configured = False
        self._debug_logged = False

    @property
    def _metadata(self) -> dict:
        return self._content

    @property
    def windmill_ui(self) -> Optional[str]:
        """URL to the Windmill UI for this job."""
        return self._metadata.get("job_url")

    # ------------------------------------------------------------------
    # Metadata setup — called once, sets env vars permanently.
    #
    # The key insight: metaflow.Run() uses lazy evaluation. When the
    # caller accesses run.finished or run.successful, the Metaflow client
    # reads data from the local datastore at that moment. If we
    # save/restore env vars around Run() creation, the lazy reads happen
    # after restore — with the WRONG datastore path. So we set the env
    # vars once and leave them set.
    # ------------------------------------------------------------------

    def _ensure_metadata(self):
        """Configure local metadata provider to point at the deployer's sysroot."""
        if self._metadata_configured:
            return
        self._metadata_configured = True

        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")

        if meta_type == "local" and not sysroot:
            sysroot = os.path.expanduser("~")

        if sysroot:
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
        if meta_type:
            os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type

        # "local@path" calls compute_info() which sets LocalStorage.datastore_root.
        if sysroot and meta_type == "local":
            mf_dir = os.path.join(sysroot, ".metaflow")
            if os.path.isdir(mf_dir):
                metaflow.metadata("local@%s" % sysroot)
            else:
                metaflow.metadata("local")
        elif meta_type:
            metaflow.metadata(meta_type)

        metaflow.namespace(None)

    # ------------------------------------------------------------------
    # Pathspec correction — the flow name in the pathspec might not match
    # the actual directory in the datastore (e.g. UNKNOWN/run_id).
    # ------------------------------------------------------------------

    def _resolve_pathspec(self) -> Optional[str]:
        """Return a corrected pathspec, or None if the run doesn't exist yet."""
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
            # Debug: log what we're looking for vs what exists
            flow_dir = os.path.join(sysroot, ".metaflow", flow_name)
            if not self._debug_logged:
                import sys as _sys
                print(
                    "[_resolve_pathspec] pathspec=%r, run_dir exists=%s, "
                    "flow_dir exists=%s, flow_dir contents=%s"
                    % (
                        pathspec,
                        os.path.isdir(run_dir),
                        os.path.isdir(flow_dir),
                        os.listdir(flow_dir) if os.path.isdir(flow_dir) else "N/A",
                    ),
                    file=_sys.stderr,
                )
                self._debug_logged = True

        # Flow name might be wrong (e.g. "UNKNOWN"). Scan the sysroot.
        if sysroot and run_id:
            actual_flow = _find_flow_for_run_id(sysroot, run_id)
            if actual_flow:
                corrected = "%s/%s" % (actual_flow, run_id)
                self.pathspec = corrected
                return corrected

        return pathspec

    # ------------------------------------------------------------------
    # The run property — simple: configure metadata, resolve pathspec,
    # create Run. No save/restore, no subprocess fallback, no Windmill
    # API fallback.
    # ------------------------------------------------------------------

    @property
    def run(self):
        """Retrieve the metaflow.Run object for this triggered run."""
        self._ensure_metadata()
        pathspec = self._resolve_pathspec()
        if not pathspec:
            return None
        try:
            return metaflow.Run(pathspec, _namespace_check=False)
        except MetaflowNotFound:
            # Debug: log what we tried and what exists on disk
            sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL", "")
            if sysroot:
                mf_root = os.path.join(sysroot, ".metaflow")
                import sys as _sys
                print(
                    "[WindmillTriggeredRun.run] MetaflowNotFound for pathspec=%r, "
                    "sysroot=%r, .metaflow exists=%s, contents=%s"
                    % (
                        pathspec,
                        sysroot,
                        os.path.isdir(mf_root),
                        os.listdir(mf_root) if os.path.isdir(mf_root) else "N/A",
                    ),
                    file=_sys.stderr,
                )
            return None

    # ------------------------------------------------------------------
    # Filesystem-based completion check. Used as fallback in status when
    # metaflow.Run() isn't available yet (data hasn't landed).
    # ------------------------------------------------------------------

    def _check_sysroot_completion(self) -> Optional[str]:
        """Check the local datastore directly for run completion."""
        import glob

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

        # Check if end step completed
        for pattern in [
            os.path.join(run_dir, "end", "*", "0.DONE.lock"),
            os.path.join(run_dir, "end", "*", "0.task_end"),
            os.path.join(run_dir, "end", "*", "_meta", "0_artifact__task_ok.json"),
        ]:
            if glob.glob(pattern):
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
        import uuid

        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        windmill_host = additional_info.get("windmill_host", "http://localhost:8000")
        windmill_token = additional_info.get("windmill_token", "")
        windmill_workspace = additional_info.get("windmill_workspace", "admins")
        flow_path = additional_info.get("flow_path")

        if not flow_path:
            from .windmill_compiler import flow_name_to_path
            flow_path = flow_name_to_path(self.name)

        try:
            import requests
        except ImportError:
            raise RuntimeError(
                "The `requests` package is required to trigger Windmill flows."
            )

        session = requests.Session()
        if windmill_token:
            session.headers["Authorization"] = "Bearer %s" % windmill_token

        url = "%s/api/w/%s/jobs/run/f/%s" % (
            windmill_host, windmill_workspace, flow_path
        )
        run_id = "windmill-" + str(uuid.uuid4()).replace("-", "")[:16]
        payload = {k: str(v) for k, v in kwargs.items()}
        payload["METAFLOW_RUN_ID"] = run_id
        resp = session.post(url, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                "Failed to trigger Windmill flow (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )

        job_id = resp.text.strip().strip('"')
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
            deployer = WindmillDeployer(
                flow_file=info.get("flow_file") or "", deployer_kwargs={}
            )
            deployer.name = info["name"]
            deployer.flow_name = info["flow_name"]
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                k: v
                for k, v in info.items()
                if k not in ("name", "flow_name", "flow_file")
            }
        else:
            windmill_host = os.environ.get("WINDMILL_HOST", "http://localhost:8000")
            windmill_token = os.environ.get("WINDMILL_TOKEN", "")
            windmill_workspace = os.environ.get("WINDMILL_WORKSPACE", "admins")

            # REQUIRED (Cap.FROM_DEPLOYMENT): handle dotted names
            flow_name = identifier.split(".")[-1]
            flow_path = flow_name_to_path(flow_name)

            deployer = _make_stub_deployer(flow_name)
            deployer.name = flow_name
            deployer.flow_name = flow_name
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                "flow_path": flow_path,
                "windmill_host": windmill_host,
                "windmill_token": windmill_token,
                "windmill_workspace": windmill_workspace,
            }

        return cls(deployer=deployer)
