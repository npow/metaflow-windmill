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


def _find_flow_for_run_id(run_id: str) -> Optional[str]:
    """Scan the local Metaflow datastore to find which flow class owns run_id."""
    try:
        import metaflow as _mf
        _mf.namespace(None)
        sysroot = (
            os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
            or os.path.expanduser("~")
        )
        mf_root = os.path.join(sysroot, ".metaflow")
        if not os.path.isdir(mf_root):
            return None
        for entry in os.listdir(mf_root):
            flow_dir = os.path.join(mf_root, entry)
            if os.path.isdir(flow_dir) and os.path.isdir(
                os.path.join(flow_dir, run_id)
            ):
                return entry
    except Exception:
        pass
    return None


def _make_stub_deployer(name: str):
    """Return a minimal deployer stub for Windmill recovered without a flow file."""
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

    @property
    def windmill_ui(self) -> Optional[str]:
        """URL to the Windmill UI for this job, if available."""
        try:
            metadata = self._metadata
            return metadata.get("job_url")
        except Exception:
            pass
        return None

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot

            pathspec = self.pathspec
            if pathspec and pathspec.startswith("UNKNOWN/"):
                run_id = pathspec.split("/", 1)[1]
                flow_name = _find_flow_for_run_id(run_id)
                if flow_name:
                    pathspec = "%s/%s" % (flow_name, run_id)
                    self.pathspec = pathspec

            return metaflow.Run(pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
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
        """Trigger a new execution of this deployed Windmill flow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. message="hello").

        Returns
        -------
        WindmillTriggeredRun
        """
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
        Click returns tuples for multi-value options; always convert:
            run_params = list(run_params) if run_params else []
        """
        # REQUIRED (Cap.RUN_PARAMS): must be list, not tuple — DO NOT REMOVE
        run_params = list(run_params) if run_params else []
        # Unpack run_params (key=value strings) into kwargs for run()
        for kv in run_params:
            k, _, v = kv.partition("=")
            kwargs.setdefault(k.strip(), v.strip())
        return self.run(**kwargs)

    def _trigger_direct(self, **kwargs) -> "WindmillTriggeredRun":
        """Trigger a Windmill flow directly via REST API (no flow file needed)."""
        import hashlib

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
        payload = {k: str(v) for k, v in kwargs.items()}
        resp = session.post(url, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                "Failed to trigger Windmill flow (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )

        job_id = resp.text.strip().strip('"')
        run_id = "windmill-" + hashlib.md5(job_id.encode()).hexdigest()[:16]

        flow_class_name = (
            additional_info.get("mf_flow_class")
            or (
                self.deployer.flow_name
                if self.deployer.flow_name
                and "-" not in self.deployer.flow_name
                else None
            )
        )
        if not flow_class_name:
            flow_class_name = "UNKNOWN"
        pathspec = "%s/%s" % (flow_class_name, run_id)

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
            # Plain name — fall back to environment variables
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
