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

    def _resolve_run_id_from_windmill(self) -> Optional[str]:
        """Query the Windmill API to find the actual Metaflow run_id.

        The init module prints the RUN_ID as its last stdout line (the module result).
        We find the init module's job and read its result to get the actual run_id.
        """
        try:
            import requests

            metadata = self._metadata
            job_id = metadata.get("job_id")
            if not job_id:
                return None

            additional_info = getattr(self.deployer, "additional_info", {}) or {}
            windmill_host = additional_info.get("windmill_host", "http://localhost:8000")
            windmill_token = additional_info.get("windmill_token", "")
            windmill_workspace = additional_info.get("windmill_workspace", "admins")

            session = requests.Session()
            if windmill_token:
                session.headers["Authorization"] = "Bearer %s" % windmill_token

            # Get the flow job to find the init module job
            job_url = "%s/api/w/%s/jobs_u/get/%s" % (
                windmill_host, windmill_workspace, job_id
            )
            resp = session.get(job_url)
            if resp.status_code != 200:
                return None
            job_data = resp.json()

            # Find the metaflow_init module job
            modules = job_data.get("flow_status", {}).get("modules", [])
            init_job_id = None
            for m in modules:
                if m.get("id") == "metaflow_init" and m.get("job"):
                    init_job_id = m["job"]
                    break

            if not init_job_id:
                return None

            # Get the init job result (last stdout line = the RUN_ID)
            init_url = "%s/api/w/%s/jobs_u/get/%s" % (
                windmill_host, windmill_workspace, init_job_id
            )
            resp = session.get(init_url)
            if resp.status_code != 200:
                return None
            init_data = resp.json()

            result = init_data.get("result", "")
            if isinstance(result, str) and "windmill-" in result:
                # The init module last line is e.g.:
                # "Initialized Metaflow run: windmill-<uuid>" (from our echo)
                # OR just "windmill-<uuid>" if the echo IS the last line
                idx = result.rfind("windmill-")
                run_id_candidate = result[idx:].strip()
                if run_id_candidate.startswith("windmill-"):
                    return run_id_candidate
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
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            # Set sysroot FIRST so metaflow.metadata() picks it up correctly
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                # Use "local@path" form to call compute_info() which properly sets
                # LocalStorage.datastore_root and verifies the .metaflow dir exists.
                # This bypasses any class-level datastore_root caching issues.
                if sysroot and meta_type == "local":
                    # Use "local@path" to call compute_info() which properly sets
                    # LocalStorage.datastore_root. Only works when .metaflow exists.
                    mf_dir = os.path.join(sysroot, ".metaflow")
                    if os.path.isdir(mf_dir):
                        metaflow.metadata("local@%s" % sysroot)
                    else:
                        metaflow.metadata(meta_type)
                else:
                    metaflow.metadata(meta_type)

            pathspec = self.pathspec
            if pathspec and pathspec.startswith("UNKNOWN/"):
                run_id = pathspec.split("/", 1)[1]
                flow_name = _find_flow_for_run_id(run_id)
                if flow_name:
                    pathspec = "%s/%s" % (flow_name, run_id)
                    self.pathspec = pathspec

            # Try the direct pathspec first
            try:
                run = metaflow.Run(pathspec, _namespace_check=False)
                return run
            except MetaflowNotFound:
                pass

            # Fallback: use fresh subprocess to verify run exists (bypasses any
            # class-level LocalStorage.datastore_root caching in this process).
            # If subprocess finds run, retry in this process.
            if sysroot:
                import subprocess as _sp
                _check = _sp.run(
                    [__import__("sys").executable, "-c", """
import os, sys
os.environ['METAFLOW_DEFAULT_DATASTORE'] = 'local'
os.environ['METAFLOW_DEFAULT_METADATA'] = 'local'
os.environ['METAFLOW_DATASTORE_SYSROOT_LOCAL'] = sys.argv[1]
from metaflow.plugins.datastores.local_storage import LocalStorage
LocalStorage.datastore_root = None
import metaflow
try:
    metaflow.metadata('local@' + sys.argv[1])
    r = metaflow.Run(sys.argv[2], _namespace_check=False)
    print('ok:' + sys.argv[2])
except Exception as e:
    print('err:' + str(e)[:100])
""", sysroot, pathspec],
                    capture_output=True, text=True, timeout=10
                )
                if _check.stdout.strip().startswith("ok:"):
                    # Subprocess found it — retry with fresh local@path metadata setup
                    metaflow.metadata("local@%s" % sysroot)
                    try:
                        run = metaflow.Run(pathspec, _namespace_check=False)
                        return run
                    except Exception:
                        pass

            # Pathspec not found - query Windmill to find the actual run_id
            actual_run_id = self._resolve_run_id_from_windmill()
            if actual_run_id:
                flow_name = pathspec.split("/")[0]
                if flow_name == "UNKNOWN":
                    flow_name = _find_flow_for_run_id(actual_run_id) or "UNKNOWN"
                new_pathspec = "%s/%s" % (flow_name, actual_run_id)
                self.pathspec = new_pathspec
                try:
                    run = metaflow.Run(new_pathspec, _namespace_check=False)
                    return run
                except MetaflowNotFound:
                    pass

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

    def _check_sysroot_completion(self) -> Optional[str]:
        """Bypass metaflow.Run() and check the sysroot directly for run completion.

        This handles cases where metaflow.Run() fails due to class-level caching of
        LocalStorage.datastore_root across test runs in the same pytest session.
        Checks for 0.DONE.lock in the end step directory as a completion signal.
        """
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        if not sysroot:
            return None
        pathspec = self.pathspec
        if not pathspec or "/" not in pathspec:
            return None
        flow_name, run_id = pathspec.split("/", 1)
        run_dir = os.path.join(sysroot, ".metaflow", flow_name, run_id)
        if not os.path.isdir(run_dir):
            return None
        # Check if end step completed via 0.DONE.lock or 0.task_end
        import glob
        done_patterns = [
            os.path.join(run_dir, "end", "*", "0.DONE.lock"),
            os.path.join(run_dir, "end", "*", "0.task_end"),
        ]
        for pattern in done_patterns:
            if glob.glob(pattern):
                return "SUCCEEDED"
        # Check for task_ok artifact indicating successful end
        end_meta = glob.glob(os.path.join(run_dir, "end", "*", "_meta", "0_artifact__task_ok.json"))
        if end_meta:
            return "SUCCEEDED"
        # Run directory exists but end step not done yet
        return "RUNNING"

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            # Fallback: check filesystem directly (avoids metaflow.Run() caching issues)
            sysroot_status = self._check_sysroot_completion()
            if sysroot_status:
                return sysroot_status
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
        # Pre-compute a stable run_id and pass it to the flow as METAFLOW_RUN_ID
        # so the init module uses the exact same ID we record in the pathspec.
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
