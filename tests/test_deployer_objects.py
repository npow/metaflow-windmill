"""Unit tests for WindmillDeployedFlow.from_deployment +
WindmillTriggeredRun.status (Windmill API path)."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

from metaflow_extensions.windmill.plugins.windmill.windmill_deployer_objects import (
    WindmillDeployedFlow,
    WindmillTriggeredRun,
)


class TestFromDeploymentToken:
    """Regression: from_deployment must populate windmill_token from the
    environment when the deployment id omits it (which it does by design,
    see WindmillDeployedFlow.id's safe_info filter). Without this fallback,
    _trigger_direct() sends an empty Authorization header and Windmill
    returns HTTP 401 Unauthorized — surfaced by metaflow's
    test_from_deployment ux test."""

    @staticmethod
    def _identifier(**overrides) -> str:
        info = {
            "name": "MyFlow",
            "flow_name": "MyFlow",
            "flow_file": None,
            "flow_path": "u/admin/myflow",
            "windmill_host": "http://localhost:8000",
            "windmill_workspace": "admins",
            # NOTE: windmill_token deliberately omitted (mirrors the id property).
        }
        info.update(overrides)
        return json.dumps(info)

    def test_token_pulled_from_env_when_missing_from_id(self):
        env = {
            "WINDMILL_TOKEN": "fresh-runtime-token",
            "WINDMILL_HOST": "http://localhost:8000",
            "WINDMILL_WORKSPACE": "admins",
        }
        with patch.dict(os.environ, env, clear=False):
            recovered = WindmillDeployedFlow.from_deployment(self._identifier())
        info = recovered.deployer.additional_info
        assert info.get("windmill_token") == "fresh-runtime-token"

    def test_id_token_preserved_when_already_present(self):
        # If a token IS present in the JSON (callers can opt to include it),
        # don't override it with the env var.
        identifier = self._identifier(windmill_token="explicit-id-token")
        env = {"WINDMILL_TOKEN": "different-env-token"}
        with patch.dict(os.environ, env, clear=False):
            recovered = WindmillDeployedFlow.from_deployment(identifier)
        info = recovered.deployer.additional_info
        assert info.get("windmill_token") == "explicit-id-token"

    def test_empty_env_does_not_inject_empty_token(self):
        # Don't accidentally insert an empty-string token if env is unset —
        # leaves additional_info clean for _trigger_direct's `.get(..., "")`.
        env = {k: v for k, v in os.environ.items() if k != "WINDMILL_TOKEN"}
        with patch.dict(os.environ, env, clear=True):
            recovered = WindmillDeployedFlow.from_deployment(self._identifier())
        info = recovered.deployer.additional_info
        assert "windmill_token" not in info or info["windmill_token"] == ""


class TestTriggeredRunStatusFromAPI:
    """Regression: status must consult the Windmill API, not just the local
    filesystem. _check_sysroot_completion returns RUNNING forever for flows
    that crash before reaching the end step (no end/ dir is ever written),
    causing test_fail_flow_reports_failed_status to time out at 300s.
    Querying the Windmill API gives the real terminal state."""

    @staticmethod
    def _make_triggered(job_id: str = "j-123", host: str = "http://localhost:8000"):
        deployer = MagicMock()
        deployer.additional_info = {
            "windmill_host": host,
            "windmill_token": "tok",
            "windmill_workspace": "admins",
        }
        deployer.env_vars = {}
        content = json.dumps({
            "pathspec": "MyFlow/wm-abc",
            "name": "MyFlow",
            "job_id": job_id,
            "job_url": f"{host}/run/{job_id}",
            "metadata": "{}",
        })
        return WindmillTriggeredRun(deployer=deployer, content=content)

    def _stub_response(self, status_code: int, json_body: dict):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_body
        return resp

    def test_completed_failed_returns_failed(self):
        triggered = self._make_triggered()
        # No metaflow Run yet (filesystem is empty), force the
        # `if run is None` branch to use the API status.
        with patch.object(WindmillTriggeredRun, "run", new_callable=lambda: property(lambda self: None)):
            with patch("requests.get", return_value=self._stub_response(
                200, {"type": "CompletedJob", "success": False}
            )):
                assert triggered.status == "FAILED"

    def test_completed_succeeded_returns_succeeded(self):
        triggered = self._make_triggered()
        with patch.object(WindmillTriggeredRun, "run", new_callable=lambda: property(lambda self: None)):
            with patch("requests.get", return_value=self._stub_response(
                200, {"type": "CompletedJob", "success": True}
            )):
                assert triggered.status == "SUCCEEDED"

    def test_still_running_returns_running(self):
        triggered = self._make_triggered()
        with patch.object(WindmillTriggeredRun, "run", new_callable=lambda: property(lambda self: None)):
            with patch("requests.get", return_value=self._stub_response(
                200, {"type": "QueuedJob", "success": False}
            )):
                # type != CompletedJob → still running per the Windmill API.
                # _check_sysroot_completion would fall through to PENDING here,
                # but the API response wins.
                assert triggered.status == "RUNNING"

    def test_api_unavailable_falls_back_to_filesystem(self):
        triggered = self._make_triggered()
        with patch.object(WindmillTriggeredRun, "run", new_callable=lambda: property(lambda self: None)):
            with patch("requests.get", side_effect=Exception("network down")):
                # _check_sysroot_completion has nothing to find, so PENDING.
                assert triggered.status == "PENDING"
