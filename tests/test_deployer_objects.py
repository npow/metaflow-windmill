"""Unit tests for WindmillDeployedFlow.from_deployment."""

from __future__ import annotations

import json
import os
from unittest.mock import patch

from metaflow_extensions.windmill.plugins.windmill.windmill_deployer_objects import (
    WindmillDeployedFlow,
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
