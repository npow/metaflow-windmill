"""Metaflow Deployer plugin for Windmill.

Registers TYPE = "windmill" so that Deployer(flow_file).windmill(...)
is available and the Metaflow Deployer API can be used with Windmill.

OrchestratorCapabilities contract — implemented in windmill_compiler.py:

  Cap.CONFIG_EXPR    — METAFLOW_FLOW_CONFIG_VALUE is extracted at compile time
                       from flow._flow_state[FlowStateItems.CONFIGS] and injected
                       into every step subprocess via _build_env_dict().

  Cap.PROJECT_BRANCH — '--branch' is forwarded to every step subprocess via
                       _step_cmd() in WindmillCompiler.

  Cap.RETRY          — retry_count is derived from WM_FLOW_RETRY_COUNT (Windmill
                       native attempt counter) in every step bash script.
                       str(retry_count) is passed via --retry-count.

  Cap.DATASTORE      — METAFLOW_DATASTORE_SYSROOT_LOCAL is captured at deploy time
                       and baked into every step env via _build_env_dict().

  Cap.ENVIRONMENT    — '--environment' is passed to every step command so @conda
                       flows use the correct Python interpreter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.windmill.plugins.windmill.windmill_deployer_objects import (
        WindmillDeployedFlow,
    )


class WindmillDeployer(DeployerImpl):
    """Deployer implementation for Windmill.

    Parameters
    ----------
    name : str, optional
        Override the Windmill flow path (default: derived from flow class name).
    windmill_host : str, optional
        Windmill server base URL (default: "http://localhost:8000").
    windmill_token : str, optional
        Windmill API token.
    windmill_workspace : str, optional
        Windmill workspace (default: "admins").
    max_workers : int, optional
        Maximum concurrent ForEach body tasks (default: 10).
    """

    TYPE: ClassVar[Optional[str]] = "windmill"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> Type["WindmillDeployedFlow"]:
        from .windmill_deployer_objects import WindmillDeployedFlow
        return WindmillDeployedFlow

    def create(self, **kwargs) -> "WindmillDeployedFlow":
        """Deploy this flow to a running Windmill instance.

        Parameters
        ----------
        windmill_host : str, optional
            Windmill server base URL.
        windmill_token : str, optional
            Windmill API token.
        windmill_workspace : str, optional
            Windmill workspace.
        max_workers : int, optional
            Maximum concurrent ForEach body tasks.
        tags : list of str, optional
            Tags to apply to all Metaflow runs.

        Returns
        -------
        WindmillDeployedFlow
        """
        from .windmill_deployer_objects import WindmillDeployedFlow
        return self._create(WindmillDeployedFlow, **kwargs)
