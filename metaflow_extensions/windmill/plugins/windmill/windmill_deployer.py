"""Metaflow Deployer plugin for Windmill.

Registers TYPE = "windmill" so that Deployer(flow_file).windmill(...)
is available and the Metaflow Deployer API can be used with Windmill.
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
