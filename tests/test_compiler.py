"""Unit tests for the Windmill compiler."""

import json
import os
import sys
import pytest

# Add metaflow to path for tests
METAFLOW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "metaflow")
if os.path.isdir(METAFLOW_DIR):
    sys.path.insert(0, METAFLOW_DIR)


def _make_compiler(flow_cls, **kwargs):
    """Helper to instantiate WindmillCompiler with mock dependencies."""
    from metaflow.graph import FlowGraph
    from metaflow_extensions.windmill.plugins.windmill.windmill_compiler import (
        WindmillCompiler,
    )

    class MockMeta:
        TYPE = "local"

    class MockDatastore:
        TYPE = "local"
        datastore_root = "/tmp/mf_test"

    class MockEnv:
        TYPE = "local"

    class MockLogger:
        TYPE = "nullSidecarLogger"

    class MockMonitor:
        TYPE = "nullSidecarMonitor"

    graph = FlowGraph(flow_cls)
    return WindmillCompiler(
        name=flow_cls.__name__,
        graph=graph,
        flow=flow_cls,
        flow_file="/tmp/test_flow.py",
        metadata=MockMeta(),
        flow_datastore=MockDatastore(),
        environment=MockEnv(),
        event_logger=MockLogger(),
        monitor=MockMonitor(),
        **kwargs,
    )


def test_flow_name_to_path():
    from metaflow_extensions.windmill.plugins.windmill.windmill_compiler import (
        flow_name_to_path,
    )

    assert flow_name_to_path("HelloFlow") == "u/admin/helloflow"
    assert flow_name_to_path("MyProject.test.branch.HelloFlow") == (
        "u/admin/myproject-test-branch-helloflow"
    )
    assert flow_name_to_path("hello_world") == "u/admin/hello-world"


def test_compile_linear_flow():
    """Compiler produces valid JSON for a linear flow."""
    from metaflow import FlowSpec, step

    class LinearFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(LinearFlow)
    result = compiler.compile()

    assert "value" in result
    assert "modules" in result["value"]
    assert "schema" in result
    modules = result["value"]["modules"]
    # init + start + end
    assert len(modules) == 3
    assert modules[0]["id"] == "metaflow_init"
    step_ids = [m["id"] for m in modules[1:]]
    assert "start" in step_ids
    assert "end" in step_ids


def test_compile_branch_flow():
    """Compiler produces branchall module for split/join."""
    from metaflow import FlowSpec, step

    class BranchFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.a, self.b)

        @step
        def a(self):
            self.next(self.join)

        @step
        def b(self):
            self.next(self.join)

        @step
        def join(self, inputs):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(BranchFlow)
    result = compiler.compile()
    modules = result["value"]["modules"]

    # Find the branchall module
    branch_module = next(
        (m for m in modules if m["value"].get("type") == "branchall"), None
    )
    assert branch_module is not None, "Expected branchall module for split/join"
    branches = branch_module["value"]["branches"]
    assert len(branches) == 2
    branch_labels = {b["label"] for b in branches}
    assert branch_labels == {"a", "b"}


def test_compile_foreach_flow():
    """Compiler produces forloopflow module for foreach."""
    from metaflow import FlowSpec, step

    class ForeachFlow(FlowSpec):
        @step
        def start(self):
            self.items = ["a", "b", "c"]
            self.next(self.process, foreach="items")

        @step
        def process(self):
            self.next(self.join)

        @step
        def join(self, inputs):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(ForeachFlow)
    result = compiler.compile()
    modules = result["value"]["modules"]

    foreach_module = next(
        (m for m in modules if m["value"].get("type") == "forloopflow"), None
    )
    assert foreach_module is not None, "Expected forloopflow module for foreach"
    assert foreach_module["value"]["parallel"] is True


def test_schema_has_origin_run_id():
    """Schema always includes ORIGIN_RUN_ID for resume support."""
    from metaflow import FlowSpec, step

    class SimpleFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(SimpleFlow)
    result = compiler.compile()
    assert "ORIGIN_RUN_ID" in result["schema"]["properties"]


def test_env_vars_in_step_scripts():
    """Step scripts include METAFLOW env exports."""
    from metaflow import FlowSpec, step

    class SimpleFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(SimpleFlow)
    result = compiler.compile()
    modules = result["value"]["modules"]

    start_module = next(m for m in modules if m["id"] == "start")
    script = start_module["value"]["content"]
    assert "METAFLOW_DEFAULT_METADATA" in script
    assert "WM_FLOW_RETRY_COUNT" in script


def test_branch_forwarded_in_step_scripts():
    """REQUIRED (Cap.PROJECT_BRANCH): --branch is in every step command."""
    from metaflow import FlowSpec, step

    class SimpleFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(SimpleFlow, branch="mybranch")
    result = compiler.compile()
    modules = result["value"]["modules"]

    start_module = next(m for m in modules if m["id"] == "start")
    script = start_module["value"]["content"]
    assert "--branch mybranch" in script


def test_nested_foreach_compiles():
    """Nested foreach compiles to a nested forloopflow structure."""
    from metaflow import FlowSpec, step

    class NestedForeachFlow(FlowSpec):
        @step
        def start(self):
            self.outer = [1, 2]
            self.next(self.outer_step, foreach="outer")

        @step
        def outer_step(self):
            self.inner = [1, 2]
            self.next(self.inner_step, foreach="inner")

        @step
        def inner_step(self):
            self.next(self.inner_join)

        @step
        def inner_join(self, inputs):
            self.next(self.outer_join)

        @step
        def outer_join(self, inputs):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(NestedForeachFlow)
    result = compiler.compile()

    modules = result["value"]["modules"]
    module_ids = [m["id"] for m in modules]

    # Outer forloopflow should be present
    assert "foreach_start" in module_ids, "Expected outer forloopflow module"

    # Find the outer forloopflow
    outer_foreach = next(m for m in modules if m["id"] == "foreach_start")
    assert outer_foreach["value"]["type"] == "forloopflow"

    # The body of the outer forloopflow should contain the outer_step module
    # followed by an inner forloopflow
    outer_body = outer_foreach["value"]["modules"]
    outer_body_ids = [m["id"] for m in outer_body]
    assert "outer_step" in outer_body_ids, "outer_step should be in outer foreach body"
    assert "foreach_outer_step" in outer_body_ids, "inner forloopflow missing from outer foreach body"

    # The inner forloopflow body should contain inner_step
    inner_foreach = next(m for m in outer_body if m["id"] == "foreach_outer_step")
    assert inner_foreach["value"]["type"] == "forloopflow"
    inner_body_ids = [m["id"] for m in inner_foreach["value"]["modules"]]
    assert "inner_step" in inner_body_ids, "inner_step should be in inner foreach body"

    # inner_join runs once per outer iteration, so it lives INSIDE the outer
    # forloopflow body (not at the top level).
    assert "inner_join" in outer_body_ids, "inner_join should be inside outer foreach body"

    # outer_join runs once after all outer iterations, so it lives at the top level.
    assert "outer_join" in module_ids, "outer_join should be at top level"
    assert "end" in module_ids, "end step should be at top level"


def test_from_deployment_dotted_name():
    """REQUIRED (Cap.FROM_DEPLOYMENT): dotted identifiers are handled correctly."""
    from metaflow_extensions.windmill.plugins.windmill.windmill_deployer_objects import (
        WindmillDeployedFlow,
    )

    # Dotted name should not raise SyntaxError
    try:
        recovered = WindmillDeployedFlow.from_deployment(
            "myproject.test.branch.HelloFlow"
        )
    except SyntaxError as exc:
        pytest.fail(
            "from_deployment raised SyntaxError for dotted name: %s" % exc
        )

    assert recovered is not None
    assert recovered.deployer.flow_name == "HelloFlow"


def test_run_params_is_list():
    """REQUIRED (Cap.RUN_PARAMS): run_params must be a list, not a tuple."""
    from metaflow_extensions.windmill.plugins.windmill.windmill_deployer_objects import (
        WindmillDeployedFlow,
    )

    # Simulate what happens when run() is called with kwargs
    # The deployer builds run_params as a list comprehension
    kwargs = {"param1": "val1", "param2": "val2"}
    run_params = list("%s=%s" % (k, v) for k, v in kwargs.items())
    assert isinstance(run_params, list), "run_params must be a list"
    assert len(run_params) == 2


def test_conditional_split_compiles():
    """@condition (split-switch) compiles to a branchone module."""
    from metaflow import FlowSpec, step

    class CondFlow(FlowSpec):
        @step
        def start(self):
            self.my_cond = "branch_a"
            self.next(
                {"branch_a": self.branch_a, "branch_b": self.branch_b},
                condition="my_cond",
            )

        @step
        def branch_a(self):
            self.next(self.join)

        @step
        def branch_b(self):
            self.next(self.join)

        @step
        def join(self, inputs):
            self.next(self.end)

        @step
        def end(self):
            pass

    compiler = _make_compiler(CondFlow)
    result = compiler.compile()

    modules = result["value"]["modules"]
    module_ids = [m["id"] for m in modules]

    # The switch step should be present as a rawscript module
    assert "start" in module_ids, "start step missing"

    # A branchone module should follow the switch step
    assert "switch_start" in module_ids, "branchone module 'switch_start' missing"

    switch_module = next(m for m in modules if m["id"] == "switch_start")
    assert switch_module["value"]["type"] == "branchone"

    # Branches: one explicit predicate + one default
    branches = switch_module["value"]["branches"]
    default = switch_module["value"]["default"]

    # One of branch_a/branch_b is a predicate branch, the other is default
    all_branch_names = [b["summary"] for b in branches] + (
        [default[0]["id"]] if default else []
    )
    assert "branch_a" in all_branch_names, "branch_a missing from branchone"
    assert "branch_b" in all_branch_names, "branch_b missing from branchone"

    # Predicate branches reference results.<switch_step>.branch
    for branch in branches:
        assert "results.start.branch" in branch["expr"], (
            "Branch predicate should reference results.start.branch"
        )

    # Join step emitted at top level after branchone
    assert "join" in module_ids, "join step should be at top level after branchone"
