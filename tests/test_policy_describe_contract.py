from __future__ import annotations

from analyst_agent.policy_registry import PolicyRegistry


def test_policy_registry_contains_expected():
    reg = PolicyRegistry()
    names = reg.list_policies()
    assert set(names) >= {"sales_v1", "orders_v1", "generic_tabular"}


def test_policy_describe_contract_shape_and_types():
    reg = PolicyRegistry()
    for name in ["sales_v1", "orders_v1", "generic_tabular"]:
        desc = reg.describe_policy(name)

        # required keys
        required_keys = {
            "name",
            "version",
            "required_roles",
            "optional_roles",
            "expected_metrics",
            "coverage_behavior",
            "anomalies_emitted",
            "severity_thresholds",
            "emits_anomalies",
            "emits_anomalies_normalized",
        }
        assert required_keys.issubset(desc.keys())

        # types
        assert isinstance(desc["name"], str)
        assert isinstance(desc["version"], str)
        assert isinstance(desc["coverage_behavior"], str)
        assert isinstance(desc["required_roles"], list)
        assert isinstance(desc["optional_roles"], list)
        assert isinstance(desc["expected_metrics"], list)
        assert isinstance(desc["anomalies_emitted"], list)
        assert isinstance(desc["severity_thresholds"], dict)
        assert isinstance(desc["emits_anomalies"], bool)
        assert isinstance(desc["emits_anomalies_normalized"], bool)

        # name must match registry key
        assert desc["name"] == name

        if name in ("sales_v1", "orders_v1"):
            assert desc["emits_anomalies"] is True
            assert desc["emits_anomalies_normalized"] is True
        else:
            assert desc["emits_anomalies"] is False
            assert desc["emits_anomalies_normalized"] is False