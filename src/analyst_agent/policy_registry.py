from __future__ import annotations

from typing import Protocol, Type, cast

from .policy import GenericTabularPolicy
from .orders_policy import OrdersPolicyV1
from .sales_policy import SalesPolicyV1


class PolicySpec(Protocol):
    name: str
    version: str
    description: str
    required_fields: list[str]


class PolicyRegistry:
    """
    Registry for available analysis policies.
    Keeps deterministic metadata for CLI inspection.
    """

    DEFAULT_VERSION = "1.0.0"

    def __init__(self) -> None:
        self._policies: dict[str, Type[PolicySpec]] = {}
        self._metadata: dict[str, dict[str, object]] = {}
        # Track versions for each base policy name.  Base names are derived from the
        # registered policy names and allow multiple versions (e.g., orders_v1, orders_v2)
        # to coexist without conflicts.  Keys are base names and values are lists of
        # registered names corresponding to that base.
        self._policy_versions: dict[str, list[str]] = {}
        self._register_builtin()

    def _register_builtin(self) -> None:
        self.register(
            name="generic_tabular",
            policy_cls=cast(Type[PolicySpec], GenericTabularPolicy),
            metadata={
                "name": "generic_tabular",
                "version": self.DEFAULT_VERSION,
                "description": "Deterministic, schema-driven policy for generic tabular datasets.",
                "required_fields": [],
            },
        )
        self.register(
            name="orders_v1",
            policy_cls=cast(Type[PolicySpec], OrdersPolicyV1),
            metadata={
                "name": OrdersPolicyV1.name,
                "version": OrdersPolicyV1.version,
                "description": OrdersPolicyV1.description,
                "required_fields": OrdersPolicyV1.required_fields,
            },
        )
        self.register(
            name="sales_v1",
            policy_cls=cast(Type[PolicySpec], SalesPolicyV1),
            metadata={
                "name": SalesPolicyV1.name,
                "version": SalesPolicyV1.version,
                "description": SalesPolicyV1.description,
                "required_fields": getattr(SalesPolicyV1, "required_fields", []),
            },
        )

    def register(self, *, name: str, policy_cls: Type[PolicySpec], metadata: dict[str, object] | None = None) -> None:
        if not name:
            raise ValueError("Policy name must be provided.")

        normalized_meta = self._normalize_metadata(name=name, policy_cls=policy_cls, metadata=metadata)

        self._policies[name] = policy_cls
        self._metadata[name] = normalized_meta
        # Derive a base name for version tracking.  A simple convention treats names
        # containing a version suffix (e.g. '_v2') as having a base before the last
        # underscore; otherwise the full name is used as the base.  This enables
        # introspection of all versions for a given policy family.
        base_name = name
        if "_v" in name:
            base_name = name.rsplit("_v", 1)[0]
        self._policy_versions.setdefault(base_name, [])
        if name not in self._policy_versions[base_name]:
            self._policy_versions[base_name].append(name)

    def list_policies(self) -> list[str]:
        """
        Return a sorted list of registered policy names.
        This includes individual versioned entries (for example, 'orders_v1', 'orders_v2').
        """
        return sorted(self._policies.keys())

    def list_policy_versions(self, base_name: str) -> list[str]:
        """
        Return a sorted list of registered policy names for the given base policy.
        For example, base_name="orders" might return ["orders_v1", "orders_v2"] if both
        versions have been registered.
        """
        return sorted(self._policy_versions.get(base_name, []))

    def get_policy(self, name: str) -> type:
        try:
            return self._policies[name]
        except KeyError as exc:
            raise KeyError(self._unknown_policy_msg(name)) from exc

    def describe_policy(self, name: str) -> dict[str, object]:
        try:
            meta = self._metadata[name]
            policy_cls = self._policies[name]
        except KeyError as exc:
            raise KeyError(self._unknown_policy_msg(name)) from exc
        # Prefer policy-provided description for explainability
        if hasattr(policy_cls, "describe_policy"):
            desc = policy_cls.describe_policy()  # type: ignore[call-arg]
            # ensure name/version filled
            desc.setdefault("name", name)
            desc.setdefault("version", meta["version"])
        else:
            desc = {
                "name": name,
                "version": meta["version"],
                "description": meta["description"],
                "required_fields": list(meta["required_fields"]),
                "required_roles": [],
                "optional_roles": [],
                "expected_metrics": [],
                "coverage_behavior": "",
                "anomalies_emitted": [],
                "severity_thresholds": {},
                "emits_anomalies": False,
                "emits_anomalies_normalized": False,
            }

        # Normalize required keys
        desc.setdefault("required_roles", [])
        desc.setdefault("optional_roles", [])
        desc.setdefault("expected_metrics", [])
        desc.setdefault("coverage_behavior", "")
        desc.setdefault("anomalies_emitted", [])
        desc.setdefault("severity_thresholds", {})
        desc.setdefault("emits_anomalies", False)
        desc.setdefault("emits_anomalies_normalized", False)
        return desc

    def _normalize_metadata(
        self, *, name: str, policy_cls: Type[PolicySpec], metadata: dict[str, object] | None
    ) -> dict[str, object]:
        meta = metadata or {}
        return {
            "name": str(meta.get("name", name)),
            "version": str(meta.get("version", getattr(policy_cls, "version", self.DEFAULT_VERSION))),
            "description": str(meta.get("description", getattr(policy_cls, "description", ""))),
            "required_fields": list(meta.get("required_fields", getattr(policy_cls, "required_fields", []))),
        }

    def _unknown_policy_msg(self, name: str) -> str:
        available = ", ".join(self.list_policies()) or "none"
        return f"Unknown policy '{name}'. Available policies: {available}."
