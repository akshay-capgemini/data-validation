"""ProfileExpander — turns a validation config row into ordered RuleInstances.

Profiles are data (meta.validation_profiles). Per-validation rows in
meta.validation_rules add column-specific/custom rules; a rules-table entry with
no column_name *replaces* the same-type profile entry (override semantics).
CUSTOM profile = rules table only.
"""

from __future__ import annotations

from dvf.core.exceptions import MetadataError
from dvf.metadata.models import Profiles, RuleInstance, ValidationConfig
from dvf.metadata.repository import MetadataRepository
from dvf.validators.registry import ValidatorRegistry


class ProfileExpander:
    def __init__(self, repo: MetadataRepository):
        self.repo = repo
        self._profile_cache: dict[str, list] = {}

    def expand(self, config: ValidationConfig,
               profile_override: str | None = None) -> list[RuleInstance]:
        profile_name = (profile_override or config.validation_profile or
                        Profiles.STANDARD).upper()

        instances: list[RuleInstance] = []
        if profile_name != Profiles.CUSTOM:
            entries = self._profile_cache.get(profile_name)
            if entries is None:
                entries = self.repo.get_profile(profile_name)
                self._profile_cache[profile_name] = entries
            if not entries:
                raise MetadataError(
                    f"Validation profile '{profile_name}' has no entries "
                    f"(validation '{config.validation_id}')")
            instances = [RuleInstance(rule_type=e.rule_type,
                                      parameters=dict(e.default_parameters),
                                      severity=str(e.default_parameters.get(
                                          "severity", "ERROR")).upper(),
                                      rule_order=e.rule_order)
                         for e in entries]

        rules = self.repo.get_rules(config.validation_id)
        overrides = {r.rule_type.upper() for r in rules if not r.column_name}
        instances = [i for i in instances if i.rule_type.upper() not in overrides]
        for r in rules:
            order = r.parameters.get("rule_order")
            instances.append(RuleInstance(
                rule_type=r.rule_type, rule_id=r.rule_id, column_name=r.column_name,
                rule_expression=r.rule_expression, severity=r.severity,
                threshold=r.threshold, parameters=dict(r.parameters),
                rule_order=int(order) if order is not None else 500))

        if not instances:
            raise MetadataError(
                f"Validation '{config.validation_id}' expands to zero rules "
                f"(profile '{profile_name}')")

        unknown = sorted({i.rule_type for i in instances
                          if not ValidatorRegistry.is_registered(i.rule_type)})
        if unknown:
            raise MetadataError(
                f"Validation '{config.validation_id}' references unregistered rule "
                f"type(s) {unknown}. Known: {ValidatorRegistry.known_types()}")

        instances.sort(key=lambda i: (i.rule_order, i.rule_type))
        return instances
