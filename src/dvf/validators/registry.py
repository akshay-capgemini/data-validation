"""ValidatorRegistry — rule_type -> validator class, decorator-registered.

Adding a validation type = subclass BaseValidator + @ValidatorRegistry.register.
No engine changes.
"""

from __future__ import annotations

from typing import Callable, Type

from dvf.core.exceptions import ConfigurationError
from dvf.validators.base import BaseValidator, ValidationContext


class ValidatorRegistry:
    _registry: dict[str, Type[BaseValidator]] = {}

    @classmethod
    def register(cls, rule_type: str) -> Callable[[Type[BaseValidator]], Type[BaseValidator]]:
        def decorator(validator_cls: Type[BaseValidator]) -> Type[BaseValidator]:
            validator_cls.rule_type = rule_type
            cls._registry[rule_type.upper()] = validator_cls
            return validator_cls
        return decorator

    @classmethod
    def create(cls, rule_type: str, ctx: ValidationContext) -> BaseValidator:
        validator_cls = cls._registry.get((rule_type or "").upper())
        if validator_cls is None:
            raise ConfigurationError(
                f"No validator registered for rule type '{rule_type}'. "
                f"Known: {sorted(cls._registry)}")
        return validator_cls(ctx)

    @classmethod
    def known_types(cls) -> list[str]:
        return sorted(cls._registry)

    @classmethod
    def is_registered(cls, rule_type: str) -> bool:
        return (rule_type or "").upper() in cls._registry
