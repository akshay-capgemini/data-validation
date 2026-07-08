"""Business-rule & custom-SQL validators — fully rule-driven pushdown checks."""

from __future__ import annotations

from dvf.audit.models import RuleResult, RuleStatus
from dvf.metadata.ddl import RuleTypes
from dvf.validators.base import BaseValidator
from dvf.validators.registry import ValidatorRegistry


class _RuleDrivenBase(BaseValidator):
    def _side(self) -> str:
        return str(self.param("side", "target")).lower()

    def _connector(self, side: str):
        return self.source if side == "source" else self.target

    def _ref(self, side: str):
        return self.source_ref() if side == "source" else self.target_ref()

    def _timeout(self) -> int:
        return self.settings.engine.query_timeout_s


@ValidatorRegistry.register(RuleTypes.BUSINESS_RULE)
class BusinessRuleValidator(_RuleDrivenBase):
    """rule_expression = SQL predicate describing the EXPECTED condition;
    violations = rows where NOT(expression). mode='violation' inverts (the
    expression describes offending rows). FAIL when violations > threshold."""

    def execute(self) -> list[RuleResult]:
        expr = (self.rule.rule_expression or "").strip()
        if not expr:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no rule_expression"})]
        side = self._side()
        connector = self._connector(side)
        mode = str(self.param("mode", "expect")).lower()
        predicate = expr if mode == "violation" else f"NOT ({expr})"
        sql = self.sqlgen.render(connector.dialect, "predicate_count.sql.j2",
                                 ref=self._ref(side), predicate=predicate)
        violations = int(connector.execute_scalar(sql, timeout_s=self._timeout()) or 0)
        threshold = float(self.rule.threshold or 0)
        ok = violations <= threshold
        kwargs = {f"{side}_sql": sql, f"{side}_value": violations}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            difference=None if ok else
                            f"{violations} row(s) violate business rule on {side}",
                            details={"side": side, "mode": mode,
                                     "expression": expr, "threshold": threshold},
                            **kwargs)]


@ValidatorRegistry.register(RuleTypes.CUSTOM_SQL)
class CustomSqlValidator(_RuleDrivenBase):
    """rule_expression = full SQL returning ONE scalar.

    Modes (first match wins):
    1. parameters['source_sql'] set  -> run source_sql on source, expression on
       target, compare numerically (tolerance-aware).
    2. parameters['expected'] set    -> scalar must equal expected (numeric or text).
    3. neither                       -> scalar is a violation count; must be <= threshold.
    """

    def execute(self) -> list[RuleResult]:
        expr = (self.rule.rule_expression or "").strip().rstrip(";")
        if not expr:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no rule_expression"})]
        side = self._side()
        source_sql_param = self.param("source_sql")

        if source_sql_param:
            src_val = self.source.execute_scalar(str(source_sql_param).rstrip(";"),
                                                 timeout_s=self._timeout())
            tgt_val = self.target.execute_scalar(expr, timeout_s=self._timeout())
            try:
                ok, tolerance, diff_pct = self.compare_numeric(
                    float(src_val), float(tgt_val))
            except (TypeError, ValueError):
                ok, tolerance, diff_pct = (str(src_val) == str(tgt_val), None, None)
            return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                                source_value=src_val, target_value=tgt_val,
                                difference_pct=diff_pct, tolerance_applied=tolerance,
                                source_sql=str(source_sql_param), target_sql=expr)]

        connector = self._connector(side)
        value = connector.execute_scalar(expr, timeout_s=self._timeout())
        expected = self.param("expected")
        kwargs = {f"{side}_sql": expr, f"{side}_value": value}
        if expected is not None:
            try:
                ok = float(value) == float(expected)
            except (TypeError, ValueError):
                ok = str(value) == str(expected)
            return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                                difference=None if ok else
                                f"expected {expected!r}, got {value!r}",
                                details={"expected": expected, "side": side},
                                **kwargs)]
        threshold = float(self.rule.threshold or 0)
        violations = float(value or 0)
        ok = violations <= threshold
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            difference=None if ok else
                            f"{value} violation(s) (threshold {threshold})",
                            details={"threshold": threshold, "side": side},
                            **kwargs)]
