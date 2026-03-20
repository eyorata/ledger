"""
ledger/domain/aggregates/compliance_record.py
ComplianceRecord aggregate with rule tracking.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.domain.errors import DomainError


@dataclass
class ComplianceRecordAggregate:
    application_id: str
    regulation_set_version: str | None = None
    required_rules: set[str] = field(default_factory=set)
    passed_rules: set[str] = field(default_factory=set)
    failed_rules: set[str] = field(default_factory=set)
    noted_rules: set[str] = field(default_factory=set)
    has_hard_block: bool = False
    version: int = -1

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        events = await store.load_stream(f"compliance-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: dict) -> None:
        et = event.get("event_type")
        payload = event.get("payload", {})
        if et == "ComplianceCheckInitiated":
            self.regulation_set_version = payload.get("regulation_set_version")
            self.required_rules.update(payload.get("rules_to_evaluate") or [])
        elif et == "ComplianceRulePassed":
            rule_id = payload.get("rule_id")
            if rule_id:
                self.passed_rules.add(rule_id)
        elif et == "ComplianceRuleFailed":
            rule_id = payload.get("rule_id")
            if rule_id:
                self.failed_rules.add(rule_id)
            if payload.get("is_hard_block"):
                self.has_hard_block = True
        elif et == "ComplianceRuleNoted":
            rule_id = payload.get("rule_id")
            if rule_id:
                self.noted_rules.add(rule_id)

        if "stream_position" in event:
            self.version = event["stream_position"]
        else:
            self.version += 1

    def assert_all_required_passed(self) -> None:
        if self.has_hard_block:
            raise DomainError("Compliance hard block present.")
        if self.required_rules and not self.required_rules.issubset(self.passed_rules):
            missing = sorted(self.required_rules - self.passed_rules)
            raise DomainError(f"Missing compliance passes: {missing}")
