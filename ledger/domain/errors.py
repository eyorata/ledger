"""Domain-level errors for aggregates and command handlers."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DomainError(Exception):
    code: str = "DOMAIN_ERROR"
    message: str = "Domain rule violated"
    context: dict[str, Any] = field(default_factory=dict)

    def __init__(self, message: str, code: str = "DOMAIN_ERROR", context: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        return self.message
