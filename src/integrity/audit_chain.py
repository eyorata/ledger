"""
src/integrity/audit_chain.py
Thin wrapper for spec path compatibility.
"""
from ledger.audit.integrity import run_integrity_check, IntegrityCheckResult  # noqa: F401

__all__ = ["run_integrity_check", "IntegrityCheckResult"]
