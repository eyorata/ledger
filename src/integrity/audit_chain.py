"""
src/integrity/audit_chain.py
Wrapper for audit integrity hash chain.
"""
from ledger.audit.integrity import run_integrity_check, IntegrityCheckResult

__all__ = ["run_integrity_check", "IntegrityCheckResult"]
