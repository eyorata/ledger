"""
ledger/registry/client.py — Applicant Registry read-only client
===============================================================
COMPLETION STATUS: STUB — implement the query methods.

This client reads from the applicant_registry schema in PostgreSQL.
It is READ-ONLY. No agent or event store component ever writes here.
The Applicant Registry is the external CRM — seeded by datagen/generate_all.py.
"""
from __future__ import annotations
from dataclasses import dataclass
import asyncpg

@dataclass
class CompanyProfile:
    company_id: str; name: str; industry: str; naics: str
    jurisdiction: str; legal_type: str; founded_year: int
    employee_count: int; risk_segment: str; trajectory: str
    submission_channel: str; ip_region: str

@dataclass
class FinancialYear:
    fiscal_year: int; total_revenue: float; gross_profit: float
    operating_income: float; ebitda: float; net_income: float
    total_assets: float; total_liabilities: float; total_equity: float
    long_term_debt: float; cash_and_equivalents: float
    current_assets: float; current_liabilities: float
    accounts_receivable: float; inventory: float
    debt_to_equity: float; current_ratio: float
    debt_to_ebitda: float; interest_coverage_ratio: float
    gross_margin: float; ebitda_margin: float; net_margin: float

@dataclass
class ComplianceFlag:
    flag_type: str; severity: str; is_active: bool; added_date: str; note: str

class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    Agents call these methods to get company profiles and historical data.
    Never write to this database from the event store system.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        """
        TODO: implement
        SELECT * FROM applicant_registry.companies WHERE company_id = $1
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM applicant_registry.companies WHERE company_id = $1",
                company_id,
            )
            if not row:
                return None
            return CompanyProfile(
                company_id=row["company_id"],
                name=row["name"],
                industry=row["industry"],
                naics=row["naics"],
                jurisdiction=row["jurisdiction"],
                legal_type=row["legal_type"],
                founded_year=row["founded_year"],
                employee_count=row["employee_count"],
                risk_segment=row["risk_segment"],
                trajectory=row["trajectory"],
                submission_channel=row["submission_channel"],
                ip_region=row["ip_region"],
            )

    async def get_financial_history(self, company_id: str,
                                     years: list[int] | None = None) -> list[FinancialYear]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.financial_history
        WHERE company_id = $1 [AND fiscal_year = ANY($2)]
        ORDER BY fiscal_year ASC
        """
        async with self._pool.acquire() as conn:
            if years:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.financial_history "
                    "WHERE company_id = $1 AND fiscal_year = ANY($2) "
                    "ORDER BY fiscal_year ASC",
                    company_id,
                    years,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.financial_history "
                    "WHERE company_id = $1 ORDER BY fiscal_year ASC",
                    company_id,
                )
            result = []
            for row in rows:
                result.append(FinancialYear(
                    fiscal_year=row["fiscal_year"],
                    total_revenue=float(row["total_revenue"]),
                    gross_profit=float(row["gross_profit"]),
                    operating_income=float(row["operating_income"]),
                    ebitda=float(row["ebitda"]),
                    net_income=float(row["net_income"]),
                    total_assets=float(row["total_assets"]),
                    total_liabilities=float(row["total_liabilities"]),
                    total_equity=float(row["total_equity"]),
                    long_term_debt=float(row["long_term_debt"]),
                    cash_and_equivalents=float(row["cash_and_equivalents"]),
                    current_assets=float(row["current_assets"]),
                    current_liabilities=float(row["current_liabilities"]),
                    accounts_receivable=float(row["accounts_receivable"]),
                    inventory=float(row["inventory"]),
                    debt_to_equity=float(row["debt_to_equity"]) if row["debt_to_equity"] is not None else 0.0,
                    current_ratio=float(row["current_ratio"]) if row["current_ratio"] is not None else 0.0,
                    debt_to_ebitda=float(row["debt_to_ebitda"]) if row["debt_to_ebitda"] is not None else 0.0,
                    interest_coverage_ratio=float(row["interest_coverage_ratio"]) if row["interest_coverage_ratio"] is not None else 0.0,
                    gross_margin=float(row["gross_margin"]) if row["gross_margin"] is not None else 0.0,
                    ebitda_margin=float(row["ebitda_margin"]) if row["ebitda_margin"] is not None else 0.0,
                    net_margin=float(row["net_margin"]) if row["net_margin"] is not None else 0.0,
                ))
            return result

    async def get_compliance_flags(self, company_id: str,
                                    active_only: bool = False) -> list[ComplianceFlag]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.compliance_flags
        WHERE company_id = $1 [AND is_active = TRUE]
        """
        async with self._pool.acquire() as conn:
            if active_only:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.compliance_flags "
                    "WHERE company_id = $1 AND is_active = TRUE",
                    company_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM applicant_registry.compliance_flags "
                    "WHERE company_id = $1",
                    company_id,
                )
            return [
                ComplianceFlag(
                    flag_type=r["flag_type"],
                    severity=r["severity"],
                    is_active=r["is_active"],
                    added_date=str(r["added_date"]),
                    note=r["note"],
                ) for r in rows
            ]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1",
                company_id,
            )
            return [dict(r) for r in rows]
