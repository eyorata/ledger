"""
ledger/agents/stub_agents.py
============================
STUB IMPLEMENTATIONS for DocumentProcessingAgent, FraudDetectionAgent,
ComplianceAgent, and DecisionOrchestratorAgent.

Each stub contains:
  - The State TypedDict
  - build_graph() with the correct node sequence
  - All node method stubs with TODO instructions
  - The exact events each node must write
  - WHEN IT WORKS criteria for each agent

Pattern: follow CreditAnalysisAgent exactly. Same build_graph() structure,
same _record_node_execution() calls, same _append_with_retry() for domain writes.
"""
from __future__ import annotations
import time, json, os, re, requests
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    DocumentFormatValidated,
    DocumentFormatRejected,
    ExtractionStarted,
    ExtractionCompleted,
    ExtractionFailed,
    QualityAssessmentCompleted,
    PackageReadyForAnalysis,
    CreditAnalysisRequested,
    FraudScreeningInitiated,
    FraudAnomalyDetected,
    FraudScreeningCompleted,
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceCheckCompleted,
    ComplianceCheckRequested,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    ApplicationApproved,
    ApplicationDeclined,
    FraudAnomaly,
    FraudAnomalyType,
    FinancialFacts,
    DocumentType,
    DocumentFormat,
)

QUALITY_SYSTEM_PROMPT = """
You are a financial document quality analyst. You receive structured data
extracted from a company's financial statements.

Check ONLY:
1. Internal consistency (Gross Profit = Revenue - COGS, Assets = Liabilities + Equity)
2. Implausible values (margins > 80%, negative equity without note)
3. Critical missing fields (total_revenue, net_income, total_assets, total_liabilities)

Return JSON: {"overall_confidence": float, "is_coherent": bool,
  "anomalies": [str], "critical_missing_fields": [str],
  "reextraction_recommended": bool, "auditor_notes": str}

DO NOT make credit or lending decisions. DO NOT suggest loan outcomes.
"""

def _safe_float(v, default=None):
    try:
        return float(v)
    except Exception:
        return default

def _refinery_paths():
    root = os.getenv(
        "DOCUMENT_REFINERY_ROOT",
        r"C:\Users\user\Documents\tenx_academy\document-refinery\document-refinery",
    )
    venv = os.getenv(
        "DOCUMENT_REFINERY_VENV",
        r"C:\Users\user\Documents\tenx_academy\document-refinery\document-refinery\.venv",
    )
    py = os.path.join(venv, "Scripts", "python.exe")
    return root, py

def _run_document_refinery(file_path: str) -> dict:
    refinery_api_url = os.getenv("DOCUMENT_REFINERY_API_URL", "http://localhost:8000/process_document")
    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/pdf")}
            response = requests.post(refinery_api_url, files=files, timeout=180)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        print(f"Document refinery API call failed: {e}")
        return {}

def _extract_text_from_refinery(payload: dict) -> str:
    extracted = payload.get("extracted") or {}
    blocks = extracted.get("text_blocks") or []
    if blocks:
        return blocks[0].get("content") or ""
    chunks = payload.get("chunks") or []
    if chunks:
        return chunks[0].get("content") or ""
    return ""

_LINE_RE = re.compile(r"^(?P<label>[A-Za-z &/]+)\s+\$?(?P<val>[-(]?[0-9,]+\.?\d*)\)?$", re.M)

def _parse_money(s: str) -> float | None:
    if s is None:
        return None
    s = s.replace(",", "")
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None

def _parse_line_items(text: str) -> dict[str, float | None]:
    items: dict[str, float | None] = {}
    for m in _LINE_RE.finditer(text or ""):
        label = m.group("label").strip().lower()
        val = _parse_money(m.group("val"))
        items[label] = val
    return items

def _parse_income_statement_text(text: str) -> dict:
    items = _parse_line_items(text)
    return {
        "total_revenue": items.get("revenue"),
        "gross_profit": items.get("gross profit"),
        "operating_income": items.get("operating income (ebit)") or items.get("operating income"),
        "ebitda": items.get("ebitda"),
        "interest_expense": items.get("interest expense"),
        "depreciation_amortization": items.get("depreciation & amortization"),
        "net_income": items.get("net income"),
    }

def _parse_balance_sheet_text(text: str) -> dict:
    items = _parse_line_items(text)
    return {
        "total_assets": items.get("total assets"),
        "total_liabilities": items.get("total liabilities"),
        "total_equity": items.get("total equity"),
        "current_assets": items.get("current assets"),
        "current_liabilities": items.get("current liabilities"),
        "cash_and_equivalents": items.get("cash and equivalents"),
        "accounts_receivable": items.get("accounts receivable"),
        "inventory": items.get("inventory"),
        "long_term_debt": items.get("long term debt"),
    }

def _parse_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return {}
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}


# ─── DOCUMENT PROCESSING AGENT ───────────────────────────────────────────────

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    document_ids: list[str] | None
    document_paths: list[str] | None
    extraction_results: list[dict] | None  # one per document
    quality_assessment: dict | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline.
    Processes uploaded PDFs and appends extraction events.

    LangGraph nodes:
        validate_inputs → validate_document_formats → extract_income_statement →
        extract_balance_sheet → assess_quality → write_output

    Output events:
        docpkg-{id}:  DocumentFormatValidated (x per doc), ExtractionStarted (x per doc),
                      ExtractionCompleted (x per doc), QualityAssessmentCompleted,
                      PackageReadyForAnalysis
        loan-{id}:    CreditAnalysisRequested

    WEEK 3 INTEGRATION:
        In _node_extract_document(), call your Week 3 pipeline:
            from document_refinery.pipeline import extract_financial_facts
            facts = await extract_financial_facts(file_path, document_type)
        Wrap in try/except — append ExtractionFailed if pipeline raises.

    LLM in _node_assess_quality():
        System: "You are a financial document quality analyst.
                 Check internal consistency. Do NOT make credit decisions.
                 Return DocumentQualityAssessment JSON."
        The LLM checks: Assets = Liabilities + Equity, margins plausible, etc.

    WHEN THIS WORKS:
        pytest tests/phase2/test_document_agent.py  # all pass
        python scripts/run_pipeline.py --app APEX-0001 --phase document
          → ExtractionCompleted event in docpkg stream with non-null total_revenue
          → QualityAssessmentCompleted event present
          → PackageReadyForAnalysis event present
          → CreditAnalysisRequested on loan stream
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",            self._node_validate_inputs)
        g.add_node("validate_document_formats",  self._node_validate_formats)
        g.add_node("extract_income_statement",   self._node_extract_is)
        g.add_node("extract_balance_sheet",      self._node_extract_bs)
        g.add_node("assess_quality",             self._node_assess_quality)
        g.add_node("write_output",               self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id, session_id=self.session_id,
            document_ids=None, document_paths=None,
            extraction_results=None, quality_assessment=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):
        t = time.time()
        # TODO:
        # 1. Load DocumentUploaded events from "loan-{app_id}" stream
        # 2. Extract document_ids and file_paths for each uploaded document
        # 3. Verify at least APPLICATION_PROPOSAL + INCOME_STATEMENT + BALANCE_SHEET uploaded
        # 4. If any required doc missing: await self._record_input_failed([...], [...]) then raise
        # 5. await self._record_input_validated(["application_id","document_ids","file_paths"], ms)
        app_id = state["application_id"]
        events = await self.store.load_stream(f"loan-{app_id}")
        uploaded = []
        for e in events:
            if e.get("event_type") == "DocumentUploaded":
                p = e.get("payload", {})
                uploaded.append({
                    "document_id": p.get("document_id"),
                    "document_type": p.get("document_type"),
                    "document_format": p.get("document_format"),
                    "file_path": p.get("file_path"),
                })

        # Keep latest upload per document_type (by stream_position order)
        latest_by_type: dict[str, dict] = {}
        for d in uploaded:
            dt = d.get("document_type")
            if not dt:
                continue
            latest_by_type[dt] = d
        deduped = list(latest_by_type.values())

        req = {DocumentType.INCOME_STATEMENT.value, DocumentType.BALANCE_SHEET.value}
        got = {d["document_type"] for d in deduped if d.get("document_type")}
        missing = req - got
        errors = []
        if missing:
            errors.append(f"Missing required documents: {sorted(missing)}")

        # Ensure package has not already been processed
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        if any(e.get("event_type") == "PackageReadyForAnalysis" for e in pkg_events):
            errors.append("Document package already processed (PackageReadyForAnalysis exists)")

        ms = int((time.time()-t)*1000)
        if errors:
            await self._record_input_failed([], errors)
            await self._record_node_execution("validate_inputs", ["application_id"], ["errors"], ms)
            raise ValueError(f"Input validation failed: {errors}")

        await self._record_input_validated(["application_id","document_ids","file_paths"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["document_paths"], ms)
        return {**state, "document_ids":[d["document_id"] for d in deduped],
                "document_paths": deduped}

    async def _node_validate_formats(self, state):
        t = time.time()
        # TODO:
        # For each document:
        #   1. Check file exists on disk, is not corrupt
        #   2. Detect actual format (PyPDF2, python-magic, etc.)
        #   3. Append DocumentFormatValidated(package_id, doc_id, page_count, detected_format)
        #      to "docpkg-{app_id}" stream
        #   4. If corrupt: append DocumentFormatRejected and remove from processing list
        # 5. await self._record_node_execution("validate_document_formats", ...)
        app_id = state["application_id"]
        valid_docs = []
        for d in state.get("document_paths") or []:
            path = d.get("file_path")
            if not path or not os.path.exists(path):
                await self._append_stream(
                    f"docpkg-{app_id}",
                    DocumentFormatRejected(
                        package_id=app_id,
                        document_id=d.get("document_id") or "",
                        rejection_reason="FILE_NOT_FOUND",
                        rejected_at=datetime.now().isoformat(),
                    ).to_store_dict(),
                )
                continue
            ext = os.path.splitext(path)[1].lower().lstrip(".")
            detected = ext if ext else "unknown"
            # Basic format validation (no external deps)
            is_valid = True
            if ext == "pdf":
                try:
                    with open(path, "rb") as f:
                        header = f.read(4)
                    if header != b"%PDF":
                        is_valid = False
                except Exception:
                    is_valid = False
            elif ext in ("xlsx", "csv"):
                # extension check only for now
                is_valid = True
            else:
                is_valid = False

            if not is_valid:
                await self._append_stream(
                    f"docpkg-{app_id}",
                    DocumentFormatRejected(
                        package_id=app_id,
                        document_id=d.get("document_id") or "",
                        rejection_reason="INVALID_FORMAT",
                        rejected_at=datetime.now().isoformat(),
                    ).to_store_dict(),
                )
                continue
            await self._append_stream(
                f"docpkg-{app_id}",
                DocumentFormatValidated(
                    package_id=app_id,
                    document_id=d.get("document_id") or "",
                    document_type=DocumentType(d.get("document_type")),
                    page_count=1,
                    detected_format=detected,
                    validated_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            valid_docs.append(d)

        await self._record_node_execution("validate_document_formats", ["document_paths"], ["validated_docs"], int((time.time()-t)*1000))
        return {**state, "document_paths": valid_docs}

    async def _node_extract_is(self, state):
        t = time.time()
        # TODO:
        # 1. Find income statement document from state["document_paths"]
        # 2. Append ExtractionStarted(package_id, doc_id, pipeline_version, "mineru-1.0")
        #    to "docpkg-{app_id}" stream
        # 3. Call Week 3 pipeline:
        #    from document_refinery.pipeline import extract_financial_facts
        #    facts = await extract_financial_facts(file_path, "income_statement")
        # 4. On success: append ExtractionCompleted(facts=FinancialFacts(**facts), ...)
        # 5. On failure: append ExtractionFailed(error_type, error_message, partial_facts)
        # 6. await self._record_tool_call("week3_extraction_pipeline", ..., ms)
        # 7. await self._record_node_execution("extract_income_statement", ...)
        app_id = state["application_id"]
        doc = next((d for d in (state.get("document_paths") or []) if d.get("document_type")==DocumentType.INCOME_STATEMENT.value), None)
        if not doc:
            state["errors"].append("Income statement not found")
            await self._record_node_execution("extract_income_statement", ["document_paths"], ["errors"], int((time.time()-t)*1000))
            raise ValueError("Income statement not found")
        doc_id = doc.get("document_id") or f"doc-{uuid4().hex[:8]}"
        await self._append_stream(
            f"docpkg-{app_id}",
            ExtractionStarted(
                package_id=app_id,
                document_id=doc_id,
                document_type=DocumentType.INCOME_STATEMENT,
                pipeline_version="week3-v1.0",
                extraction_model="mineru-1.0",
                started_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )
        try:
            raw = _run_document_refinery(doc.get("file_path") or "")
            text = _extract_text_from_refinery(raw)
            facts = _parse_income_statement_text(text)

            field_conf: dict = {}
            notes: list[str] = []
            for key in ("total_revenue", "net_income", "ebitda"):
                if facts.get(key) is None:
                    field_conf[key] = 0.0
                    notes.append(f"Missing {key} from extraction")

            ff = FinancialFacts(
                total_revenue=facts.get("total_revenue"),
                gross_profit=facts.get("gross_profit"),
                operating_income=facts.get("operating_income"),
                ebitda=facts.get("ebitda"),
                interest_expense=facts.get("interest_expense"),
                depreciation_amortization=facts.get("depreciation_amortization"),
                net_income=facts.get("net_income"),
                field_confidence=field_conf,
                extraction_notes=notes,
            )
            tables = (raw.get("extracted") or {}).get("tables") or []
            await self._append_stream(
                f"docpkg-{app_id}",
                ExtractionCompleted(
                    package_id=app_id,
                    document_id=doc_id,
                    document_type=DocumentType.INCOME_STATEMENT,
                    facts=ff,
                    raw_text_length=len(text or ""),
                    tables_extracted=len(tables),
                    processing_ms=int((time.time()-t)*1000),
                    completed_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            await self._record_tool_call("document_refinery_api", doc.get("file_path",""), "income_statement facts", int((time.time()-t)*1000))
            await self._record_node_execution("extract_income_statement", ["document_paths"], ["extraction_results"], int((time.time()-t)*1000))
            return state
        except Exception as e:
            await self._append_stream(
                f"docpkg-{app_id}",
                ExtractionFailed(
                    package_id=app_id,
                    document_id=doc_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    failed_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            raise

    async def _node_extract_bs(self, state):
        t = time.time()
        # TODO: Same pattern as _node_extract_is but for balance sheet
        # Key difference: ExtractionCompleted for balance sheet should populate
        # total_assets, total_liabilities, total_equity, current_assets, etc.
        # The QualityAssessmentCompleted LLM will check Assets = Liabilities + Equity
        app_id = state["application_id"]
        doc = next((d for d in (state.get("document_paths") or []) if d.get("document_type")==DocumentType.BALANCE_SHEET.value), None)
        if not doc:
            state["errors"].append("Balance sheet not found")
            await self._record_node_execution("extract_balance_sheet", ["document_paths"], ["errors"], int((time.time()-t)*1000))
            raise ValueError("Balance sheet not found")
        doc_id = doc.get("document_id") or f"doc-{uuid4().hex[:8]}"
        await self._append_stream(
            f"docpkg-{app_id}",
            ExtractionStarted(
                package_id=app_id,
                document_id=doc_id,
                document_type=DocumentType.BALANCE_SHEET,
                pipeline_version="week3-v1.0",
                extraction_model="mineru-1.0",
                started_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )
        try:
            raw = _run_document_refinery(doc.get("file_path") or "")
            text = _extract_text_from_refinery(raw)
            facts = _parse_balance_sheet_text(text)

            field_conf: dict = {}
            notes: list[str] = []
            for key in ("total_assets", "total_liabilities", "total_equity"):
                if facts.get(key) is None:
                    field_conf[key] = 0.0
                    notes.append(f"Missing {key} from extraction")

            ff = FinancialFacts(
                total_assets=facts.get("total_assets"),
                total_liabilities=facts.get("total_liabilities"),
                total_equity=facts.get("total_equity"),
                current_assets=facts.get("current_assets"),
                current_liabilities=facts.get("current_liabilities"),
                cash_and_equivalents=facts.get("cash_and_equivalents"),
                accounts_receivable=facts.get("accounts_receivable"),
                inventory=facts.get("inventory"),
                long_term_debt=facts.get("long_term_debt"),
                field_confidence=field_conf,
                extraction_notes=notes,
            )
            tables = (raw.get("extracted") or {}).get("tables") or []
            await self._append_stream(
                f"docpkg-{app_id}",
                ExtractionCompleted(
                    package_id=app_id,
                    document_id=doc_id,
                    document_type=DocumentType.BALANCE_SHEET,
                    facts=ff,
                    raw_text_length=len(text or ""),
                    tables_extracted=len(tables),
                    processing_ms=int((time.time()-t)*1000),
                    completed_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            await self._record_tool_call("document_refinery_api", doc.get("file_path",""), "balance_sheet facts", int((time.time()-t)*1000))
            await self._record_node_execution("extract_balance_sheet", ["document_paths"], ["extraction_results"], int((time.time()-t)*1000))
            return state
        except Exception as e:
            await self._append_stream(
                f"docpkg-{app_id}",
                ExtractionFailed(
                    package_id=app_id,
                    document_id=doc_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    failed_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            raise

    async def _node_assess_quality(self, state):
        t = time.time()
        # TODO:
        # 1. Merge extraction results from IS + BS into a combined FinancialFacts
        # 2. Build LLM prompt asking for quality assessment (consistency check)
        # 3. content, ti, to, cost = await self._call_llm(SYSTEM, USER, 512)
        # 4. Parse DocumentQualityAssessment from JSON response
        # 5. Append QualityAssessmentCompleted to "docpkg-{app_id}" stream
        # 6. If critical_missing_fields: add to state["quality_flags"]
        # 7. await self._record_node_execution("assess_quality", ..., ms, ti, to, cost)
        app_id = state["application_id"]

        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extraction_events = [e for e in pkg_events if e.get("event_type") == "ExtractionCompleted"]
        merged: dict = {}
        for ev in extraction_events:
            facts = (ev.get("payload") or {}).get("facts") or {}
            for k, v in facts.items():
                if v is not None and k not in merged:
                    merged[k] = v

        critical_missing = [
            f for f in ("total_revenue", "net_income", "total_assets", "total_liabilities")
            if merged.get(f) is None
        ]

        user = json.dumps(
            {"facts": merged, "critical_missing_fields": critical_missing},
            indent=2,
            default=str,
        )

        try:
            content, ti, to, cost = await self._call_llm(QUALITY_SYSTEM_PROMPT, user, max_tokens=512)
            qa = _parse_json(content) or {}
        except Exception:
            qa = {}
            ti = to = 0
            cost = 0.0

        if not qa:
            total_assets = merged.get("total_assets") or 0.0
            total_liabilities = merged.get("total_liabilities") or 0.0
            total_equity = merged.get("total_equity") or 0.0
            diff = abs(total_assets - (total_liabilities + total_equity))
            is_coherent = total_assets == 0 or (diff / total_assets) < 0.02
            qa = {
                "overall_confidence": 0.9 if is_coherent else 0.6,
                "is_coherent": is_coherent,
                "anomalies": [] if is_coherent else ["BALANCE_SHEET_MISMATCH"],
                "critical_missing_fields": critical_missing,
                "reextraction_recommended": not is_coherent,
                "auditor_notes": "Fallback coherence check (LLM unavailable or unparseable)",
            }

        await self._append_stream(
            f"docpkg-{app_id}",
            QualityAssessmentCompleted(
                package_id=app_id,
                document_id=f"doc-{uuid4().hex[:8]}",
                overall_confidence=float(qa.get("overall_confidence", 0.7)),
                is_coherent=bool(qa.get("is_coherent", False)),
                anomalies=qa.get("anomalies", []) or [],
                critical_missing_fields=qa.get("critical_missing_fields", []) or [],
                reextraction_recommended=bool(qa.get("reextraction_recommended", False)),
                auditor_notes=qa.get("auditor_notes", ""),
                assessed_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )
        await self._append_stream(
            f"docpkg-{app_id}",
            PackageReadyForAnalysis(
                package_id=app_id,
                application_id=app_id,
                documents_processed=len(extraction_events),
                has_quality_flags=bool(qa.get("anomalies") or qa.get("critical_missing_fields")),
                quality_flag_count=len(qa.get("anomalies", []) or []) + len(qa.get("critical_missing_fields", []) or []),
                ready_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )
        await self._record_node_execution("assess_quality", ["extraction_results"], ["quality_assessment"], int((time.time()-t)*1000), ti, to, cost)
        return state

    async def _node_write_output(self, state):
        t = time.time()
        # TODO:
        # 1. Append PackageReadyForAnalysis to "docpkg-{app_id}" stream
        # 2. Append CreditAnalysisRequested to "loan-{app_id}" stream
        # 3. await self._record_output_written([...], summary)
        # 4. await self._record_node_execution("write_output", ...)
        # 5. return {**state, "next_agent": "credit_analysis"}
        app_id = state["application_id"]
        await self._append_stream(
            f"loan-{app_id}",
            CreditAnalysisRequested(
                application_id=app_id,
                requested_at=datetime.now().isoformat(),
                requested_by=f"system:session-{self.session_id}",
                priority="NORMAL",
            ).to_store_dict(),
        )
        await self._record_output_written(
            [
                {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"},
            ],
            "Document processing complete; credit analysis requested.",
        )
        await self._record_node_execution("write_output", ["quality_assessment"], ["events_written"], int((time.time()-t)*1000))
        return {**state, "next_agent": "credit_analysis"}

    def _load_financial_summary(self, file_path: str | None) -> dict:
        if file_path:
            base = os.path.dirname(os.path.dirname(file_path))
            summary = os.path.join(base, "financial_summary.csv")
        else:
            summary = None
        if summary and os.path.exists(summary):
            path = summary
        else:
            # fallback: search in documents root by application id not available
            return {}
        data = {}
        try:
            with open(path, "r") as f:
                next(f, None)
                for line in f:
                    parts = line.strip().split(",")
                    if len(parts) >= 2:
                        data[parts[0]] = _safe_float(parts[1])
        except Exception:
            return {}
        return {
            "total_revenue": data.get("total_revenue"),
            "gross_profit": data.get("gross_profit"),
            "operating_income": data.get("operating_income"),
            "ebitda": data.get("ebitda"),
            "interest_expense": data.get("interest_expense"),
            "depreciation_amortization": data.get("depreciation_amortization"),
            "net_income": data.get("net_income"),
            "total_assets": data.get("total_assets"),
            "total_liabilities": data.get("total_liabilities"),
            "total_equity": data.get("total_equity"),
            "current_assets": data.get("current_assets"),
            "current_liabilities": data.get("current_liabilities"),
            "cash_and_equivalents": data.get("cash_and_equivalents"),
            "accounts_receivable": data.get("accounts_receivable"),
            "inventory": data.get("inventory"),
            "long_term_debt": data.get("long_term_debt"),
        }


# ─── FRAUD DETECTION AGENT ───────────────────────────────────────────────────

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class FraudDetectionAgent(BaseApexAgent):
    """
    Cross-references extracted document facts against historical registry data.
    Detects anomalous discrepancies that suggest fraud or document manipulation.

    LangGraph nodes:
        validate_inputs → load_document_facts → cross_reference_registry →
        analyze_fraud_patterns → write_output

    Output events:
        fraud-{id}: FraudScreeningInitiated, FraudAnomalyDetected (0..N),
                    FraudScreeningCompleted
        loan-{id}:  ComplianceCheckRequested

    KEY SCORING LOGIC:
        fraud_score = base(0.05)
            + revenue_discrepancy_factor   (doc revenue vs prior year registry)
            + submission_pattern_factor    (channel, timing, IP region)
            + balance_sheet_consistency    (assets = liabilities + equity within tolerance)

        revenue_discrepancy_factor:
            gap = abs(doc_revenue - registry_prior_revenue) / registry_prior_revenue
            if gap > 0.40 and trajectory not in (GROWTH, RECOVERING): += 0.25

        FraudAnomalyDetected is appended for each anomaly where severity >= MEDIUM.
        fraud_score > 0.60 → recommendation = "DECLINE"
        fraud_score 0.30..0.60 → "FLAG_FOR_REVIEW"
        fraud_score < 0.30 → "PROCEED"

    LLM in _node_analyze():
        System: "You are a financial fraud analyst.
                 Given the cross-reference results, identify specific named anomalies.
                 For each anomaly: type, severity, evidence, affected_fields.
                 Compute a final fraud_score 0-1. Return FraudAssessment JSON."

    WHEN THIS WORKS:
        pytest tests/phase2/test_fraud_agent.py
          → FraudScreeningCompleted event in fraud stream
          → fraud_score between 0.0 and 1.0
          → ComplianceCheckRequested on loan stream
          → NARR-03 (crash recovery) test passes
    """

    def build_graph(self):
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_document_facts",     self._node_load_facts)
        g.add_node("cross_reference_registry",self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",  self._node_analyze)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):
        t = time.time()
        app_id = state["application_id"]
        errors = []

        loan_events = await self.store.load_stream(f"loan-{app_id}")
        applicant_id = None
        has_request = any(e.get("event_type") == "FraudScreeningRequested" for e in loan_events)
        for e in loan_events:
            if e.get("event_type") == "ApplicationSubmitted":
                applicant_id = e.get("payload", {}).get("applicant_id")
                break
        if not applicant_id:
            errors.append("Missing ApplicationSubmitted/applicant_id")
        if not has_request:
            errors.append("Missing FraudScreeningRequested on loan stream")

        doc_events = await self.store.load_stream(f"docpkg-{app_id}")
        has_facts = any(e.get("event_type") == "ExtractionCompleted" for e in doc_events)
        if not has_facts:
            errors.append("No ExtractionCompleted events found")

        ms = int((time.time()-t)*1000)
        if errors:
            await self._record_input_failed([], errors)
            await self._record_node_execution("validate_inputs", ["application_id"], ["errors"], ms)
            raise ValueError(f"Input validation failed: {errors}")

        await self._record_input_validated(["application_id","applicant_id"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["applicant_id"], ms)
        return {**state, "applicant_id": applicant_id}

    async def _node_load_facts(self, state):
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"docpkg-{app_id}")
        facts = {}
        for e in events:
            if e.get("event_type") != "ExtractionCompleted":
                continue
            payload = e.get("payload", {})
            f = payload.get("facts") or {}
            for k, v in f.items():
                if v is not None and k not in facts:
                    facts[k] = v
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", f"docpkg-{app_id}", "ExtractionCompleted loaded", ms)
        await self._record_node_execution("load_document_facts", ["application_id"], ["extracted_facts"], ms)
        return {**state, "extracted_facts": facts}

    async def _node_cross_reference(self, state):
        t = time.time()
        applicant_id = state.get("applicant_id")
        if not self.registry:
            raise ValueError("Registry client not configured")
        profile = await self.registry.get_company(applicant_id)
        hist = await self.registry.get_financial_history(applicant_id)
        flags = await self.registry.get_compliance_flags(applicant_id, active_only=False)
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={applicant_id}", "profile + financial history loaded", ms)
        profile_dict = profile.__dict__ if profile else {}
        profile_dict["compliance_flags"] = [f.__dict__ for f in flags]
        history = [h.__dict__ for h in hist]

        # Compute deltas: current vs prior year for revenue/EBITDA/margins
        current = state.get("extracted_facts") or {}
        prior = history[-1] if history else {}
        deltas = {}
        for key in ("total_revenue", "ebitda", "gross_margin", "net_margin"):
            cur = _safe_float(current.get(key))
            prv = _safe_float(prior.get(key)) if isinstance(prior, dict) else None
            if cur is not None and prv is not None and prv != 0:
                deltas[key] = (cur - prv) / prv
        signals = {
            "current_facts": current,
            "prior_year": prior,
            "deltas": deltas,
        }

        await self._record_node_execution("cross_reference_registry", ["applicant_id"], ["registry_profile","historical_financials","fraud_signals"], ms)
        return {**state, "registry_profile": profile_dict, "historical_financials": history, "fraud_signals": signals}

    async def _node_analyze(self, state):
        t = time.time()
        facts = state.get("extracted_facts") or {}
        history = state.get("historical_financials") or []
        signals = state.get("fraud_signals") or {}

        SYSTEM = """You are a financial fraud analyst.
Given the current-year extracted figures and 3-year historicals,
identify anomalous gaps or inconsistencies. Return ONLY JSON:
{
  "anomalies": [
    {"anomaly_type": "revenue_discrepancy" | "balance_sheet_inconsistency" | "unusual_submission_pattern" | "identity_mismatch" | "document_alteration_suspected",
     "description": "...",
     "severity": "LOW" | "MEDIUM" | "HIGH",
     "evidence": "...",
     "affected_fields": ["field"]
    }
  ]
}
"""

        USER = json.dumps(
            {
                "current_facts": facts,
                "history": history,
                "signals": signals,
            },
            indent=2,
            default=str,
        )

        anomalies = []
        ti = to = 0
        cost = 0.0
        try:
            content, ti, to, cost = await self._call_llm(SYSTEM, USER, max_tokens=512)
            data = _parse_json(content) or {}
            anomalies = data.get("anomalies") or []
        except Exception:
            anomalies = []

        # Compute fraud_score from anomaly severities
        weight = {"LOW": 0.10, "MEDIUM": 0.25, "HIGH": 0.45}
        fraud_score = 0.0
        for a in anomalies:
            sev = str(a.get("severity", "LOW")).upper()
            fraud_score += weight.get(sev, 0.10)
        fraud_score = min(fraud_score, 1.0)

        ms = int((time.time()-t)*1000)
        await self._record_node_execution("analyze_fraud_patterns", ["extracted_facts","historical_financials"], ["fraud_score","anomalies"], ms, ti, to, cost)
        return {**state, "fraud_score": fraud_score, "anomalies": anomalies}

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        fraud_score = state.get("fraud_score") or 0.0
        anomalies = state.get("anomalies") or []
        recommendation = "PROCEED"
        if fraud_score > 0.60:
            recommendation = "DECLINE"
        elif fraud_score >= 0.30:
            recommendation = "FLAG_FOR_REVIEW"

        risk_level = "LOW"
        if fraud_score > 0.60:
            risk_level = "HIGH"
        elif fraud_score >= 0.30:
            risk_level = "MEDIUM"

        # Append fraud stream events
        stream_id = f"fraud-{app_id}"
        await self._append_stream(stream_id, FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            screening_model_version="heuristic-v1",
            initiated_at=datetime.now().isoformat(),
        ).to_store_dict())

        for a in anomalies:
            at = str(a.get("anomaly_type", "")).lower()
            if at not in {t.value for t in FraudAnomalyType}:
                at = FraudAnomalyType.DOCUMENT_ALTERATION_SUSPECTED.value
            anomaly = FraudAnomaly(
                anomaly_type=FraudAnomalyType(at),
                description=a.get("description", a.get("evidence", "")),
                severity=str(a.get("severity", "MEDIUM")).upper(),
                evidence=a.get("evidence", ""),
                affected_fields=a.get("affected_fields", []) or [],
            )
            await self._append_stream(stream_id, FraudAnomalyDetected(
                application_id=app_id,
                session_id=self.session_id,
                anomaly=anomaly,
                detected_at=datetime.now().isoformat(),
            ).to_store_dict())

        await self._append_stream(stream_id, FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=fraud_score,
            risk_level=risk_level,
            anomalies_found=len(anomalies),
            recommendation=recommendation,
            screening_model_version="heuristic-v1",
            input_data_hash=self._sha(state.get("extracted_facts")),
            completed_at=datetime.now().isoformat(),
        ).to_store_dict())

        # Trigger compliance
        await self._append_stream(f"loan-{app_id}", ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.now().isoformat(),
            triggered_by_event_id="fraud_screening_completed",
            regulation_set_version="2026-Q1-v1",
            rules_to_evaluate=list(REGULATIONS.keys()),
        ).to_store_dict())

        await self._record_output_written(
            events_written=[
                {"stream_id": stream_id, "event_type": "FraudScreeningCompleted"},
                {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"},
            ],
            summary=f"fraud_score={fraud_score:.2f} recommendation={recommendation}",
        )
        await self._record_node_execution("write_output", ["fraud_score"], ["output_events"], int((time.time()-t)*1000))
        return {**state, "next_agent": "compliance"}


# ─── COMPLIANCE AGENT ─────────────────────────────────────────────────────────

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    rule_results: list[dict] | None
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2026 - (co.get("founded_year") or 2026)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,   # Always noted, never fails
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules in sequence.
    Stops at first hard block (is_hard_block=True).
    LLM not used in rule evaluation — only for human-readable evidence summaries.

    LangGraph nodes:
        validate_inputs → load_company_profile → evaluate_reg001 → evaluate_reg002 →
        evaluate_reg003 → evaluate_reg004 → evaluate_reg005 → evaluate_reg006 → write_output

    Note: Use conditional edges after each rule so hard blocks skip remaining rules.
    See add_conditional_edges() in LangGraph docs.

    Output events:
        compliance-{id}: ComplianceCheckInitiated,
                         ComplianceRulePassed/Failed/Noted (one per rule evaluated),
                         ComplianceCheckCompleted
        loan-{id}:       DecisionRequested (if no hard block)
                         ApplicationDeclined (if hard block)

    RULE EVALUATION PATTERN (each _node_evaluate_regXXX):
        1. co = state["company_profile"]
        2. passes = REGULATIONS[rule_id]["check"](co)
        3. eh = self._sha(f"{rule_id}-{co['company_id']}")
        4. If passes: append ComplianceRulePassed or ComplianceRuleNoted
        5. If fails: append ComplianceRuleFailed; if is_hard_block: set state["has_hard_block"]=True
        6. await self._record_node_execution(...)

    ROUTING:
        After each rule node, use conditional edge:
            g.add_conditional_edges(
                "evaluate_reg001",
                lambda s: "write_output" if s["has_hard_block"] else "evaluate_reg002",
            )

    WHEN THIS WORKS:
        pytest tests/phase2/test_compliance_agent.py
          → ComplianceCheckCompleted with correct verdict
          → NARR-04 (Montana REG-003 hard block): no DecisionRequested event,
            ApplicationDeclined present, adverse_action_notice_required=True
    """

    def build_graph(self):
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",     self._node_validate_inputs)
        g.add_node("load_company_profile",self._node_load_profile)
        async def _eval_001(s): return await self._evaluate_rule(s, "REG-001")
        async def _eval_002(s): return await self._evaluate_rule(s, "REG-002")
        async def _eval_003(s): return await self._evaluate_rule(s, "REG-003")
        async def _eval_004(s): return await self._evaluate_rule(s, "REG-004")
        async def _eval_005(s): return await self._evaluate_rule(s, "REG-005")
        async def _eval_006(s): return await self._evaluate_rule(s, "REG-006")
        g.add_node("evaluate_reg001",     _eval_001)
        g.add_node("evaluate_reg002",     _eval_002)
        g.add_node("evaluate_reg003",     _eval_003)
        g.add_node("evaluate_reg004",     _eval_004)
        g.add_node("evaluate_reg005",     _eval_005)
        g.add_node("evaluate_reg006",     _eval_006)
        g.add_node("write_output",        self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        # Conditional edges: stop at hard block, proceed otherwise
        for src, nxt in [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, rule_results=[], has_hard_block=False,
            block_rule_id=None, errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        applicant_id = None
        requested_amount = None
        has_request = any(e.get("event_type") == "ComplianceCheckRequested" for e in loan_events)
        for e in loan_events:
            if e.get("event_type") == "ApplicationSubmitted":
                p = e.get("payload", {})
                applicant_id = p.get("applicant_id")
                requested_amount = p.get("requested_amount_usd")
                break
        errors = []
        if not applicant_id:
            errors.append("Missing ApplicationSubmitted/applicant_id")
        if not has_request:
            errors.append("Missing ComplianceCheckRequested on loan stream")

        ms = int((time.time()-t)*1000)
        if errors:
            await self._record_input_failed([], errors)
            await self._record_node_execution("validate_inputs", ["application_id"], ["errors"], ms)
            raise ValueError(f"Input validation failed: {errors}")

        # Initiate compliance check
        await self._append_stream(
            f"compliance-{app_id}",
            ComplianceCheckInitiated(
                application_id=app_id,
                session_id=self.session_id,
                regulation_set_version="2026-Q1-v1",
                rules_to_evaluate=list(REGULATIONS.keys()),
                initiated_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )
        await self._record_input_validated(["application_id","applicant_id"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["applicant_id"], ms)
        return {**state, "applicant_id": applicant_id, "requested_amount_usd": requested_amount}

    async def _node_load_profile(self, state):
        t = time.time()
        applicant_id = state.get("applicant_id")
        profile = await self.registry.get_company(applicant_id)
        flags = await self.registry.get_compliance_flags(applicant_id, active_only=False)
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={applicant_id}", "profile + flags loaded", ms)
        if profile:
            company = profile.__dict__
        else:
            company = {"company_id": applicant_id}
        company["compliance_flags"] = [f.__dict__ for f in flags]
        company["requested_amount_usd"] = state.get("requested_amount_usd")
        await self._record_node_execution("load_company_profile", ["applicant_id"], ["company_profile"], ms)
        return {**state, "company_profile": company}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        """
        TODO:
        1. reg = REGULATIONS[rule_id]
        2. co = state["company_profile"] — add "requested_amount_usd" from app
        3. passes = reg["check"](co)
        4. evidence_hash = self._sha(f"{rule_id}-{co['company_id']}-{passes}")
        5. If REG-006 (always noted):
               append ComplianceRuleNoted to "compliance-{app_id}" stream
        6. Elif passes:
               append ComplianceRulePassed
        7. Else:
               append ComplianceRuleFailed
               if reg["is_hard_block"]: state["has_hard_block"]=True, state["block_rule_id"]=rule_id
        8. await self._record_node_execution(f"evaluate_{rule_id.lower().replace('-','_')}", ...)
        """
        t = time.time()
        reg = REGULATIONS[rule_id]
        co = dict(state.get("company_profile") or {})
        passes = reg["check"](co)
        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id')}-{passes}")

        if reg.get("note_type"):
            ev = ComplianceRuleNoted(
                application_id=state["application_id"],
                session_id=state["session_id"],
                rule_id=rule_id,
                rule_name=reg.get("name"),
                note_type=reg.get("note_type"),
                note_text=reg.get("note_text"),
                evaluated_at=datetime.now().isoformat(),
            )
            outcome = "NOTED"
        elif passes:
            ev = ComplianceRulePassed(
                application_id=state["application_id"],
                session_id=state["session_id"],
                rule_id=rule_id,
                rule_name=reg.get("name"),
                rule_version=reg.get("version"),
                evidence_hash=evidence_hash,
                evaluation_notes="passed",
                evaluated_at=datetime.now().isoformat(),
            )
            outcome = "PASSED"
        else:
            ev = ComplianceRuleFailed(
                application_id=state["application_id"],
                session_id=state["session_id"],
                rule_id=rule_id,
                rule_name=reg.get("name"),
                rule_version=reg.get("version"),
                failure_reason=reg.get("failure_reason") or "failed",
                is_hard_block=bool(reg.get("is_hard_block")),
                remediation_available=reg.get("remediation") is not None,
                remediation_description=reg.get("remediation"),
                evidence_hash=evidence_hash,
                evaluated_at=datetime.now().isoformat(),
            )
            outcome = "FAILED"
            if reg.get("is_hard_block"):
                state["has_hard_block"] = True
                state["block_rule_id"] = rule_id

        await self._append_stream(f"compliance-{state['application_id']}", ev.to_store_dict())
        state["rule_results"].append({"rule_id": rule_id, "outcome": outcome, "is_hard_block": reg.get("is_hard_block")})
        await self._record_node_execution(f"evaluate_{rule_id.lower().replace('-','_')}", ["company_profile"], ["rule_results"], int((time.time()-t)*1000))
        return state

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        results = state.get("rule_results") or []
        passed = sum(1 for r in results if r["outcome"] == "PASSED")
        failed = sum(1 for r in results if r["outcome"] == "FAILED")
        noted = sum(1 for r in results if r["outcome"] == "NOTED")
        has_block = state.get("has_hard_block", False)
        verdict = "BLOCKED" if has_block else "CLEAR"

        await self._append_stream(
            f"compliance-{app_id}",
            ComplianceCheckCompleted(
                application_id=app_id,
                session_id=self.session_id,
                rules_evaluated=len(results),
                rules_passed=passed,
                rules_failed=failed,
                rules_noted=noted,
                has_hard_block=has_block,
                overall_verdict=verdict,
                completed_at=datetime.now().isoformat(),
            ).to_store_dict(),
        )

        if has_block:
            await self._append_stream(
                f"loan-{app_id}",
                ApplicationDeclined(
                    application_id=app_id,
                    decline_reasons=[f"Compliance hard block: {state.get('block_rule_id')}"],
                    declined_by="compliance_agent",
                    adverse_action_notice_required=True,
                    adverse_action_codes=[],
                    declined_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            next_agent = None
        else:
            await self._append_stream(
                f"loan-{app_id}",
                DecisionRequested(
                    application_id=app_id,
                    requested_at=datetime.now().isoformat(),
                    all_analyses_complete=True,
                    triggered_by_event_id="compliance_check_completed",
                ).to_store_dict(),
            )
            next_agent = "decision_orchestrator"

        # Optional LLM summary (no effect on decision)
        summary_text = f"verdict={verdict} passed={passed} failed={failed} noted={noted}"
        try:
            system = "You are a compliance officer. Summarize rule outcomes in 1-2 sentences."
            user = json.dumps({"results": results, "verdict": verdict}, default=str)
            content, ti, to, cost = await self._call_llm(system, user, max_tokens=256)
            summary_text = content.strip()[:500]
        except Exception:
            ti = to = 0
            cost = 0.0

        await self._record_output_written(
            events_written=[
                {"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted"},
                {"stream_id": f"loan-{app_id}", "event_type": "DecisionRequested" if not has_block else "ApplicationDeclined"},
            ],
            summary=summary_text,
        )
        await self._record_node_execution("write_output", ["rule_results"], ["output_events"], int((time.time()-t)*1000), ti, to, cost)
        return {**state, "next_agent": next_agent}


# ─── DECISION ORCHESTRATOR ────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs into a final recommendation.
    The only agent that reads from multiple aggregate streams before deciding.

    LangGraph nodes:
        validate_inputs → load_credit_result → load_fraud_result →
        load_compliance_result → synthesize_decision → apply_hard_constraints →
        write_output

    Input streams read (load_* nodes):
        credit-{id}:     CreditAnalysisCompleted (last event of this type)
        fraud-{id}:      FraudScreeningCompleted
        compliance-{id}: ComplianceCheckCompleted

    Output events:
        loan-{id}:  DecisionGenerated
                    ApplicationApproved (if APPROVE)
                    ApplicationDeclined (if DECLINE)
                    HumanReviewRequested (if REFER)

    HARD CONSTRAINTS (Python, not LLM — applied in apply_hard_constraints node):
        1. compliance BLOCKED → recommendation = DECLINE (cannot override)
        2. confidence < 0.60 → recommendation = REFER
        3. fraud_score > 0.60 → recommendation = REFER
        4. risk_tier == HIGH and confidence < 0.70 → recommendation = REFER

    LLM in synthesize_decision:
        System: "You are a senior loan officer synthesising multi-agent analysis.
                 Produce a recommendation (APPROVE/DECLINE/REFER),
                 approved_amount_usd, executive_summary (3-5 sentences),
                 and key_risks list. Return OrchestratorDecision JSON."
        NOTE: The LLM recommendation may be overridden by apply_hard_constraints.
              Log this override in DecisionGenerated.policy_overrides_applied.

    WHEN THIS WORKS:
        pytest tests/phase2/test_orchestrator_agent.py
          → DecisionGenerated event on loan stream
          → NARR-05 (human override): DecisionGenerated.recommendation="DECLINE",
            followed by HumanReviewCompleted.override=True,
            followed by ApplicationApproved with correct override fields
    """

    def build_graph(self):
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_credit_result",      self._node_load_credit)
        g.add_node("load_fraud_result",       self._node_load_fraud)
        g.add_node("load_compliance_result",  self._node_load_compliance)
        g.add_node("synthesize_decision",     self._node_synthesize)
        g.add_node("apply_hard_constraints",  self._node_constraints)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):
        t = time.time()
        await self._record_node_execution("validate_inputs", ["application_id"], ["application_id"], int((time.time()-t)*1000))
        return state

    async def _node_load_credit(self, state):
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"credit-{app_id}")
        credit = None
        for e in reversed(events):
            if e.get("event_type") == "CreditAnalysisCompleted":
                credit = e.get("payload", {})
                break
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", f"credit-{app_id}", "CreditAnalysisCompleted loaded", ms)
        await self._record_node_execution("load_credit_result", ["application_id"], ["credit_result"], ms)
        return {**state, "credit_result": credit}

    async def _node_load_fraud(self, state):
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"fraud-{app_id}")
        fraud = None
        for e in reversed(events):
            if e.get("event_type") == "FraudScreeningCompleted":
                fraud = e.get("payload", {})
                break
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", f"fraud-{app_id}", "FraudScreeningCompleted loaded", ms)
        await self._record_node_execution("load_fraud_result", ["application_id"], ["fraud_result"], ms)
        return {**state, "fraud_result": fraud}

    async def _node_load_compliance(self, state):
        t = time.time()
        app_id = state["application_id"]
        events = await self.store.load_stream(f"compliance-{app_id}")
        comp = None
        for e in reversed(events):
            if e.get("event_type") == "ComplianceCheckCompleted":
                comp = e.get("payload", {})
                break
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", f"compliance-{app_id}", "ComplianceCheckCompleted loaded", ms)
        await self._record_node_execution("load_compliance_result", ["application_id"], ["compliance_result"], ms)
        return {**state, "compliance_result": comp}

    async def _node_synthesize(self, state):
        t = time.time()
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        risk_tier = (credit.get("decision") or {}).get("risk_tier")
        confidence = (credit.get("decision") or {}).get("confidence") or 0.7
        approved_amount = (credit.get("decision") or {}).get("recommended_limit_usd")
        fraud_score = fraud.get("fraud_score") or 0.0
        compliance_verdict = compliance.get("overall_verdict") or "CLEAR"

        recommendation = "APPROVE"
        if compliance_verdict == "BLOCKED":
            recommendation = "DECLINE"
        elif risk_tier == "HIGH" or fraud_score > 0.60:
            recommendation = "REFER"

        summary = (
            f"Credit risk tier {risk_tier}, confidence {confidence:.2f}. "
            f"Fraud score {fraud_score:.2f}. Compliance verdict {compliance_verdict}."
        )

        ms = int((time.time()-t)*1000)
        await self._record_node_execution("synthesize_decision", ["credit_result","fraud_result","compliance_result"],
                                          ["recommendation","executive_summary","approved_amount"], ms)
        return {
            **state,
            "recommendation": recommendation,
            "confidence": confidence,
            "approved_amount": approved_amount,
            "executive_summary": summary,
            "conditions": [],
        }

    async def _node_constraints(self, state):
        t = time.time()
        overrides = list(state.get("hard_constraints_applied") or [])
        recommendation = state.get("recommendation")
        confidence = state.get("confidence") or 0.0
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        fraud_score = fraud.get("fraud_score") or 0.0
        risk_tier = (credit.get("decision") or {}).get("risk_tier")
        compliance_verdict = compliance.get("overall_verdict") or "CLEAR"

        if compliance_verdict == "BLOCKED" and recommendation != "DECLINE":
            recommendation = "DECLINE"
            overrides.append("compliance_blocked")
        if confidence < 0.60 and recommendation != "REFER":
            recommendation = "REFER"
            overrides.append("low_confidence")
        if fraud_score > 0.60 and recommendation != "REFER":
            recommendation = "REFER"
            overrides.append("high_fraud_score")
        if risk_tier == "HIGH" and confidence < 0.70 and recommendation != "REFER":
            recommendation = "REFER"
            overrides.append("high_risk_low_confidence")

        ms = int((time.time()-t)*1000)
        await self._record_node_execution("apply_hard_constraints", ["recommendation","confidence"], ["recommendation","hard_constraints_applied"], ms)
        return {**state, "recommendation": recommendation, "hard_constraints_applied": overrides}

    async def _node_write_output(self, state):
        t = time.time()
        app_id = state["application_id"]
        recommendation = state.get("recommendation") or "REFER"
        confidence = state.get("confidence") or 0.7
        approved_amount = state.get("approved_amount")
        summary = state.get("executive_summary") or ""
        key_risks = []

        decision_event = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=recommendation,
            confidence=confidence,
            approved_amount_usd=approved_amount,
            conditions=state.get("conditions") or [],
            executive_summary=summary,
            key_risks=key_risks,
            contributing_sessions=[],
            model_versions={},
            generated_at=datetime.now().isoformat(),
        )
        await self._append_stream(f"loan-{app_id}", decision_event.to_store_dict())

        # Load decision event id for references if needed
        decision_id = None
        events = await self.store.load_stream(f"loan-{app_id}")
        if events:
            decision_id = events[-1].get("event_id")

        if recommendation == "REFER":
            await self._append_stream(
                f"loan-{app_id}",
                HumanReviewRequested(
                    application_id=app_id,
                    reason="Low confidence or risk flags",
                    decision_event_id=str(decision_id or "pending"),
                    assigned_to=None,
                    requested_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            next_agent = "human_review"
        elif recommendation == "APPROVE":
            await self._append_stream(
                f"loan-{app_id}",
                ApplicationApproved(
                    application_id=app_id,
                    approved_amount_usd=approved_amount or 0.0,
                    interest_rate_pct=8.5,
                    term_months=36,
                    conditions=[],
                    approved_by="decision_orchestrator",
                    effective_date=datetime.now().date().isoformat(),
                    approved_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            next_agent = None
        else:
            await self._append_stream(
                f"loan-{app_id}",
                ApplicationDeclined(
                    application_id=app_id,
                    decline_reasons=state.get("hard_constraints_applied") or ["policy_decline"],
                    declined_by="decision_orchestrator",
                    adverse_action_notice_required=True,
                    adverse_action_codes=[],
                    declined_at=datetime.now().isoformat(),
                ).to_store_dict(),
            )
            next_agent = None

        await self._record_output_written(
            events_written=[
                {"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"},
            ],
            summary=f"recommendation={recommendation}",
        )
        await self._record_node_execution("write_output", ["recommendation"], ["output_events"], int((time.time()-t)*1000))
        return {**state, "next_agent": next_agent}
