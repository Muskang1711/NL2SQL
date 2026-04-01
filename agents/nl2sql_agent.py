import sys
import os
import time
import json
import re
import logging
import importlib
import importlib.util
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

from utils.logger import setup_logger

logger = setup_logger(__name__)

# ── Load nl2sql modules without polluting sys.path ──
_NL2SQL_DIR = Path(__file__).resolve().parent.parent / "nl2sql"


def _import_nl2sql_module(module_name: str):
    """Import a module from nl2sql/ directory without sys.path manipulation."""
    module_path = _NL2SQL_DIR / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"nl2sql_{module_name}", str(module_path)
    )
    module = importlib.util.module_from_spec(spec)
    # Need nl2sql/config.py loaded first for other modules
    if module_name != "config":
        # Ensure nl2sql config is available as 'config' in the module's namespace
        nl2sql_config = _import_nl2sql_module("config")
        sys.modules["nl2sql_config_temp"] = nl2sql_config
        # Temporarily override config in sys.modules for the import
        original_config = sys.modules.get("config")
        sys.modules["config"] = nl2sql_config
        try:
            spec.loader.exec_module(module)
        finally:
            # Restore original config
            if original_config is not None:
                sys.modules["config"] = original_config
            else:
                del sys.modules["config"]
    else:
        spec.loader.exec_module(module)
    return module


# Import nl2sql services
_nl2sql_config = _import_nl2sql_module("config")
_nl2sql_llm = _import_nl2sql_module("llm_service")
_nl2sql_db = _import_nl2sql_module("db_service")

LLMService = _nl2sql_llm.LLMService
DatabaseService = _nl2sql_db.DatabaseService


class CustomJSONEncoder(json.JSONEncoder):
    """Handle datetime, Decimal, bytes in JSON serialization."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


class NL2SQLAgent:
    """
    NL2SQL Agent that uses LLM to convert natural language to SQL.
    Enriched with FK/PK relationships for accurate JOIN generation.
    """

    def __init__(self):
        self.llm_service = LLMService()
        self.db_service = DatabaseService()
        logger.info(
            f"NL2SQL Agent initialized — LLM: {self.llm_service.provider}/{self.llm_service.model_name}"
        )

    def _build_fk_context(self, schema: str, fk_relationships: list = None,
                          true_pks: dict = None) -> str:
        """
        Build FK/PK context string to inject into LLM prompt.
        This tells the LLM exactly which JOIN conditions to use.
        """
        lines = []

        if true_pks:
            lines.append("## PRIMARY KEYS (use these for JOINs):")
            for table, pk_col in sorted(true_pks.items()):
                if pk_col:
                    lines.append(f"  - {schema}.{table}.{pk_col} (PRIMARY KEY)")
            lines.append("")

        if fk_relationships:
            lines.append("## FOREIGN KEY RELATIONSHIPS (use these for JOINs):")
            lines.append("## Format: child_table.fk_column → parent_table.pk_column")
            for fk in fk_relationships:
                fk_table = fk.get("fk_table", "")
                fk_column = fk.get("fk_column", "")
                ref_table = fk.get("ref_table", "")
                ref_column = fk.get("ref_column", "")
                status = fk.get("status", "confirmed")
                if status in ("confirmed", "partial"):
                    lines.append(
                        f"  - {schema}.{fk_table}.{fk_column} → "
                        f"{schema}.{ref_table}.{ref_column}"
                    )
            lines.append("")
            lines.append("## JOIN RULES:")
            lines.append("  - ALWAYS use the FK relationships above for JOIN conditions")
            lines.append("  - NEVER guess JOIN conditions — only use the FKs listed above")
            lines.append("  - If a JOIN path doesn't exist in the FKs above, say so")
            lines.append("")

        return "\n".join(lines)

    def process_query(
        self,
        user_query: str,
        schema: str = "shopify",
        fk_relationships: list = None,
        true_pks: dict = None,
    ) -> dict:
        """
        Full NL2SQL pipeline: query → SQL → execute → results.

        Args:
            user_query: Natural language question
            schema: Database schema to query
            fk_relationships: Confirmed FK relationships from FK detection pipeline
            true_pks: True PKs per table from FK detection pipeline

        Returns:
            {
                "status": "success" | "failed",
                "query": original NL query,
                "generated_sql": final SQL,
                "columns": [...],
                "data": [...],
                "row_count": int,
                "attempts": [...],
                "pipeline_log": [...]
            }
        """
        start_time = time.time()
        pipeline_log = []

        def log_step(step: str, detail: str = ""):
            elapsed = round(time.time() - start_time, 2)
            entry = {"step": step, "detail": detail, "elapsed_seconds": elapsed}
            pipeline_log.append(entry)
            logger.info(f"[NL2SQL] {step}: {detail} ({elapsed}s)")

        # ── Step 1: Get metadata context ──
        log_step("metadata_retrieval", f"Fetching metadata for schema: {schema}")
        metadata_context = self.db_service.get_full_metadata(schema)

        if "No tables found" in metadata_context:
            return {
                "status": "failed",
                "query": user_query,
                "error": f"No tables found in schema '{schema}'. Upload data first.",
                "pipeline_log": pipeline_log,
            }

        # ── Step 2: Inject FK/PK context ──
        fk_context = self._build_fk_context(schema, fk_relationships, true_pks)
        if fk_context:
            log_step("fk_injection", f"Injecting {len(fk_relationships or [])} FK relationships")
            metadata_context += "\n\n" + fk_context

        # ── Step 3: Generate SQL via LLM ──
        log_step("sql_generation", f"Sending to LLM: {user_query[:80]}...")
        llm_result = self.llm_service.generate_sql(user_query, metadata_context)

        if llm_result["status"] == "error":
            return {
                "status": "failed",
                "query": user_query,
                "error": f"LLM error: {llm_result.get('error', 'Unknown')}",
                "pipeline_log": pipeline_log,
            }

        generated_sql = llm_result["sql"]
        all_attempts = llm_result["attempts"]
        log_step("sql_generated", f"SQL: {generated_sql[:120]}...")

        # ── Step 4: Validate safety ──
        log_step("safety_validation", "Checking query is read-only...")
        safety = self.db_service.validate_sql_readonly(generated_sql)
        if not safety["safe"]:
            log_step("safety_blocked", safety["reason"])
            return {
                "status": "failed",
                "query": user_query,
                "generated_sql": generated_sql,
                "error": safety["reason"],
                "pipeline_log": pipeline_log,
            }

        # ── Step 5: Execute + Self-Correction Loop ──
        current_sql = generated_sql
        final_result = None
        max_attempts = self.llm_service.max_attempts

        for attempt in range(1, max_attempts + 1):
            log_step("sql_execution", f"Attempt {attempt}: executing SQL...")

            exec_result = self.db_service.execute_query(current_sql)

            if exec_result["status"] == "success":
                log_step("execution_success", f"Query succeeded on attempt {attempt}")
                final_result = exec_result
                all_attempts.append({
                    "attempt": attempt,
                    "sql": current_sql,
                    "status": "success",
                })
                break
            else:
                # Self-correction: feed error back to LLM
                error_msg = exec_result.get("error_message", "Unknown error")
                error_detail = exec_result.get("error_detail")
                error_hint = exec_result.get("error_hint")

                log_step(
                    "execution_error",
                    f"Attempt {attempt} failed: {error_msg[:150]}"
                )

                all_attempts.append({
                    "attempt": attempt,
                    "sql": current_sql,
                    "status": "error",
                    "error": error_msg,
                })

                if attempt < max_attempts:
                    log_step(
                        "self_correction",
                        f"Feeding error to LLM for correction (attempt {attempt + 1})..."
                    )

                    correction = self.llm_service.correct_sql(
                        original_query=user_query,
                        failed_sql=current_sql,
                        error_message=error_msg,
                        metadata_context=metadata_context,
                        error_detail=error_detail,
                        error_hint=error_hint,
                        attempt_number=attempt + 1,
                    )

                    if correction["status"] == "error":
                        log_step("correction_failed", "LLM correction failed")
                        break

                    current_sql = correction["sql"]

                    # Re-validate safety
                    safety = self.db_service.validate_sql_readonly(current_sql)
                    if not safety["safe"]:
                        log_step("safety_blocked", f"Corrected SQL blocked: {safety['reason']}")
                        break
                else:
                    log_step("max_attempts_reached", f"All {max_attempts} attempts exhausted.")

        total_time = round(time.time() - start_time, 2)

        # ── Build response ──
        if final_result and final_result["status"] == "success":
            # Serialize data through CustomJSONEncoder
            data = final_result.get("data", [])
            serialized = json.loads(json.dumps(data, cls=CustomJSONEncoder))

            return {
                "status": "success",
                "query": user_query,
                "generated_sql": current_sql,
                "columns": final_result.get("columns", []),
                "data": serialized,
                "row_count": final_result.get("row_count", 0),
                "truncated": final_result.get("truncated", False),
                "attempts": all_attempts,
                "total_attempts": len(all_attempts),
                "total_time_seconds": total_time,
                "model_used": f"{self.llm_service.provider}/{self.llm_service.model_name}",
                "pipeline_log": pipeline_log,
            }
        else:
            last_error = (
                all_attempts[-1].get("error", "Unknown error")
                if all_attempts else "No attempts made"
            )
            return {
                "status": "failed",
                "query": user_query,
                "generated_sql": current_sql,
                "error": (
                    f"Could not generate valid SQL after {len(all_attempts)} attempts. "
                    f"Last error: {last_error}"
                ),
                "attempts": all_attempts,
                "total_attempts": len(all_attempts),
                "total_time_seconds": total_time,
                "pipeline_log": pipeline_log,
            }
