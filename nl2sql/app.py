import logging
import json
import time
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from config import Config
from db_service import DatabaseService
from llm_service import LLMService
from csv_service import CSVService

# ─── Logging Setup ─────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nl2sql")

# ─── App Init ──────────────────────────────────────
app = FastAPI(
    title="NL2SQL",
    description="Natural Language to SQL with self-correction",
    version="1.0.0",
)

# Mount static files
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Services
db_service = DatabaseService()
llm_service = LLMService()
csv_service = CSVService()

# Cache metadata (refresh on demand)
_metadata_cache: dict = {}


@app.on_event("startup")
def startup_clear_tables():
    try:
        tables = csv_service.list_imported_tables("shopify")
        if tables:
            logger.info(f"Startup cleanup: dropping {len(tables)} existing tables...")
            for t in tables:
                csv_service.delete_table(t["table_name"], "shopify")
                logger.info(f"  Dropped: {t['table_name']}")
            # Clear metadata cache
            _metadata_cache.clear()
            logger.info("Startup cleanup complete — clean slate ready for CSV uploads.")
        else:
            logger.info("Startup: no existing tables — clean slate ready.")
    except Exception as e:
        logger.warning(f"Startup cleanup warning: {e}")


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


def get_metadata(schema: str = "shopify", force_refresh: bool = False) -> str:
    global _metadata_cache
    if schema not in _metadata_cache or force_refresh:
        logger.info(f"Fetching metadata for schema: {schema}")
        _metadata_cache[schema] = db_service.get_full_metadata(schema)
    return _metadata_cache[schema]


# ─── Routes ────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    html_file = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health_check():
    """Check system health — DB + LLM connectivity."""
    db_status = db_service.test_connection()
    llm_info = llm_service.get_info()
    return {
        "status": "healthy" if db_status["status"] == "connected" and llm_info["configured"] else "degraded",
        "database": db_status,
        "llm": llm_info,
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/schemas")
async def list_schemas():
    """List available database schemas."""
    schemas = db_service.get_all_schemas()
    return {"schemas": schemas}


@app.get("/api/tables/{schema}")
async def list_tables(schema: str = "shopify"):
    """List tables in a schema."""
    tables = db_service.get_all_tables(schema)
    return {"schema": schema, "tables": tables}


@app.get("/api/metadata/{schema}")
async def get_schema_metadata(schema: str = "shopify"):
    """Get full metadata for a schema (what LLM sees)."""
    metadata = get_metadata(schema, force_refresh=True)
    return {"schema": schema, "metadata": metadata}


@app.post("/api/query")
async def process_query(request: Request):

    body = await request.json()
    user_query = body.get("query", "").strip()
    schema = body.get("schema", "shopify")

    if not user_query:
        return JSONResponse(
            status_code=400,
            content={"error": "Query cannot be empty."},
        )

    start_time = time.time()
    pipeline_log = []

    def log_step(step: str, detail: str = ""):
        elapsed = round(time.time() - start_time, 2)
        entry = {"step": step, "detail": detail, "elapsed_seconds": elapsed}
        pipeline_log.append(entry)
        logger.info(f"[Pipeline] {step}: {detail} ({elapsed}s)")

    # ── Step 1: Get metadata context (RAG) ──
    log_step("metadata_retrieval", f"Fetching metadata for schema: {schema}")
    metadata_context = get_metadata(schema)

    if "No tables found" in metadata_context:
        return JSONResponse(
            status_code=400,
            content={
                "error": "No dataset uploaded yet. Please upload a CSV file first using the Data Sources panel above.",
                "pipeline_log": pipeline_log,
            },
        )

    # ── Step 2: Generate SQL via LLM ──
    log_step("sql_generation", f"Sending to Gemini: {user_query[:80]}...")
    llm_result = llm_service.generate_sql(user_query, metadata_context)

    if llm_result["status"] == "error":
        return JSONResponse(
            status_code=500,
            content={
                "error": f"LLM error: {llm_result.get('error', 'Unknown error')}",
                "pipeline_log": pipeline_log,
            },
        )

    generated_sql = llm_result["sql"]
    all_attempts = llm_result["attempts"]

    # ── Step 3: Validate safety ──
    log_step("safety_validation", "Checking query safety...")
    safety = db_service.validate_sql_readonly(generated_sql)
    if not safety["safe"]:
        log_step("safety_blocked", safety["reason"])
        return JSONResponse(
            status_code=400,
            content={
                "error": safety["reason"],
                "generated_sql": generated_sql,
                "pipeline_log": pipeline_log,
            },
        )

    # ── Step 4: Execute + Self-Correction Loop ──
    current_sql = generated_sql
    final_result = None
    max_attempts = Config.MAX_CORRECTION_ATTEMPTS

    for attempt in range(1, max_attempts + 1):
        log_step("sql_execution", f"Attempt {attempt}: executing SQL...")

        exec_result = db_service.execute_query(current_sql)

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
            # ── Self-Correction (like Athena error feedback in AWS solution) ──
            error_msg = exec_result.get("error_message", "Unknown error")
            error_detail = exec_result.get("error_detail")
            error_hint = exec_result.get("error_hint")

            # Friendly error for missing columns/tables
            friendly_error = error_msg
            if 'column' in error_msg.lower() and 'does not exist' in error_msg.lower():
                friendly_error = f"Column not found in dataset. {error_msg}"
            elif 'relation' in error_msg.lower() and 'does not exist' in error_msg.lower():
                friendly_error = f"Table not found in dataset. {error_msg}"

            log_step(
                "execution_error",
                f"Attempt {attempt} failed: {friendly_error[:150]}"
            )

            all_attempts.append({
                "attempt": attempt,
                "sql": current_sql,
                "status": "error",
                "error": friendly_error,
            })

            if attempt < max_attempts:
                log_step(
                    "self_correction",
                    f"Feeding error to LLM for correction (attempt {attempt + 1})..."
                )

                correction = llm_service.correct_sql(
                    original_query=user_query,
                    failed_sql=current_sql,
                    error_message=error_msg,
                    metadata_context=metadata_context,
                    error_detail=error_detail,
                    error_hint=error_hint,
                    attempt_number=attempt + 1,
                )

                if correction["status"] == "error":
                    log_step("correction_failed", f"LLM correction failed: {correction.get('error')}")
                    break

                current_sql = correction["sql"]

                # Re-validate safety of corrected SQL
                safety = db_service.validate_sql_readonly(current_sql)
                if not safety["safe"]:
                    log_step("safety_blocked", f"Corrected SQL blocked: {safety['reason']}")
                    break
            else:
                log_step("max_attempts_reached", f"All {max_attempts} attempts exhausted.")

    total_time = round(time.time() - start_time, 2)

    # ── Build response ──
    if final_result and final_result["status"] == "success":
        response_data = {
            "status": "success",
            "query": user_query,
            "generated_sql": current_sql,
            "columns": final_result.get("columns", []),
            "data": final_result.get("data", []),
            "row_count": final_result.get("row_count", 0),
            "truncated": final_result.get("truncated", False),
            "attempts": all_attempts,
            "total_attempts": len(all_attempts),
            "total_time_seconds": total_time,
            "pipeline_log": pipeline_log,
        }
    else:
        last_error = all_attempts[-1].get("error", "Unknown error") if all_attempts else "No attempts made"
        response_data = {
            "status": "failed",
            "query": user_query,
            "generated_sql": current_sql,
            "error": f"Could not generate a valid SQL query after {len(all_attempts)} attempts. Last error: {last_error}",
            "attempts": all_attempts,
            "total_attempts": len(all_attempts),
            "total_time_seconds": total_time,
            "pipeline_log": pipeline_log,
        }

    return JSONResponse(
        content=json.loads(json.dumps(response_data, cls=CustomJSONEncoder))
    )


@app.post("/api/refresh-metadata")
async def refresh_metadata(request: Request):
    body = await request.json()
    schema = body.get("schema", "shopify")
    metadata = get_metadata(schema, force_refresh=True)
    return {"status": "refreshed", "schema": schema, "preview": metadata[:500]}


# ─── CSV Upload & Data Management ─────────────────

@app.post("/api/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    table_name: str = Form(default=""),
):
    # Force usage of the 'shopify' schema regardless of client input
    schema = "shopify"

    if not file.filename.lower().endswith('.csv'):
        return JSONResponse(
            status_code=400,
            content={"error": "Only CSV files are supported."},
        )

    content = await file.read()
    if len(content) == 0:
        return JSONResponse(
            status_code=400,
            content={"error": "File is empty."},
        )

    result = csv_service.import_csv_from_upload(
        file_content=content,
        filename=file.filename,
        table_name=table_name.strip() or None,
        schema=schema,
    )

    # After successful import: enrich column descriptions with LLM-generated synonyms
    if result["status"] == "success":
        # Extract table name from result (e.g. "shopify.my_table" → "my_table")
        tbl_name = result.get("table_name", "").split(".")[-1]
        if tbl_name:
            logger.info(f"Enriching column descriptions for {tbl_name}...")
            csv_service.enrich_column_descriptions(tbl_name, schema, llm_service)
        get_metadata(schema, force_refresh=True)

    status_code = 200 if result["status"] == "success" else 400
    return JSONResponse(status_code=status_code, content=result)


@app.get("/api/data-tables/{schema}")
async def list_data_tables(schema: str = "shopify"):
    tables = csv_service.list_imported_tables(schema)
    return {"schema": schema, "tables": tables}


@app.delete("/api/data-tables/{schema}/{table_name}")
async def delete_data_table(schema: str, table_name: str):
    result = csv_service.delete_table(table_name, schema)
    if result["status"] == "success":
        get_metadata(schema, force_refresh=True)
    return result


# ─── Entry Point ───────────────────────────────────
if __name__ == "__main__":
    # Validate config
    errors = Config.validate()
    if errors:
        logger.warning("Configuration warnings:")
        for e in errors:
            logger.warning(f"  ⚠ {e}")

    llm_info = llm_service.get_info()

    logger.info("  NL2SQL — Natural Language to SQL")
    logger.info(f"  Database: {Config.DB_NAME}@{Config.DB_HOST}:{Config.DB_PORT}")
    logger.info(f"  LLM: {llm_info['provider']} ({llm_info['model']})")
    logger.info(f"  Mode: Clean slate — only CSV uploads, no mock data")
    logger.info(f"  Max correction attempts: {Config.MAX_CORRECTION_ATTEMPTS}")


    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=Config.APP_PORT,
        reload=True,
    )
