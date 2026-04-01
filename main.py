from fastapi import FastAPI, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import shutil
from pathlib import Path
from config import get_settings
from utils.logger import setup_logger
from services.excel_service import ExcelService
from services.data_analyzer import DataAnalyzer
from database.connection import DatabaseManager
from agents.orchestrator import Orchestrator
from agents.nl2sql_agent import NL2SQLAgent
from models.schemas import (
    FileUploadResponse, AnalysisResponse, ProcessingStatus,
    IngestResponse, PipelineResult
)

logger = setup_logger(__name__)

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Mount static files path
FRONTEND_DIR = Path(__file__).parent / "nl2sql" / "static"

# Cache for FK analysis results (populated by /analyze, used by /query)
_fk_cache: dict = {
    "true_pks": {},
    "confirmed_fks": [],
}


async def _run_fk_analysis():
    """
    Run the FK detection pipeline (Orchestrator) and cache PK/FK results.
    Called automatically after ingest / CSV upload so NL2SQL queries
    can use the relationships for JOINs and GROUP BY.
    """
    try:
        logger.info("Auto-running FK analysis after data import...")
        orchestrator = Orchestrator()
        result = await orchestrator.run_pipeline()

        _fk_cache["true_pks"] = result.get("true_pks", {})
        confirmed = result.get("results", {}).get("confirmed_fks", [])
        _fk_cache["confirmed_fks"] = confirmed
        logger.info(f"FK analysis done: {len(_fk_cache['true_pks'])} PKs, {len(confirmed)} FKs cached")
        return result
    except Exception as e:
        logger.error(f"FK analysis failed (non-fatal): {e}")
        return None

# NL2SQL agent (initialized lazily)
_nl2sql_agent: NL2SQLAgent = None


def _get_nl2sql_agent() -> NL2SQLAgent:
    global _nl2sql_agent
    if _nl2sql_agent is None:
        _nl2sql_agent = NL2SQLAgent()
    return _nl2sql_agent


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")
    DatabaseManager.initialize_pool()
    yield
    logger.info("Application shutdown")
    DatabaseManager.close_pool()


app = FastAPI(title="NL2SQL System", lifespan=lifespan)

# Mount static files folder
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main frontend UI."""
    index_file = FRONTEND_DIR / "index.html"
    return index_file.read_text(encoding="utf-8")

@app.post("/upload", response_model=FileUploadResponse)
async def upload_excel(file: UploadFile):
    """Upload an Excel file and parse all sheets into memory."""
    logger.info(f"Received file upload: {file.filename}")

    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Only Excel files accepted")

    file_path = UPLOAD_DIR / file.filename
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    session_id = ExcelService.load_excel(str(file_path))
    session = ExcelService.get_session(session_id)

    return FileUploadResponse(
        session_id=session_id,
        filename=file.filename,
        sheets_found=session["original_sheets"],
        status=ProcessingStatus.PENDING
    )


@app.post("/ingest/{session_id}", response_model=IngestResponse)
async def ingest_to_database(session_id: str):
    """
    Analyze all sheets in the session, create tables in Postgres,
    and insert the data. This is the data ingestion step.
    """
    logger.info(f"Ingesting session: {session_id}")

    try:
        session = ExcelService.get_session(session_id)
        sheets = session["original_sheets"]
        created_tables = []

        for sheet_name in sheets:
            # Analyze sheet to detect header row and clean column names
            analysis = ExcelService.analyze_sheet(session_id, sheet_name)
            df = analysis["dataframe"]

            # Standardize column names
            df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]

            # Infer SQL types
            columns = {}
            for col in df.columns:
                columns[col] = DataAnalyzer.infer_sql_type(df[col])

            # Detect PK candidate and mark it
            pk_candidates, _ = DataAnalyzer.detect_potential_keys(df)
            if pk_candidates:
                pk_col = pk_candidates[0]
                if pk_col in columns:
                    columns[pk_col] = columns[pk_col] + " PRIMARY KEY"

            # Drop existing table if it exists, then create fresh and insert data
            table_name = sheet_name.strip().replace(" ", "_").lower()
            DatabaseManager.drop_table(table_name)
            DatabaseManager.create_table(table_name, columns)
            DatabaseManager.insert_dataframe(table_name, df)
            created_tables.append(table_name)
            logger.info(f"Ingested sheet '{sheet_name}' as table '{table_name}'")

        # Auto-run FK analysis to detect PK/FK relationships
        await _run_fk_analysis()

        return IngestResponse(
            session_id=session_id,
            tables_created=created_tables,
            status=ProcessingStatus.COMPLETED
        )

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(500, str(e))


@app.post("/analyze/{session_id}")
async def analyze_fk_relationships(session_id: str):
    """
    Run the full FK detection pipeline via MCP server.
    Results are cached for use by /query endpoint.
    """
    logger.info(f"Running FK analysis for session: {session_id}")

    try:
        orchestrator = Orchestrator()
        result = await orchestrator.run_pipeline()

        # Cache FK results for NL2SQL queries
        _fk_cache["true_pks"] = result.get("true_pks", {})
        confirmed = result.get("results", {}).get("confirmed_fks", [])
        _fk_cache["confirmed_fks"] = confirmed
        logger.info(f"Cached {len(confirmed)} confirmed FKs for NL2SQL")

        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"FK analysis failed: {e}")
        raise HTTPException(500, str(e))


# ─── NL2SQL API Endpoints (frontend uses /api/* prefix) ────

# Metadata cache for LLM context
_metadata_cache: dict = {}


def _get_metadata(schema: str, force_refresh: bool = False) -> str:
    """Get or refresh metadata context for a schema."""
    agent = _get_nl2sql_agent()
    if force_refresh or schema not in _metadata_cache:
        _metadata_cache[schema] = agent.db_service.get_full_metadata(schema)
    return _metadata_cache[schema]


@app.get("/api/health")
async def api_health_check():
    """Health check — returns DB connection status and LLM info for frontend."""
    agent = _get_nl2sql_agent()
    db_status = agent.db_service.test_connection()
    return {
        "status": "healthy" if db_status.get("status") == "connected" else "degraded",
        "database": db_status,
        "llm": {
            "model": f"{agent.llm_service.provider}/{agent.llm_service.model_name}",
            "configured": True,
        },
    }


@app.get("/api/schemas")
async def api_list_schemas():
    """List available database schemas."""
    agent = _get_nl2sql_agent()
    schemas = agent.db_service.get_all_schemas()
    return {"schemas": schemas}


@app.post("/api/query")
async def api_nl2sql_query(request: Request):
    """Convert natural language to SQL, execute, and return results."""
    body = await request.json()
    user_query = body.get("query", "").strip()
    schema = body.get("schema", "shopify")

    if not user_query:
        return JSONResponse(
            status_code=400,
            content={"status": "failed", "error": "Query cannot be empty."},
        )

    logger.info(f"NL2SQL query: {user_query[:100]}...")

    agent = _get_nl2sql_agent()

    # Use cached FK/PK if available
    fk_relationships = _fk_cache.get("confirmed_fks", [])
    true_pks = _fk_cache.get("true_pks", {})

    result = agent.process_query(
        user_query=user_query,
        schema=schema,
        fk_relationships=fk_relationships if fk_relationships else None,
        true_pks=true_pks if true_pks else None,
    )

    import json
    from agents.nl2sql_agent import CustomJSONEncoder

    return JSONResponse(
        content=json.loads(json.dumps(result, cls=CustomJSONEncoder))
    )


@app.post("/api/refresh-metadata")
async def api_refresh_metadata(request: Request):
    """Refresh the cached metadata for a schema."""
    body = await request.json()
    schema = body.get("schema", "shopify")
    metadata = _get_metadata(schema, force_refresh=True)
    return {"status": "refreshed", "schema": schema, "preview": metadata[:500]}


@app.get("/api/data-tables/{schema}")
async def api_list_data_tables(schema: str = "shopify"):
    """List tables with column counts and row counts — used by Data Sources panel."""
    try:
        with DatabaseManager.get_connection() as conn:
            cur = conn.cursor()
            # Get table names and column counts
            cur.execute("""
                SELECT
                    t.table_name,
                    COALESCE(obj_description(
                        (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
                    ), '') as description,
                    (SELECT count(*)
                     FROM information_schema.columns c
                     WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name
                    ) as column_count
                FROM information_schema.tables t
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name;
            """, (schema,))
            columns = [desc[0] for desc in cur.description]
            tables = [dict(zip(columns, row)) for row in cur.fetchall()]

            # Get actual row counts (reliable, unlike reltuples)
            for tbl in tables:
                try:
                    cur.execute(f"SELECT count(*) FROM {schema}.{tbl['table_name']}")
                    tbl["row_count"] = cur.fetchone()[0]
                except Exception:
                    tbl["row_count"] = -1

            cur.close()
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        tables = []
    return {"schema": schema, "tables": tables}


@app.delete("/api/data-tables/{schema}/{table_name}")
async def api_delete_data_table(schema: str, table_name: str):
    """Delete a table from the database."""
    try:
        with DatabaseManager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table_name)
            )
            if not cur.fetchone():
                cur.close()
                return {"status": "error", "message": f"Table {table_name} not found in schema {schema}"}
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
            cur.close()
        _get_metadata(schema, force_refresh=True)
        return {"status": "success", "message": f"Table {schema}.{table_name} deleted."}
    except Exception as e:
        logger.error(f"Delete table failed: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/api/upload-csv")
async def api_upload_file(file: UploadFile):
    """
    Upload CSV or Excel file and import directly into the database.
    Returns {status: "success", message: "..."} as the frontend expects.
    """
    import pandas as pd
    import io

    filename = file.filename or "unknown"
    lower_name = filename.lower()
    schema = "shopify"

    logger.info(f"File upload: {filename}")

    # Read file into pandas DataFrame
    content = await file.read()
    if len(content) == 0:
        return JSONResponse(status_code=400, content={"status": "error", "error": "File is empty."})

    try:
        if lower_name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        elif lower_name.endswith((".xlsx", ".xls")):
            # For Excel, read first sheet by default
            xls = pd.ExcelFile(io.BytesIO(content))
            sheet_names = xls.sheet_names
            if len(sheet_names) == 1:
                df = pd.read_excel(xls, sheet_name=sheet_names[0])
            else:
                # Import all sheets as separate tables
                results = []
                for sheet in sheet_names:
                    sheet_df = pd.read_excel(xls, sheet_name=sheet)
                    tbl = sheet.strip().replace(" ", "_").lower()
                    sheet_df.columns = [str(c).strip().replace(" ", "_").lower() for c in sheet_df.columns]
                    _import_dataframe_to_db(sheet_df, tbl, schema)
                    results.append(tbl)

                # Auto-run FK analysis after multi-sheet import
                await _run_fk_analysis()
                _get_metadata(schema, force_refresh=True)
                return {
                    "status": "success",
                    "message": f"Imported {len(results)} sheets: {', '.join(results)}",
                    "tables": results,
                }
        else:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "error": "Only CSV and Excel (.xlsx/.xls) files are supported."}
            )

        # Single table import (CSV or single-sheet Excel)
        table_name = filename.rsplit(".", 1)[0].strip().replace(" ", "_").lower()
        df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]
        _import_dataframe_to_db(df, table_name, schema)

        # Auto-run FK analysis after single-table import
        await _run_fk_analysis()
        _get_metadata(schema, force_refresh=True)
        return {
            "status": "success",
            "message": f"Imported '{table_name}' ({len(df)} rows, {len(df.columns)} columns)",
            "table_name": f"{schema}.{table_name}",
        }

    except Exception as e:
        logger.error(f"File import failed: {e}")
        return JSONResponse(status_code=400, content={"status": "error", "error": str(e)})


def _import_dataframe_to_db(df, table_name: str, schema: str = "shopify"):
    """Import a pandas DataFrame into a PostgreSQL table using psycopg2."""
    # Infer SQL types
    type_map = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT",
    }

    columns_sql = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = type_map.get(dtype, "TEXT")
        columns_sql.append(f'"{col}" {sql_type}')

    import pandas as pd

    with DatabaseManager.get_connection() as conn:
        cur = conn.cursor()
        try:
            # Drop and recreate
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")

            create_sql = f"CREATE TABLE {schema}.{table_name} ({', '.join(columns_sql)});"
            cur.execute(create_sql)

            # Insert data in batch
            if not df.empty:
                cols = ', '.join([f'"{c}"' for c in df.columns])
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f"INSERT INTO {schema}.{table_name} ({cols}) VALUES ({placeholders})"

                # Convert NaN to None for psycopg2
                data = df.where(pd.notnull(df), None).values.tolist()
                cur.executemany(insert_sql, data)

            logger.info(f"Imported {len(df)} rows into {schema}.{table_name}")
        finally:
            cur.close()


# ─── Legacy endpoints (still available via curl/API) ────


@app.post("/query")
async def nl2sql_query(request: Request):
    """Legacy endpoint — redirects to /api/query."""
    return await api_nl2sql_query(request)


@app.get("/tables")
async def list_tables_in_schema(schema: str = "shopify"):
    """List all tables in the given schema."""
    agent = _get_nl2sql_agent()
    tables = agent.db_service.get_all_tables(schema)
    return {"schema": schema, "tables": tables}


@app.get("/health")
async def health_check():
    """Legacy health check."""
    return await api_health_check()


@app.get("/session/{session_id}")
async def get_session_info(session_id: str):
    """Get session information including parsed sheets."""
    logger.info(f"Getting session info: {session_id}")

    try:
        session = ExcelService.get_session(session_id)
        return {
            "session_id": session_id,
            "original_sheets": session["original_sheets"],
            "analysis": {
                sheet: {
                    "summary": analysis["summary"],
                    "issues": analysis["issues"],
                    "pk_candidates": analysis["pk_candidates"],
                    "fk_candidates": analysis["fk_candidates"]
                }
                for sheet, analysis in session.get("analysis", {}).items()
            }
        }
    except Exception as e:
        logger.error(f"Session not found: {e}")
        raise HTTPException(404, str(e))