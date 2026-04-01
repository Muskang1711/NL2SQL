"""
Cardinality Agent - Detects TRUE Primary Keys per table.

Algorithm:
1. Query pg_constraint to find the ACTUAL PRIMARY KEY constraint for each table.
2. If no constraint exists (e.g., data was loaded without PKs), use heuristic:
   - First NOT NULL + fully-unique column, preferring columns named 'id' or '<table>_id'.
3. Returns exactly ONE true PK per table (not a list of candidates).

This is the foundation for FK detection — FKs must reference a TRUE PK.
"""
from config import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class CardinalityAgent:

    @staticmethod
    async def find_true_pks(mcp_client, tables_metadata: dict) -> dict:
        """
        Detect the TRUE primary key for each table.

        Returns:
            {"table_name": "pk_column_name", ...}
            Value is None if no PK could be determined.
        """
        settings = get_settings()
        schema = settings.db_schema
        true_pks = {}

        logger.info(f"Detecting true PKs for {len(tables_metadata)} tables")

        for table_name in tables_metadata:
            # Strategy 1: Query the actual PK constraint from database
            pk_col = await _query_pk_constraint(mcp_client, schema, table_name)
            if pk_col:
                true_pks[table_name] = pk_col
                logger.info(f"PK (constraint): {table_name}.{pk_col}")
                continue

            # Strategy 2: Heuristic - find single unique NOT NULL column
            pk_col = await _heuristic_pk(
                mcp_client, schema, table_name, tables_metadata[table_name]
            )
            if pk_col:
                true_pks[table_name] = pk_col
                logger.info(f"PK (heuristic): {table_name}.{pk_col}")
            else:
                true_pks[table_name] = None
                logger.warning(f"No PK found for table: {table_name}")

        logger.info(f"True PKs detected: {true_pks}")
        return true_pks


async def _query_pk_constraint(mcp_client, schema: str, table_name: str) -> str:
    """Query the actual PRIMARY KEY constraint from pg_constraint."""
    sql = (
        f"SELECT kcu.column_name "
        f"FROM information_schema.table_constraints tc "
        f"JOIN information_schema.key_column_usage kcu "
        f"  ON tc.constraint_name = kcu.constraint_name "
        f"  AND tc.table_schema = kcu.table_schema "
        f"WHERE tc.table_schema = '{schema}' "
        f"  AND tc.table_name = '{table_name}' "
        f"  AND tc.constraint_type = 'PRIMARY KEY' "
        f"LIMIT 1"
    )
    try:
        result = await mcp_client.run_query(sql)
        if result and len(result) > 0:
            row = result[0]
            if isinstance(row, dict):
                return row.get("column_name")
    except Exception as e:
        logger.warning(f"PK constraint query failed for {table_name}: {e}")
    return None


async def _heuristic_pk(mcp_client, schema: str, table_name: str, meta: dict) -> str:
    """
    Heuristic PK detection when no constraint exists.
    Priority:
    1. NOT NULL column named exactly 'id'
    2. NOT NULL column named '<table_singular>_id' (e.g., order_id for orders)
    3. First NOT NULL column ending in '_id' that is fully unique
    """
    columns = meta.get("columns", [])
    
    # Build candidate list ordered by priority
    candidates = []
    table_lower = table_name.lower()
    
    for col in columns:
        col_name = col["column_name"].lower()
        is_not_null = col.get("is_nullable", "YES") == "NO"
        
        if not is_not_null:
            continue
            
        # Priority scoring
        if col_name == "id":
            candidates.insert(0, col["column_name"])  # highest priority
        elif col_name == f"{_singularize(table_lower)}_id":
            candidates.insert(min(1, len(candidates)), col["column_name"])
        elif col_name.endswith("_id"):
            candidates.append(col["column_name"])

    # Verify uniqueness for top candidates
    for candidate in candidates[:3]:  # check at most 3
        sql = (
            f'SELECT (COUNT(DISTINCT "{candidate}") = COUNT(*)) AS is_unique '
            f'FROM "{schema}"."{table_name}"'
        )
        try:
            result = await mcp_client.run_query(sql)
            if result and len(result) > 0:
                row = result[0]
                is_unique = row.get("is_unique", False) if isinstance(row, dict) else False
                if isinstance(is_unique, str):
                    is_unique = is_unique.lower() in ("true", "t", "1")
                if is_unique:
                    return candidate
        except Exception as e:
            logger.warning(f"Uniqueness check failed for {table_name}.{candidate}: {e}")

    return None


def _singularize(name: str) -> str:
    """Very basic singularization for table names."""
    # Remove common prefixes
    prefixes = ["shopify_", "app_", "public_"]
    clean = name
    for prefix in prefixes:
        if clean.startswith(prefix):
            clean = clean[len(prefix):]
            break
    
    # Basic singular rules
    if clean.endswith("ies"):
        return clean[:-3] + "y"
    if clean.endswith("ses"):
        return clean[:-2]
    if clean.endswith("s") and not clean.endswith("ss"):
        return clean[:-1]
    return clean
