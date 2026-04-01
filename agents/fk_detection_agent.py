from config import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class FKDetectionAgent:
    """
    Confirms foreign key relationships using two-phase verification:
    
    Phase 1 - Jaccard Similarity (Quick Check):
        Compare top 100 unique values from both columns.
        If zero overlap, discard immediately.
    
    Phase 2 - Left Anti-Join (Referential Integrity):
        SELECT COUNT(*) FROM schema.table_b
        WHERE fk_col IS NOT NULL
        AND fk_col NOT IN (SELECT pk_col FROM schema.table_a)
        
        Result = 0: Perfect FK (100% match)
        Result > 0: Report orphan count and match percentage
    """

    @staticmethod
    async def verify_fk_pairs(mcp_client, filtered_pairs: list) -> list:
        """
        Run Jaccard similarity and referential integrity checks on filtered FK pairs.

        Args:
            mcp_client: MCPClient instance
            filtered_pairs: list of {"fk_table", "fk_column", "ref_table", "ref_column"}

        Returns:
            list of confirmed FK relationships with match details
        """
        settings = get_settings()
        schema = settings.db_schema
        sample_size = settings.jaccard_sample_size
        threshold = settings.fk_match_threshold
        confirmed = []

        for pair in filtered_pairs:
            fk_table = pair["fk_table"]
            fk_column = pair["fk_column"]
            ref_table = pair["ref_table"]
            ref_column = pair["ref_column"]

            logger.info(
                f"Verifying FK: {fk_table}.{fk_column} -> {ref_table}.{ref_column}"
            )

            # Phase 1: Jaccard Similarity (top N unique values)
            jaccard_pass = await _jaccard_check(
                mcp_client, schema, fk_table, fk_column, ref_table, ref_column, sample_size
            )
            if not jaccard_pass:
                logger.info(
                    f"Jaccard check failed for {fk_table}.{fk_column} -> "
                    f"{ref_table}.{ref_column}. Skipping."
                )
                continue

            # Phase 2: Referential Integrity (Left Anti-Join)
            result = await _referential_integrity_check(
                mcp_client, schema, fk_table, fk_column, ref_table, ref_column
            )

            if result is None:
                continue

            orphan_count = result["orphan_count"]
            total_fk_rows = result["total_fk_rows"]

            if total_fk_rows == 0:
                logger.info(f"No non-null rows in {fk_table}.{fk_column}. Skipping.")
                continue

            match_pct = ((total_fk_rows - orphan_count) / total_fk_rows) * 100

            status = "confirmed" if match_pct >= (threshold * 100) else "partial"
            if match_pct < 50:
                status = "rejected"

            entry = {
                "fk_table": fk_table,
                "fk_column": fk_column,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "match_percentage": round(match_pct, 2),
                "orphan_count": orphan_count,
                "total_fk_rows": total_fk_rows,
                "status": status
            }
            confirmed.append(entry)
            logger.info(
                f"FK result: {fk_table}.{fk_column} -> {ref_table}.{ref_column} "
                f"| match={match_pct:.2f}% | orphans={orphan_count} | status={status}"
            )

        logger.info(f"FK detection complete. {len(confirmed)} relationships verified")
        return confirmed


async def _jaccard_check(
    mcp_client, schema: str, fk_table: str, fk_column: str,
    ref_table: str, ref_column: str, sample_size: int
) -> bool:
    """
    Quick overlap check using top N unique values from both columns.
    Returns True if there is any overlap, False otherwise.
    """
    sql = (
        f'SELECT COUNT(*) AS overlap FROM ('
        f'  SELECT DISTINCT "{fk_column}" AS val FROM "{schema}"."{fk_table}" '
        f'  WHERE "{fk_column}" IS NOT NULL LIMIT {sample_size}'
        f') fk_vals '
        f'INNER JOIN ('
        f'  SELECT DISTINCT "{ref_column}" AS val FROM "{schema}"."{ref_table}" '
        f'  WHERE "{ref_column}" IS NOT NULL LIMIT {sample_size}'
        f') ref_vals ON fk_vals.val::text = ref_vals.val::text'
    )

    try:
        result = await mcp_client.run_query(sql)
        if result and len(result) > 0:
            row = result[0]
            overlap = 0
            if isinstance(row, dict):
                overlap = row.get("overlap", 0)
            elif isinstance(row, list) and len(row) > 0:
                overlap = row[0]

            if isinstance(overlap, str):
                overlap = int(overlap)

            return overlap > 0
    except Exception as e:
        logger.warning(f"Jaccard check query failed: {e}")
        # On failure, let it pass to the next phase rather than discarding
        return True

    return False


async def _referential_integrity_check(
    mcp_client, schema: str, fk_table: str, fk_column: str,
    ref_table: str, ref_column: str
) -> dict:
    """
    Left Anti-Join check.
    Returns {"orphan_count": int, "total_fk_rows": int} or None on failure.
    """
    sql = (
        f'SELECT '
        f'(SELECT COUNT(*) FROM "{schema}"."{fk_table}" WHERE "{fk_column}" IS NOT NULL '
        f' AND "{fk_column}"::text NOT IN (SELECT "{ref_column}"::text FROM "{schema}"."{ref_table}")) AS orphan_count, '
        f'(SELECT COUNT(*) FROM "{schema}"."{fk_table}" WHERE "{fk_column}" IS NOT NULL) AS total_fk_rows'
    )

    try:
        result = await mcp_client.run_query(sql)
        if result and len(result) > 0:
            row = result[0]
            if isinstance(row, dict):
                orphan = int(row.get("orphan_count", 0))
                total = int(row.get("total_fk_rows", 0))
            elif isinstance(row, list) and len(row) >= 2:
                orphan = int(row[0])
                total = int(row[1])
            else:
                return None
            return {"orphan_count": orphan, "total_fk_rows": total}
    except Exception as e:
        logger.warning(f"Referential integrity check failed: {e}")

    return None
