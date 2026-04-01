"""
Verification Agent — Generates ALTER TABLE FK statements and human-readable summaries.
"""
from config import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class VerificationAgent:

    @staticmethod
    def generate_results(
        confirmed_fks: list,
        true_pks: dict,
        tables_metadata: dict
    ) -> dict:
        """
        Generate ALTER TABLE statements and summary from confirmed FKs.

        Args:
            confirmed_fks: list of FK relationship dicts from FKDetectionAgent
            true_pks: {"table_name": "pk_column_name"}  (single PK per table)
            tables_metadata: full table metadata

        Returns:
            {
                "alter_statements": ["ALTER TABLE ...", ...],
                "summary": [...],
                "true_pks": {"table": "pk_col"},
                "confirmed_fks": [...],
                "rejected_fks": [...]
            }
        """
        settings = get_settings()
        schema = settings.db_schema
        logger.info("Generating verification results")

        alter_statements = []
        summary_lines = []
        confirmed = []
        rejected = []

        for fk in confirmed_fks:
            fk_table = fk["fk_table"]
            fk_column = fk["fk_column"]
            ref_table = fk["ref_table"]
            ref_column = fk["ref_column"]
            match_pct = fk["match_percentage"]
            orphan_count = fk["orphan_count"]
            total_rows = fk.get("total_fk_rows", 0)
            status = fk["status"]

            if status == "confirmed":
                constraint_name = f"fk_{fk_table}_{fk_column}_{ref_table}"
                # Truncate constraint name if too long (Postgres limit is 63 chars)
                if len(constraint_name) > 63:
                    constraint_name = constraint_name[:63]

                alter_sql = (
                    f'ALTER TABLE "{schema}"."{fk_table}" '
                    f'ADD CONSTRAINT "{constraint_name}" '
                    f'FOREIGN KEY ("{fk_column}") '
                    f'REFERENCES "{schema}"."{ref_table}" ("{ref_column}");'
                )
                alter_statements.append(alter_sql)
                confirmed.append(fk)

                summary_lines.append(
                    f"{fk_table}.{fk_column} → {ref_table}.{ref_column} "
                    f"[CONFIRMED] {match_pct}% match, {total_rows} rows, "
                    f"0 orphans"
                )
            elif status == "partial":
                confirmed.append(fk)
                summary_lines.append(
                    f"{fk_table}.{fk_column} → {ref_table}.{ref_column} "
                    f"[PARTIAL] {match_pct}% match, {orphan_count} orphans "
                    f"out of {total_rows} rows. Data cleaning needed."
                )
            else:
                rejected.append(fk)
                summary_lines.append(
                    f"{fk_table}.{fk_column} → {ref_table}.{ref_column} "
                    f"[REJECTED] {match_pct}% match, {orphan_count} orphans."
                )

        result = {
            "alter_statements": alter_statements,
            "summary": summary_lines,
            "true_pks": true_pks,
            "confirmed_fks": confirmed,
            "rejected_fks": rejected
        }

        logger.info(
            f"Verification complete: {len(alter_statements)} ALTER statements, "
            f"{len(confirmed)} confirmed, {len(rejected)} rejected"
        )
        return result
