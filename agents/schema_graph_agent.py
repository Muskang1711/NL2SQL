from utils.logger import setup_logger

logger = setup_logger(__name__)


class SchemaGraphAgent:

    @staticmethod
    def build_candidates(tables_metadata: dict, true_pks: dict) -> list:
        """
        Generate FK candidates based on true PKs.

        Args:
            tables_metadata: {"table_name": {"columns": [...]}}
            true_pks: {"table_name": "pk_column_name"}

        Returns:
            list of {"fk_table", "fk_column", "ref_table", "ref_column"}
        """
        logger.info(f"Building FK candidates for {len(tables_metadata)} tables")

        # Build reverse index: pk_column_name → [table_name, ...]
        pk_reverse = {}
        for table_name, pk_col in true_pks.items():
            if pk_col is None:
                continue
            if pk_col not in pk_reverse:
                pk_reverse[pk_col] = []
            pk_reverse[pk_col].append(table_name)

        candidates = []
        seen = set()  # Avoid duplicates

        for table_name, meta in tables_metadata.items():
            table_pk = true_pks.get(table_name)
            columns = meta.get("columns", [])

            for col in columns:
                col_name = col["column_name"]
                col_name_lower = col_name.lower()

                # Skip if this column IS the table's own PK
                if table_pk and col_name_lower == table_pk.lower():
                    continue

                # Skip bare 'id' — it's ALWAYS a surrogate PK for its own table,
                # never an FK to another table's 'id'. Only descriptive _id columns
                # like 'order_id', 'product_id' are FK candidates.
                if col_name_lower == "id":
                    continue

                # Strategy 1: Exact match — column name matches a PK name
                #  e.g., order_line_items.order_id → orders.order_id (PK)
                if col_name_lower in pk_reverse:
                    for ref_table in pk_reverse[col_name_lower]:
                        if ref_table == table_name:
                            continue
                        key = (table_name, col_name, ref_table)
                        if key not in seen:
                            seen.add(key)
                            candidates.append({
                                "fk_table": table_name,
                                "fk_column": col_name,
                                "ref_table": ref_table,
                                "ref_column": true_pks[ref_table]
                            })

                # Strategy 2: Suffix match — customer_id → customers.id
                #  e.g., orders.customer_id → customers table with PK 'id'
                if col_name_lower.endswith("_id") and col_name_lower != "id":
                    prefix = col_name_lower[:-3]  # "customer"
                    if not prefix:
                        continue

                    for ref_table, ref_pk in true_pks.items():
                        if ref_table == table_name or ref_pk is None:
                            continue
                        ref_pk_lower = ref_pk.lower()
                        ref_table_lower = ref_table.lower()

                        # Only match if ref table PK is 'id' (bare surrogate)
                        # AND ref table name matches the prefix
                        if ref_pk_lower != "id":
                            continue

                        # Match: prefix == table name (singular or plural forms)
                        if ref_table_lower in _name_variants(prefix):
                            key = (table_name, col_name, ref_table)
                            if key not in seen:
                                seen.add(key)
                                candidates.append({
                                    "fk_table": table_name,
                                    "fk_column": col_name,
                                    "ref_table": ref_table,
                                    "ref_column": ref_pk
                                })

        logger.info(f"Generated {len(candidates)} FK candidates")
        for c in candidates:
            logger.info(
                f"  Candidate: {c['fk_table']}.{c['fk_column']} → "
                f"{c['ref_table']}.{c['ref_column']}"
            )
        return candidates


def _name_variants(prefix: str) -> set:
    """Generate possible table name variants from a column prefix."""
    variants = {
        prefix,
        prefix + "s",
        prefix + "es",
    }
    # Handle 'y' → 'ies' (e.g., category → categories)
    if prefix.endswith("y"):
        variants.add(prefix[:-1] + "ies")
    # Handle common prefixed table names
    common_prefixes = ["shopify_", "app_", "public_"]
    expanded = set()
    for v in variants:
        for p in common_prefixes:
            expanded.add(p + v)
    variants.update(expanded)
    return variants
