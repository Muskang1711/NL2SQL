import logging
import psycopg2
import psycopg2.extras
from config import Config

logger = logging.getLogger("nl2sql.db")


class DatabaseService:

    def __init__(self):
        self.conn_string = Config.get_db_connection_string()

    def _get_connection(self):
        return psycopg2.connect(self.conn_string)

    def test_connection(self) -> dict:

        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            cur.execute("SELECT current_database();")
            db_name = cur.fetchone()[0]
            cur.close()
            conn.close()
            return {"status": "connected", "version": version, "database": db_name}
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return {"status": "error", "message": str(e)}


    # METADATA RETRIEVAL (RAG Context)

    def get_all_schemas(self) -> list[str]:

        query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name;
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(query)
            schemas = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return schemas
        except Exception as e:
            logger.error(f"Error fetching schemas: {e}")
            return []

    def get_all_tables(self, schema: str = "shopify") -> list[dict]:

        query = """
            SELECT 
                t.table_name,
                t.table_type,
                COALESCE(
                    obj_description(
                        (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
                    ), 
                    ''
                ) as table_comment,
                (SELECT reltuples::bigint 
                 FROM pg_class 
                 WHERE oid = (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
                ) as estimated_rows
            FROM information_schema.tables t
            WHERE t.table_schema = %s
              AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name;
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, (schema,))
            tables = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return tables
        except Exception as e:
            logger.error(f"Error fetching tables: {e}")
            return []

    def get_table_columns(self, table_name: str, schema: str = "shopify") -> list[dict]:

        query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.ordinal_position,
                COALESCE(
                    col_description(
                        (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass, 
                        c.ordinal_position
                    ), 
                    ''
                ) as column_comment
            FROM information_schema.columns c
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position;
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, (schema, table_name))
            columns = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return columns
        except Exception as e:
            logger.error(f"Error fetching columns for {table_name}: {e}")
            return []

    def get_table_constraints(self, table_name: str, schema: str = "shopify") -> list[dict]:
        query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY tc.constraint_type, kcu.ordinal_position;
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, (schema, table_name))
            constraints = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return constraints
        except Exception as e:
            logger.error(f"Error fetching constraints for {table_name}: {e}")
            return []

    def get_sample_data(self, table_name: str, schema: str = "shopify", limit: int = 3) -> list[dict]:
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                f"SELECT * FROM {psycopg2.extensions.quote_ident(schema, conn)}.{psycopg2.extensions.quote_ident(table_name, conn)} LIMIT %s;",
                (limit,)
            )
            rows = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"Error fetching sample data for {table_name}: {e}")
            return []

    def get_full_metadata(self, schema: str = "shopify") -> str:
  
        metadata_parts = []
        metadata_parts.append(f"DATABASE: {Config.DB_NAME}")
        metadata_parts.append(f"SCHEMA: {schema}")
        metadata_parts.append("=" * 60)

        tables = self.get_all_tables(schema)
        if not tables:
            return f"No tables found in schema '{schema}'."

        for table in tables:
            table_name = table["table_name"]
            table_comment = table.get("table_comment", "")
            estimated_rows = table.get("estimated_rows", 0)

            metadata_parts.append(f"\nTABLE: {schema}.{table_name}")
            if table_comment:
                metadata_parts.append(f"  Description: {table_comment}")
            metadata_parts.append(f"  Type: {table['table_type']}")
            metadata_parts.append(f"  Estimated Rows: {estimated_rows}")
            metadata_parts.append("  COLUMNS:")

            columns = self.get_table_columns(table_name, schema)
            for col in columns:
                nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                col_info = f"    - {col['column_name']}: {col['data_type']} ({nullable})"
                if col.get("column_comment"):
                    col_info += f" -- {col['column_comment']}"
                if col.get("column_default"):
                    col_info += f" [default: {col['column_default']}]"
                metadata_parts.append(col_info)

            # Constraints
            constraints = self.get_table_constraints(table_name, schema)
            if constraints:
                metadata_parts.append("  CONSTRAINTS:")
                for c in constraints:
                    if c["constraint_type"] == "PRIMARY KEY":
                        metadata_parts.append(f"    - PRIMARY KEY: {c['column_name']}")
                    elif c["constraint_type"] == "FOREIGN KEY":
                        metadata_parts.append(
                            f"    - FOREIGN KEY: {c['column_name']} → "
                            f"{c['foreign_table_name']}.{c['foreign_column_name']}"
                        )
                    elif c["constraint_type"] == "UNIQUE":
                        metadata_parts.append(f"    - UNIQUE: {c['column_name']}")

            # Sample data
            sample = self.get_sample_data(table_name, schema, limit=2)
            if sample:
                metadata_parts.append("  SAMPLE DATA (first 2 rows):")
                for i, row in enumerate(sample):
                    row_str = ", ".join(f"{k}={v}" for k, v in row.items())
                    metadata_parts.append(f"    Row {i+1}: {row_str}")

            metadata_parts.append("-" * 40)

        return "\n".join(metadata_parts)


    # SQL EXECUTION (replaces Amazon Athena)


    def execute_query(self, sql: str) -> dict:

        max_rows = Config.MAX_ROWS_RETURN

        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql)

            # Check if query returns data (SELECT, etc.)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchmany(max_rows)
                total_rows = cur.rowcount if cur.rowcount >= 0 else len(rows)
                data = [dict(row) for row in rows]

                cur.close()
                conn.close()

                return {
                    "status": "success",
                    "columns": columns,
                    "data": data,
                    "row_count": total_rows,
                    "truncated": total_rows > max_rows,
                }
            else:
                # DML or DDL statement
                affected = cur.rowcount
                conn.commit()
                cur.close()
                conn.close()

                return {
                    "status": "success",
                    "message": f"Query executed successfully. {affected} rows affected.",
                    "columns": [],
                    "data": [],
                    "row_count": affected,
                }

        except psycopg2.Error as e:
            logger.warning(f"SQL execution error: {e.pgerror or str(e)}")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass

            return {
                "status": "error",
                "error_code": e.pgcode or "UNKNOWN",
                "error_message": str(e.pgerror or e).strip(),
                "error_detail": str(e.diag.message_detail) if e.diag and e.diag.message_detail else None,
                "error_hint": str(e.diag.message_hint) if e.diag and e.diag.message_hint else None,
            }
        except Exception as e:
            logger.error(f"Unexpected error executing SQL: {e}")
            return {
                "status": "error",
                "error_code": "UNEXPECTED",
                "error_message": str(e),
            }

    def validate_sql_readonly(self, sql: str) -> dict:

        sql_upper = sql.strip().upper()
        dangerous_keywords = [
            "DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
            "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"
        ]

        for keyword in dangerous_keywords:
            # Check if keyword appears as a standalone word (not part of a column name)
            import re
            if re.search(rf'\b{keyword}\b', sql_upper):
                # Allow CREATE in CTEs with CREATE as part of data
                if keyword == "CREATE" and "WITH" in sql_upper and "AS" in sql_upper:
                    continue
                return {
                    "safe": False,
                    "reason": f"Query contains potentially dangerous keyword: {keyword}. "
                              f"Only SELECT queries are allowed."
                }

        if not sql_upper.lstrip().startswith(("SELECT", "WITH", "EXPLAIN")):
            return {
                "safe": False,
                "reason": "Query must start with SELECT, WITH, or EXPLAIN."
            }

        return {"safe": True}
