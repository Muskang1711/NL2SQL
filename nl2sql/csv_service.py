import csv
import io
import os
import logging
import re
from datetime import datetime

import psycopg2
import psycopg2.extras
from config import Config

logger = logging.getLogger("nl2sql.csv")


class CSVService:

    def __init__(self):
        self.conn_string = Config.get_db_connection_string()

    def _get_connection(self):
        return psycopg2.connect(self.conn_string)

    def _sanitize_name(self, name: str) -> str:
        name = name.strip().lower()
        name = re.sub(r'[^a-z0-9_]', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        if name[0].isdigit():
            name = '_' + name
        # Truncate to 63 chars (PG limit)
        return name[:63]

    def _detect_column_type(self, values: list[str]) -> str:

        # Filter out empty values
        non_empty = [v.strip() for v in values if v and v.strip()]
        if not non_empty:
            return "TEXT"

        # Check if all values are integers
        all_int = True
        all_numeric = True
        all_date = True

        for val in non_empty:
            # Integer check
            try:
                int(val)
            except (ValueError, OverflowError):
                all_int = False

            # Numeric check
            try:
                float(val)
            except ValueError:
                all_numeric = False

            # Date check
            if all_date:
                date_matched = False
                for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y'):
                    try:
                        datetime.strptime(val, fmt)
                        date_matched = True
                        break
                    except ValueError:
                        continue
                if not date_matched:
                    all_date = False

        if all_int:
            # Check if values are too large for INTEGER
            max_val = max(abs(int(v)) for v in non_empty)
            if max_val > 2147483647:
                return "BIGINT"
            return "INTEGER"

        if all_numeric:
            return "DECIMAL(18,4)"

        if all_date:
            return "DATE"

        # Check max length for TEXT vs VARCHAR
        max_len = max(len(v) for v in non_empty)
        if max_len <= 255:
            return "VARCHAR(255)"

        return "TEXT"

    def import_csv_from_path(self, file_path: str, table_name: str = None, schema: str = "shopify") -> dict:
        if not os.path.exists(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}

        with open(file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()

        if not table_name:
            # Derive table name from filename
            base = os.path.splitext(os.path.basename(file_path))[0]
            table_name = self._sanitize_name(base)

        return self._import_csv_content(content, table_name, schema, os.path.basename(file_path))

    def import_csv_from_upload(self, file_content: bytes, filename: str, table_name: str = None, schema: str = "shopify") -> dict:
        try:
            content = file_content.decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                content = file_content.decode('latin-1')
            except Exception:
                return {"status": "error", "message": "Could not decode file. Please ensure it's UTF-8 or Latin-1 encoded."}

        if not table_name:
            base = os.path.splitext(filename)[0]
            table_name = self._sanitize_name(base)

        return self._import_csv_content(content, table_name, schema, filename)

    def _import_csv_content(self, content: str, table_name: str, schema: str, source_filename: str) -> dict:
        try:
            # Parse CSV
            reader = csv.reader(io.StringIO(content))
            headers_raw = next(reader)
            headers = [self._sanitize_name(h) for h in headers_raw]

            # Check for duplicate column names
            seen = {}
            unique_headers = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    unique_headers.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    unique_headers.append(h)
            headers = unique_headers

            # Read all rows
            rows = list(reader)
            if not rows:
                return {"status": "error", "message": "CSV file has headers but no data rows."}

            # Detect column types from first 500 rows (more rows = better accuracy)
            sample_size = min(500, len(rows))
            column_types = []
            for col_idx in range(len(headers)):
                sample_values = [rows[i][col_idx] for i in range(sample_size) if col_idx < len(rows[i])]
                col_type = self._detect_column_type(sample_values)
                column_types.append(col_type)

            logger.info(f"Detected types: {dict(zip(headers, column_types))}")

            # Build CREATE TABLE
            conn = self._get_connection()
            cur = conn.cursor()

            # Drop if exists
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")

            columns_def = ",\n    ".join(
                f"{headers[i]} {column_types[i]}" for i in range(len(headers))
            )
            create_sql = f"""
                CREATE TABLE {schema}.{table_name} (
                    {columns_def}
                );
            """
            logger.info(f"Creating table: {schema}.{table_name} with {len(headers)} columns")
            cur.execute(create_sql)

            # Add table comment
            cur.execute(
                f"COMMENT ON TABLE {schema}.{table_name} IS %s;",
                (f"Imported from CSV: {source_filename} ({len(rows)} rows, {len(headers)} columns)",)
            )

            # Add column comments with original header names
            for i, (orig, clean) in enumerate(zip(headers_raw, headers)):
                if orig.strip().lower() != clean:
                    cur.execute(
                        f"COMMENT ON COLUMN {schema}.{table_name}.{clean} IS %s;",
                        (f"Original column: {orig.strip()}",)
                    )

            # Insert data using COPY for performance
            buffer = io.StringIO()
            
            null_marker = '\\N'
            for row in rows:
                # Pad or trim row to match headers
                padded = row[:len(headers)]
                while len(padded) < len(headers):
                    padded.append('')
                # Replace empty strings with NULL marker
                processed = []
                for val in padded:
                    if val.strip() == '':
                        processed.append(null_marker)
                    else:
                        # Escape tabs and backslashes
                        processed.append(val.replace('\\', '\\\\').replace('\t', '\\t'))
                buffer.write('\t'.join(processed) + '\n')

            buffer.seek(0)

            try:
                cur.copy_from(buffer, f"{schema}.{table_name}", sep='\t', null=null_marker, columns=headers)
                imported_count = len(rows)
            except Exception as copy_err:
                logger.warning(f"COPY failed, falling back to INSERT: {copy_err}")
                conn.rollback()
                
                # Recreate table with SAME detected types (not TEXT fallback)
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
                cur.execute(create_sql)
                cur.execute(
                    f"COMMENT ON TABLE {schema}.{table_name} IS %s;",
                    (f"Imported from CSV: {source_filename} ({len(rows)} rows, {len(headers)} columns)",)
                )

                # Insert row by row with type casting
                placeholders = ','.join(['%s'] * len(headers))
                insert_sql = f"INSERT INTO {schema}.{table_name} ({','.join(headers)}) VALUES ({placeholders})"
                
                imported_count = 0
                errors = 0
                for row in rows:
                    padded = row[:len(headers)]
                    while len(padded) < len(headers):
                        padded.append(None)
                    values = [v if v and v.strip() else None for v in padded]
                    try:
                        cur.execute(insert_sql, values)
                        imported_count += 1
                    except Exception as row_err:
                        errors += 1
                        conn.rollback()
                        # Recreate and try continuing
                        if errors > 100:
                            logger.error(f"Too many insert errors ({errors}), aborting")
                            break
                        continue

            conn.commit()

            # Run ANALYZE for accurate row counts
            cur.execute(f"ANALYZE {schema}.{table_name};")
            conn.commit()

            cur.close()
            conn.close()

            logger.info(f"Successfully imported {imported_count} rows into {schema}.{table_name}")

            return {
                "status": "success",
                "table_name": f"{schema}.{table_name}",
                "columns": [{"name": headers[i], "type": column_types[i], "original": headers_raw[i].strip()} for i in range(len(headers))],
                "rows_imported": imported_count,
                "total_rows": len(rows),
                "message": f"Successfully imported {imported_count} rows into {schema}.{table_name}",
            }

        except Exception as e:
            logger.error(f"CSV import error: {e}")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            return {"status": "error", "message": str(e)}

    def list_imported_tables(self, schema: str = "shopify") -> list[dict]:

        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT 
                    t.table_name,
                    COALESCE(obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass), '') as description,
                    (SELECT count(*) FROM information_schema.columns c WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count,
                    (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as row_count
                FROM information_schema.tables t
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name;
            """, (schema,))
            tables = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return tables
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def delete_table(self, table_name: str, schema: str = "shopify") -> dict:

        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
            conn.commit()
            cur.close()
            conn.close()
            return {"status": "success", "message": f"Table {schema}.{table_name} deleted."}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def enrich_column_descriptions(self, table_name: str, schema: str, llm_service) -> bool:

        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get column names and types
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = [dict(r) for r in cur.fetchall()]
            
            if not columns:
                return False

            # Get 3 sample rows for context
            cur_sample = conn.cursor()
            cur_sample.execute(f"SELECT * FROM {schema}.{table_name} LIMIT 3")
            sample_rows = cur_sample.fetchall()
            col_names = [desc[0] for desc in cur_sample.description]
            cur_sample.close()
            
            # Format sample data
            sample_text = ""
            for row in sample_rows:
                pairs = [f"{col_names[i]}={row[i]}" for i in range(len(col_names))]
                sample_text += "  " + ", ".join(pairs) + "\n"

            # Build the prompt for the LLM
            columns_list = "\n".join(
                f"  - {c['column_name']} ({c['data_type']})" for c in columns
            )
            
            prompt = f"""Analyze these database columns and generate a brief description for EACH column. 
Include synonyms, aliases, and related business terms that a user might use to refer to this column.

TABLE: {schema}.{table_name}
COLUMNS:
{columns_list}

SAMPLE DATA:
{sample_text}

For EACH column, provide a JSON object with this exact format (no markdown, no code blocks, just raw JSON array):
[
  {{"column": "column_name", "description": "Brief description. Synonyms: term1, term2, term3"}},
  ...
]

RULES:
- Keep descriptions SHORT (under 100 chars each)
- Include 3-5 relevant synonyms/aliases for each column
- For abbreviations (ctr, cpc, cpm, roas, atc), ALWAYS expand them
- Map business concepts: revenue→roas, cost→spend, views→impressions, etc.  
- Include both English and Hindi business terms if applicable
- Return ONLY the JSON array, no other text"""

            system_prompt = "You are a data analyst. Return ONLY valid JSON array, no explanations."
            
            response_text = llm_service._call_llm(system_prompt, prompt)
            
            # Parse the JSON response
            import json
            # Clean response - remove code blocks if any
            clean = response_text.strip()
            if clean.startswith("```"):
                clean = re.sub(r'```\w*\n?', '', clean).strip()
            
            descriptions = json.loads(clean)
            
            # Apply descriptions as PostgreSQL comments
            applied = 0
            cur2 = conn.cursor()
            for desc_item in descriptions:
                col_name = desc_item.get("column", "")
                description = desc_item.get("description", "")
                if col_name and description:
                    try:
                        cur2.execute(
                            f"COMMENT ON COLUMN {schema}.{table_name}.{col_name} IS %s;",
                            (description,)
                        )
                        applied += 1
                    except Exception as col_err:
                        logger.warning(f"Could not set comment for {col_name}: {col_err}")
                        conn.rollback()
            
            conn.commit()
            cur2.close()
            cur.close()
            conn.close()
            
            logger.info(f"Enriched {applied}/{len(columns)} column descriptions for {schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.warning(f"Column enrichment failed (non-critical): {e}")
            try:
                conn.close()
            except Exception:
                pass
            return False
