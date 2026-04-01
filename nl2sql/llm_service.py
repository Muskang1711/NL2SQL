import logging
import re
import time
from config import Config

logger = logging.getLogger("nl2sql.llm")

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 3


class LLMService:

    def __init__(self):
        self.provider = Config.get_llm_provider()
        self.model_name = "none"
        self._groq_client = None
        self._gemini_model = None

        if self.provider == "groq":
            self._init_groq()
        elif self.provider == "gemini":
            self._init_gemini()
        else:
            logger.warning("No LLM provider configured. Set GROQ_API_KEY or GEMINI_API_KEY in .env")

        self.max_attempts = Config.MAX_CORRECTION_ATTEMPTS

    def _init_groq(self):
        try:
            from groq import Groq
            self._groq_client = Groq(api_key=Config.GROQ_API_KEY)
            self.model_name = "llama-3.3-70b-versatile"
            self.provider = "groq"
            logger.info(f"LLM initialized: Groq ({self.model_name})")
        except Exception as e:
            logger.error(f"Failed to initialize Groq: {e}")
            # Fallback to Gemini if available
            if Config.GEMINI_API_KEY:
                logger.info("Falling back to Gemini...")
                self._init_gemini()

    def _init_gemini(self):
        try:
            import google.generativeai as genai
            genai.configure(api_key=Config.GEMINI_API_KEY)
            self._gemini_model = genai.GenerativeModel("gemini-2.0-flash")
            self.model_name = "gemini-2.0-flash"
            self.provider = "gemini"
            logger.info(f"LLM initialized: Gemini ({self.model_name})")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini: {e}")

    def _call_llm(self, system_prompt: str, user_prompt: str) -> str:

        last_error = None

        for retry in range(MAX_RETRIES):
            try:
                if self.provider == "groq":
                    return self._call_groq(system_prompt, user_prompt)
                elif self.provider == "gemini":
                    return self._call_gemini(system_prompt, user_prompt)
                else:
                    raise Exception(
                        "No LLM provider configured. "
                        "Please set GROQ_API_KEY or GEMINI_API_KEY in your .env file."
                    )

            except Exception as e:
                error_str = str(e)
                last_error = e

                # Check if rate limit error
                is_rate_limit = any(kw in error_str.lower() for kw in ["429", "rate", "quota", "limit"])

                if is_rate_limit and retry < MAX_RETRIES - 1:
                    wait_time = INITIAL_RETRY_DELAY * (2 ** retry)
                    logger.warning(
                        f"Rate limited ({self.provider}, attempt {retry + 1}/{MAX_RETRIES}). "
                        f"Waiting {wait_time}s..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    raise

        raise last_error

    def _call_groq(self, system_prompt: str, user_prompt: str) -> str:

        response = self._groq_client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=2048,
        )
        return response.choices[0].message.content

    def _call_gemini(self, system_prompt: str, user_prompt: str) -> str:

        import google.generativeai as genai
        response = self._gemini_model.generate_content(
            [system_prompt, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.1,
                max_output_tokens=2048,
            )
        )
        return response.text

    def get_info(self) -> dict:

        return {
            "provider": self.provider.capitalize() if self.provider != "none" else "Not configured",
            "model": self.model_name,
            "configured": self.provider != "none",
        }

    # ─── Prompt Building ─────────────────────────────

    def _build_system_prompt(self, metadata_context: str) -> str:

        return f"""You are an expert PostgreSQL SQL query generator. Your role is to convert natural language questions into accurate, syntactically correct PostgreSQL SQL queries.

## DATABASE METADATA (Use this to write accurate queries):
{metadata_context}

## CRITICAL RULES:
1. **READ COLUMN DESCRIPTIONS**: Each column has a description with synonyms and business terms. You MUST read these descriptions to correctly map user terms to column names. For example, if a user asks about "revenue", find the column whose description mentions "revenue" as a synonym.
2. **NEVER GUESS COLUMNS**: If the user asks about a concept that does NOT match any column name or its described synonyms, do NOT substitute a different column. Instead, return: ```sql SELECT 'Column not found in dataset: <user_term> does not match any available column' AS error ```
3. **PostgreSQL Syntax ONLY**: Generate queries that strictly comply with PostgreSQL syntax.
4. **Use Correct Table/Column Names**: ONLY use table and column names from the metadata above. Never invent table or column names.
5. **Schema Qualification**: Always use schema-qualified table names (e.g., shopify.table_name).
6. **Aliases for Joins**: When joining tables, always use table aliases. If column names are the same across tables, use alias.column_name in the SELECT statement.
7. **String Columns**: If a column is of type varchar/text/character varying, enclose comparison values in single quotes.
8. **Numeric Columns**: Do NOT enclose numeric comparison values in quotes.
9. **Date Handling**: For date columns being compared to strings, cast the string appropriately using ::date or TO_DATE().
10. **CTEs**: When writing CTEs (WITH clause), include ALL required columns in the CTE that will be used later.
11. **Type Casting**: When concatenating non-string columns with strings, cast them using ::text or CAST(column AS text).
12. **NULL Handling**: Use IS NULL / IS NOT NULL for null comparisons, not = NULL.
13. **LIMIT**: Always add LIMIT 100 at the end unless the user specifies a different limit.
14. **Case Sensitivity**: PostgreSQL identifiers are case-sensitive when quoted. Use lowercase for unquoted identifiers.
15. **ONLY SELECT**: Generate ONLY SELECT/WITH queries. Never generate INSERT, UPDATE, DELETE, DROP, or any data modification queries.
16. **FILTER NULLs IN RANKINGS**: When doing ORDER BY to find highest/lowest/top/bottom values, ALWAYS add `WHERE column IS NOT NULL AND column > 0` to exclude NULL and zero values. NULLs sort first in DESC order and give meaningless results.
17. **SHOW METRIC VALUES**: When finding the "best" or "worst" by a metric, ALWAYS include the metric column in SELECT alongside the ID. For example, if asked "which ad has highest roas", return both ad_id AND roas value.
18. **AGGREGATE PROPERLY**: For "highest/most/best" questions, prefer using aggregation (SUM, MAX, AVG) with GROUP BY rather than just ORDER BY, especially when the same entity appears in multiple rows.

## ANALYTICS & BUSINESS INTELLIGENCE RULES:
These rules ensure correct analytical queries for advertising, marketing, and business data.

19. **AGGREGATION LEVEL**: When the user asks about a campaign, ad, platform, etc. — ALWAYS GROUP BY that entity first. Never filter at the row level when the question is about an entity-level aggregate.
    -  `WHERE impressions >= 1000` (filters individual rows)
    - `GROUP BY campaign_name HAVING SUM(impressions) >= 1000` (filters entity totals)

20. **WEIGHTED METRICS**: Rate-based metrics (CPC, CTR, CPM, ROAS) must be calculated using weighted formulas, NOT simple AVG:
    - `AVG(cpc)` — this gives wrong results because it weights each row equally
    - `CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END` — correct weighted CPC
    - `AVG(ctr)` →  `CASE WHEN SUM(impressions) > 0 THEN SUM(clicks)::decimal / SUM(impressions) ELSE 0 END`
    -  `AVG(roas)` →  `CASE WHEN SUM(spend) > 0 THEN SUM(revenue) / SUM(spend) ELSE 0 END` (or SUM(roas * spend) / SUM(spend) for weighted avg)

21. **DATE + TIME GROUPING**: When grouping by week, month, or any time period:
    - ALWAYS include YEAR alongside the period: `EXTRACT(YEAR FROM date), EXTRACT(WEEK FROM date)`
    - Without year, weeks from different years get merged — this gives wrong analysis.
    - Prefer `DATE_TRUNC('week', date)` over `EXTRACT(WEEK FROM date)` for cleaner results.

22. **BEST/WORST COMPARISONS**: When asked for best and worst together, use window functions or CASE — NOT UNION:
    -  Use `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY metric DESC)` + filter rank = 1
    - Or use a CTE with CASE to flag MAX/MIN within the same query
    -  Avoid UNION with NULLs for missing columns

23. **ZERO vs NULL**: In metrics, treat 0 and NULL differently:
    - 0 means "measured but no value" (e.g., 0 clicks means the ad ran but nobody clicked)
    - NULL means "not measured" or "not applicable"
    - For rankings/averages, usually exclude NULLs but include 0s unless the question implies otherwise.

24. **ROUND DECIMALS**: Always ROUND numeric results for readability:
    - `ROUND(value::numeric, 2)` for rate metrics (CTR, CPC, CPM, ROAS)
    - `ROUND(value::numeric, 0)` for counts and currency

## RESPONSE FORMAT:
- Return ONLY the SQL query inside a code block like ```sql ... ```
- Do NOT include any explanation, comments, or text outside the SQL code block.
- The SQL must be ready to execute as-is.
"""

    def _build_user_prompt(self, user_query: str) -> str:
        """Build the user-facing part of the prompt."""
        return f"""Convert the following natural language question into a PostgreSQL SQL query. 
The question may be in any language — understand the intent and generate the correct SQL.

Question: {user_query}

Remember: Return ONLY the SQL inside ```sql ... ``` code block. No explanations."""

    def _build_correction_prompt(
        self, 
        original_query: str, 
        generated_sql: str, 
        error_message: str,
        error_detail: str = None,
        error_hint: str = None,
        attempt: int = 1
    ) -> str:
        """
        Build the self-correction prompt using database error feedback.
        This is equivalent to the Athena error feedback loop in the AWS solution.
        """
        correction_context = f"""The previously generated SQL query has an error. Please fix it.

## ORIGINAL QUESTION:
{original_query}

## GENERATED SQL THAT FAILED:
```sql
{generated_sql}
```

## ERROR FROM PostgreSQL (Attempt {attempt}):
Error Message: {error_message}
"""
        if error_detail:
            correction_context += f"Error Detail: {error_detail}\n"
        if error_hint:
            correction_context += f"Hint: {error_hint}\n"

        correction_context += """
## INSTRUCTIONS FOR CORRECTION:
1. Analyze the error message carefully.
2. Generate a corrected SQL query that fixes the specific error.
3. Make sure the corrected query still answers the original question.
4. Ensure all table and column names exist in the database metadata provided earlier.
5. Return ONLY the corrected SQL inside ```sql ... ``` code block.
"""
        return correction_context

    def _extract_sql(self, response_text: str) -> str:
        """Extract SQL from LLM response (from code block markers)."""
        patterns = [
            r'```sql\s*(.*?)\s*```',
            r'```\s*(.*?)\s*```',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, response_text, re.DOTALL | re.IGNORECASE)
            if matches:
                sql = matches[0].strip()
                if sql.lower().startswith("sql\n"):
                    sql = sql[4:].strip()
                return sql

        # Fallback: look for SQL-like lines
        lines = response_text.strip().split('\n')
        sql_lines = []
        in_sql = False
        for line in lines:
            stripped = line.strip().upper()
            if stripped.startswith(("SELECT", "WITH", "EXPLAIN")):
                in_sql = True
            if in_sql:
                sql_lines.append(line)

        if sql_lines:
            return "\n".join(sql_lines).strip()

        return response_text.strip()

    # ─── Main Methods ─────────────────────────────

    def generate_sql(self, user_query: str, metadata_context: str) -> dict:
 
        system_prompt = self._build_system_prompt(metadata_context)
        user_prompt = self._build_user_prompt(user_query)
        attempts = []
        
        try:
            logger.info(f"Generating SQL for: {user_query[:100]}...")
            
            response_text = self._call_llm(system_prompt, user_prompt)
            generated_sql = self._extract_sql(response_text)
            
            attempts.append({
                "attempt": 1,
                "sql": generated_sql,
                "status": "generated",
                "model": self.model_name,
                "raw_response": response_text[:500],
            })
            
            logger.info(f"SQL generated ({self.provider}/{self.model_name}): {generated_sql[:100]}...")
            
            return {
                "sql": generated_sql,
                "attempts": attempts,
                "final_attempt": 1,
                "status": "generated",
                "model_used": f"{self.provider}/{self.model_name}",
            }
            
        except Exception as e:
            logger.error(f"LLM generation error: {e}")
            attempts.append({
                "attempt": 1,
                "sql": None,
                "status": "error",
                "error": str(e),
            })
            return {
                "sql": None,
                "attempts": attempts,
                "final_attempt": 1,
                "status": "error",
                "error": str(e),
            }

    def correct_sql(
        self,
        original_query: str,
        failed_sql: str,
        error_message: str,
        metadata_context: str,
        error_detail: str = None,
        error_hint: str = None,
        attempt_number: int = 2,
    ) -> dict:
        """
        Self-correction: use the error feedback to generate corrected SQL.
        Key innovation from the AWS solution — using PostgreSQL error
        messages to improve the query iteratively.
        """
        system_prompt = self._build_system_prompt(metadata_context)
        correction_prompt = self._build_correction_prompt(
            original_query, failed_sql, error_message,
            error_detail, error_hint, attempt_number
        )
        
        try:
            logger.info(f"Self-correction attempt {attempt_number} for: {original_query[:80]}...")
            
            response_text = self._call_llm(system_prompt, correction_prompt)
            corrected_sql = self._extract_sql(response_text)
            
            logger.info(f"Corrected SQL attempt {attempt_number}: {corrected_sql[:100]}...")
            
            return {
                "sql": corrected_sql,
                "attempt": attempt_number,
                "status": "corrected",
                "model": self.model_name,
                "raw_response": response_text[:500],
            }
            
        except Exception as e:
            logger.error(f"Self-correction error on attempt {attempt_number}: {e}")
            return {
                "sql": None,
                "attempt": attempt_number,
                "status": "error",
                "error": str(e),
            }
