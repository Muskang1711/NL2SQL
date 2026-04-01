import subprocess
import json
import asyncio
from utils.logger import setup_logger
from config import get_settings

logger = setup_logger(__name__)


class MCPClient:
    """MCP client that communicates with @modelcontextprotocol/server-postgres via stdio."""

    def __init__(self):
        self._process = None
        self._request_id = 0
        self._lock = asyncio.Lock()

    async def connect(self):
        settings = get_settings()
        connection_string = (
            f"postgresql://{settings.db_user}:{settings.db_password}"
            f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
        )
        logger.info("Starting MCP server subprocess: @modelcontextprotocol/server-postgres")
        self._process = await asyncio.create_subprocess_exec(
            "npx", "-y", "@modelcontextprotocol/server-postgres", connection_string,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        # Send initialize request
        await self._send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "adv_nl2sql", "version": "1.0.0"}
        })
        # Send initialized notification
        await self._send_notification("notifications/initialized", {})
        logger.info("MCP server connected and initialized")

    async def disconnect(self):
        if self._process:
            try:
                self._process.stdin.close()
                await self._process.wait()
            except Exception:
                pass
            self._process = None
            logger.info("MCP server disconnected")

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _send_notification(self, method: str, params: dict):
        message = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }
        line = json.dumps(message) + "\n"
        self._process.stdin.write(line.encode())
        await self._process.stdin.drain()

    async def _send_request(self, method: str, params: dict) -> dict:
        async with self._lock:
            request_id = self._next_id()
            message = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params
            }
            line = json.dumps(message) + "\n"
            self._process.stdin.write(line.encode())
            await self._process.stdin.drain()

            # Read response lines until we get our response
            while True:
                response_line = await self._process.stdout.readline()
                if not response_line:
                    # Check stderr for error details
                    stderr_data = b""
                    try:
                        stderr_data = await asyncio.wait_for(
                            self._process.stderr.read(4096), timeout=1.0
                        )
                    except (asyncio.TimeoutError, Exception):
                        pass
                    err_msg = stderr_data.decode().strip() if stderr_data else "unknown error"
                    raise ConnectionError(f"MCP server closed connection: {err_msg}")

                response_line = response_line.decode().strip()
                if not response_line:
                    continue
                try:
                    response = json.loads(response_line)
                except json.JSONDecodeError:
                    continue
                # Skip notifications, wait for our response
                if "id" in response and response["id"] == request_id:
                    if "error" in response:
                        raise RuntimeError(f"MCP error: {response['error']}")
                    return response.get("result", {})

    async def run_query(self, sql: str) -> list:
        """Run a SQL query via the MCP server's 'query' tool."""
        logger.info(f"MCP query: {sql[:120]}")
        result = await self._send_request("tools/call", {
            "name": "query",
            "arguments": {"sql": sql}
        })
        # Extract text content from MCP tool result
        content_list = result.get("content", [])
        text_parts = []
        for item in content_list:
            if item.get("type") == "text":
                text_parts.append(item["text"])
        raw = "\n".join(text_parts)
        try:
            parsed = json.loads(raw)
            # The server returns rows as list of dicts
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict) and "rows" in parsed:
                return parsed["rows"]
            return [parsed]
        except json.JSONDecodeError:
            return [{"raw": raw}]

    async def list_tables(self, schema: str = None) -> list:
        """List all tables in the given schema using SQL."""
        if schema is None:
            settings = get_settings()
            schema = settings.db_schema
        logger.info(f"MCP list_tables (schema={schema})")
        sql = (
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
            f"ORDER BY table_name"
        )
        rows = await self.run_query(sql)
        tables = []
        for row in rows:
            if isinstance(row, dict) and "table_name" in row:
                tables.append(row["table_name"])
            elif isinstance(row, dict):
                # Take the first value
                tables.append(str(list(row.values())[0]))
        return tables

    async def describe_table(self, table_name: str, schema: str = None) -> dict:
        """Describe a table's columns using SQL."""
        if schema is None:
            settings = get_settings()
            schema = settings.db_schema
        logger.info(f"MCP describe_table: {schema}.{table_name}")
        sql = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table_name}' "
            f"ORDER BY ordinal_position"
        )
        rows = await self.run_query(sql)
        columns = []
        for row in rows:
            if isinstance(row, dict):
                columns.append({
                    "column_name": row.get("column_name", ""),
                    "data_type": row.get("data_type", ""),
                    "is_nullable": row.get("is_nullable", "YES")
                })
        return {"columns": columns}
