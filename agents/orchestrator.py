from mcp_client.client import MCPClient
from agents.cardinality_agent import CardinalityAgent
from agents.schema_graph_agent import SchemaGraphAgent
from agents.fk_detection_agent import FKDetectionAgent
from agents.verification_agent import VerificationAgent
from utils.logger import setup_logger

logger = setup_logger(__name__)


class Orchestrator:

    def __init__(self):
        self.mcp_client = MCPClient()

    async def run_pipeline(self) -> dict:
        logger.info("Starting FK detection pipeline")

        try:
            # Step 1: Connect to MCP server
            await self.mcp_client.connect()
            logger.info("MCP server connected")

            # Step 2: List all tables
            tables = await self.mcp_client.list_tables()
            logger.info(f"Found {len(tables)} tables: {tables}")

            if not tables:
                return {"tables": [], "error": "No tables found in database"}

            # Step 3: Describe each table
            tables_metadata = {}
            for table_name in tables:
                if isinstance(table_name, dict):
                    table_name = table_name.get("table_name", str(table_name))
                table_name = str(table_name)
                meta = await self.mcp_client.describe_table(table_name)
                tables_metadata[table_name] = meta
                logger.info(f"Described table: {table_name}")

            # Step 4: Detect TRUE PK per table
            true_pks = await CardinalityAgent.find_true_pks(
                self.mcp_client, tables_metadata
            )
            logger.info(f"True PKs: {true_pks}")

            # Step 5: Generate FK candidates using true PKs
            fk_candidates = SchemaGraphAgent.build_candidates(
                tables_metadata, true_pks
            )
            logger.info(f"FK candidates: {len(fk_candidates)}")

            if not fk_candidates:
                return {
                    "tables": tables,
                    "tables_metadata": tables_metadata,
                    "true_pks": true_pks,
                    "fk_candidates": [],
                    "verified_fks": [],
                    "results": {
                        "alter_statements": [],
                        "summary": [
                            "No FK candidates found from column naming patterns."
                        ],
                        "true_pks": true_pks,
                        "confirmed_fks": [],
                        "rejected_fks": []
                    }
                }

            # Step 6: Verify candidates with Jaccard + referential integrity
            verified_fks = await FKDetectionAgent.verify_fk_pairs(
                self.mcp_client, fk_candidates
            )
            logger.info(f"FK verification complete: {len(verified_fks)} results")

            # Step 7: Generate ALTER TABLE statements
            results = VerificationAgent.generate_results(
                verified_fks, true_pks, tables_metadata
            )

            pipeline_result = {
                "tables": tables,
                "tables_metadata": tables_metadata,
                "true_pks": true_pks,
                "fk_candidates": fk_candidates,
                "verified_fks": verified_fks,
                "results": results
            }

            logger.info("FK detection pipeline completed successfully")
            return pipeline_result

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            await self.mcp_client.disconnect()
