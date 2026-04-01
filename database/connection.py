import math
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from config import get_settings
from utils.logger import setup_logger

logger = setup_logger(__name__)


class DatabaseManager:
    _pool = None
    
    @classmethod
    def initialize_pool(cls):
        if cls._pool is None:
            settings = get_settings()
            logger.info("Initializing database connection pool")
            # Try to create the schema (may fail if user lacks CREATE privilege)
            try:
                conn = psycopg2.connect(
                    host=settings.db_host,
                    port=settings.db_port,
                    database=settings.db_name,
                    user=settings.db_user,
                    password=settings.db_password
                )
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema}")
                conn.close()
                logger.info(f"Schema '{settings.db_schema}' ensured")
            except Exception as e:
                logger.warning(f"Could not create schema '{settings.db_schema}': {e}")
                logger.warning("Schema must be created manually by a superuser")

            # Now create the pool with search_path set to the schema
            cls._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.db_host,
                port=settings.db_port,
                database=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                options=f"-c search_path={settings.db_schema}"
            )
            logger.info(f"Database pool initialized (schema={settings.db_schema})")
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        cls.initialize_pool()
        settings = get_settings()
        conn = cls._pool.getconn()
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {settings.db_schema}")
        conn.commit()
        logger.debug(f"Connection acquired (schema={settings.db_schema})")
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cls._pool.putconn(conn)
            logger.debug("Connection returned to pool")
    
    @classmethod
    def execute_query(cls, query: str, params: tuple = None):
        logger.info(f"Executing query: {query[:100]}...")
        with cls.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                return None
    
    @classmethod
    def drop_table(cls, table_name: str):
        """Drop a table if it exists (CASCADE to remove dependencies)."""
        settings = get_settings()
        schema = settings.db_schema
        logger.info(f"Dropping table if exists: {schema}.{table_name}")
        query = f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'
        cls.execute_query(query)
        logger.info(f"Table {schema}.{table_name} dropped")

    @classmethod
    def create_table(cls, table_name: str, columns: dict):
        settings = get_settings()
        schema = settings.db_schema
        logger.info(f"Creating table: {schema}.{table_name}")
        col_defs = ", ".join([f'"{k}" {v}' for k, v in columns.items()])
        query = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({col_defs})'
        cls.execute_query(query)
        logger.info(f"Table {schema}.{table_name} created successfully")
    
    @classmethod
    def insert_dataframe(cls, table_name: str, df):
        settings = get_settings()
        schema = settings.db_schema
        logger.info(f"Inserting {len(df)} rows into {schema}.{table_name}")
        with cls.get_connection() as conn:
            with conn.cursor() as cur:
                cols = ", ".join([f'"{c}"' for c in df.columns])
                vals = ", ".join(["%s"] * len(df.columns))
                query = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES ({vals})'
                for row in df.itertuples(index=False):
                    # Convert NaN/NaT to None for proper SQL NULL
                    values = tuple(
                        None if (v is None or (isinstance(v, float) and math.isnan(v)))
                        else v
                        for v in row
                    )
                    cur.execute(query, values)
        logger.info(f"Data inserted into {schema}.{table_name} successfully")
    
    @classmethod
    def close_pool(cls):
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logger.info("Database pool closed")