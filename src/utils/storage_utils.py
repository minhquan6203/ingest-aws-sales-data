"""
Utility functions for storage (MinIO and PostgreSQL)
"""
import os
import time
from minio import Minio
from minio.error import S3Error
import psycopg2
from loguru import logger

from src.config.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
    BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET
)


def get_minio_client():
    """
    Create a MinIO client
    """
    logger.info(f"Creating MinIO client for endpoint: {MINIO_ENDPOINT}")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def upload_file_to_minio(local_file_path, bucket_name, object_name=None):
    """
    Upload a file to MinIO

    Args:
        local_file_path: Path to the local file
        bucket_name: Name of the bucket
        object_name: Name of the object in MinIO (if None, use the filename)
    """
    if object_name is None:
        object_name = os.path.basename(local_file_path)
    
    # Add retry logic
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = get_minio_client()
            
            logger.info(f"Uploading {local_file_path} to {bucket_name}/{object_name}")
            client.fput_object(bucket_name, object_name, local_file_path)
            logger.info(f"Successfully uploaded {object_name} to {bucket_name}")
            return
        except S3Error as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(f"MinIO connection error (attempt {retry_count}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Error uploading {local_file_path}: {e}")
                raise


def get_postgres_connection():
    """
    Get a connection to PostgreSQL
    """
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}")
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
                connect_timeout=10  # Add connection timeout
            )
            connection.autocommit = False  # Ensure explicit transaction control
            return connection
        except psycopg2.OperationalError as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(f"PostgreSQL connection error (attempt {retry_count}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")
                raise


def execute_sql(sql, params=None, fetch=False):
    """
    Execute a SQL statement in PostgreSQL

    Args:
        sql: SQL statement to execute
        params: Parameters for the SQL statement
        fetch: Whether to fetch results

    Returns:
        Query results if fetch is True
    """
    conn = None
    cursor = None
    
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        logger.info(f"Executing SQL: {sql}")
        cursor.execute(sql, params or ())
        
        # Explicitly commit the transaction
        conn.commit()
        logger.debug(f"SQL executed and transaction committed successfully")
        
        if fetch:
            results = cursor.fetchall()
            return results
            
        return None
        
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
        logger.error(f"Error executing SQL: {e}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.debug("Database connection closed")


def execute_sql_with_retry(sql, params=None, fetch=False, max_retries=3, retry_delay=2):
    """
    Execute a SQL statement with retry logic for handling transient errors
    
    Args:
        sql: SQL statement to execute
        params: Parameters for the SQL statement
        fetch: Whether to fetch results
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay between retries (will use exponential backoff)
        
    Returns:
        Query results if fetch is True
    """
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            return execute_sql(sql, params, fetch)
        except psycopg2.OperationalError as e:
            # Operational errors like connection failures are retryable
            last_error = e
            retry_count += 1
            backoff = retry_delay * (2 ** (retry_count - 1))  # Exponential backoff
            logger.warning(f"Database connection error (attempt {retry_count}/{max_retries}): {e}")
            logger.info(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
        except Exception as e:
            # Don't retry other types of errors
            logger.error(f"Non-retryable database error: {e}")
            raise
    
    # If we get here, all retries failed
    logger.error(f"All {max_retries} database connection attempts failed: {last_error}")
    if last_error:
        raise last_error
    else:
        raise Exception(f"All {max_retries} database connection attempts failed with unknown error")


def create_table_if_not_exists(table_name, schema_dict, schema_name="public", primary_key=None):
    """
    Create a table in PostgreSQL if it doesn't exist
    
    Args:
        table_name: Name of the table to create
        schema_dict: Dictionary mapping column names to their types
        schema_name: Schema name in PostgreSQL
        primary_key: Column name to use as primary key
    """
    columns = []
    for col_name, col_type in schema_dict.items():
        # Map our simple types to PostgreSQL types
        pg_type = "TEXT"
        if col_type.lower() == "integer":
            pg_type = "INTEGER"
        elif col_type.lower() == "double":
            pg_type = "DOUBLE PRECISION"
        elif col_type.lower() == "boolean":
            pg_type = "BOOLEAN"
        elif col_type.lower() == "date":
            pg_type = "DATE"
        elif col_type.lower() == "timestamp":
            pg_type = "TIMESTAMP"
            
        columns.append(f"\"{col_name}\" {pg_type}")
    
    # Add primary key constraint if specified
    primary_key_clause = ""
    if primary_key and primary_key in schema_dict:
        primary_key_clause = f", PRIMARY KEY (\"{primary_key}\")"
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {', '.join(columns)}{primary_key_clause}
    );
    """
    
    logger.info(f"Creating table {schema_name}.{table_name} if it doesn't exist")
    execute_sql_with_retry(create_table_sql)


def load_data_from_minio_to_postgres(bucket_name, table_name, schema_name="public"):
    """
    Load data from MinIO to PostgreSQL table
    
    Args:
        bucket_name: Name of the MinIO bucket
        table_name: Name of the PostgreSQL table
        schema_name: Schema name in PostgreSQL
    """
    client = get_minio_client()
    conn = None
    cursor = None
    
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # List all objects in the bucket
        objects = client.list_objects(bucket_name)
        
        # Read each Parquet file and insert into PostgreSQL
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                # Get the Parquet file from MinIO
                data = client.get_object(bucket_name, obj.object_name)
                
                # Read Parquet data using pandas
                import pandas as pd
                import io
                df = pd.read_parquet(io.BytesIO(data.read()))
                
                # Insert data into PostgreSQL
                for _, row in df.iterrows():
                    columns = ', '.join(f'"{col}"' for col in df.columns)
                    values = ', '.join('%s' for _ in df.columns)
                    sql = f'INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({values})'
                    cursor.execute(sql, tuple(row))
        
        conn.commit()
        logger.info(f"Successfully loaded data from {bucket_name} to {schema_name}.{table_name}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error loading data from MinIO to PostgreSQL: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def init_database():
    """
    Initialize the database by creating necessary tables
    """
    logger.info("Initializing database...")
    
    # Create schemas
    create_schema_sql = """
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
    """
    execute_sql_with_retry(create_schema_sql)
    
    # Create audit table
    create_audit_table_sql = """
    CREATE TABLE IF NOT EXISTS public.etl_audit (
        audit_id SERIAL PRIMARY KEY,
        pipeline_id VARCHAR(255) NOT NULL,
        source_name VARCHAR(255),
        destination_name VARCHAR(255),
        start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        end_time TIMESTAMP,
        records_processed INTEGER,
        status VARCHAR(50) DEFAULT 'RUNNING',
        error TEXT,
        load_type VARCHAR(50),
        metadata JSONB
    );
    """
    
    # Create watermarks table
    create_watermarks_table_sql = """
    CREATE TABLE IF NOT EXISTS public.etl_watermarks (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        column_name VARCHAR(255) NOT NULL,
        max_value TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (table_name, column_name)
    );
    """
    
    # Execute SQL
    try:
        execute_sql_with_retry(create_schema_sql)
        execute_sql_with_retry(create_audit_table_sql)
        execute_sql_with_retry(create_watermarks_table_sql)
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise


def init_minio():
    """
    Initialize MinIO by creating necessary buckets
    """
    logger.info("Initializing MinIO...")
    
    # Add retry logic for MinIO connection
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = get_minio_client()
            
            # Create buckets if they don't exist
            for bucket in [BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET]:
                if not client.bucket_exists(bucket):
                    client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
                else:
                    logger.info(f"Bucket already exists: {bucket}")
            
            logger.info("MinIO initialization completed successfully")
            return
            
        except S3Error as e:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(f"MinIO connection error (attempt {retry_count}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to initialize MinIO after {max_retries} attempts: {e}")
                raise 