"""
Bronze layer ingestion module
"""
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
import os
from datetime import datetime
from pyspark.sql.functions import col, lit

from src.config.config import BRONZE_BUCKET
from src.utils.spark_utils import create_spark_session, read_csv_to_dataframe, write_dataframe_to_parquet
from src.utils.storage_utils import upload_file_to_minio, execute_sql
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier


def get_last_processed_timestamp(table_name, incremental_column):
    """
    Get the most recent timestamp value for a given table and column
    
    Args:
        table_name: Name of the table
        incremental_column: Column used for incremental processing
        
    Returns:
        The last processed timestamp or None if no records exist
    """
    sql = f"""
    SELECT max_value FROM public.etl_watermarks 
    WHERE table_name = %s AND column_name = %s;
    """
    try:
        result = execute_sql(sql, (table_name, incremental_column), fetch=True)
        if result and len(result) > 0 and result[0][0]:
            last_timestamp = result[0][0]
            logger.info(f"Found last processed timestamp for {table_name}.{incremental_column}: {last_timestamp}")
            return last_timestamp
        logger.info(f"No previous timestamp found for {table_name}.{incremental_column}")
        return None
    except Exception as e:
        logger.warning(f"Failed to retrieve last processed timestamp: {e}")
        return None


def update_watermark(table_name, incremental_column, max_value):
    """
    Update the watermark for a table and column
    
    Args:
        table_name: Name of the table
        incremental_column: Column used for incremental processing
        max_value: Maximum value to store
    """
    if not max_value:
        logger.warning(f"Attempted to update watermark with null value for {table_name}.{incremental_column}")
        return
        
    # First check if a record exists
    check_sql = """
    SELECT 1 FROM public.etl_watermarks 
    WHERE table_name = %s AND column_name = %s;
    """
    result = execute_sql(check_sql, (table_name, incremental_column), fetch=True)
    
    if result and len(result) > 0:
        # Update existing record
        update_sql = """
        UPDATE public.etl_watermarks 
        SET max_value = %s, last_updated = NOW() 
        WHERE table_name = %s AND column_name = %s;
        """
        execute_sql(update_sql, (max_value, table_name, incremental_column))
    else:
        # Insert new record
        insert_sql = """
        INSERT INTO public.etl_watermarks (table_name, column_name, max_value, last_updated)
        VALUES (%s, %s, %s, NOW());
        """
        execute_sql(insert_sql, (table_name, incremental_column, max_value))
    
    logger.info(f"Updated watermark for {table_name}.{incremental_column} to {max_value}")


def ingest_to_bronze(source_config, audit=True, notify=True, incremental=True):
    """
    Ingest data from source to bronze layer
    
    Args:
        source_config: Source configuration from metadata
        audit: Whether to create audit records
        notify: Whether to send notifications
        incremental: Whether to use incremental loading (vs. full load)
        
    Returns:
        DataFrame containing the ingested data
        Number of records processed
    """
    source_type = source_config.get("source_type")
    source_path = source_config.get("source_path")
    schema_dict = source_config.get("schema", {})
    bronze_table = source_config.get("bronze_table")
    incremental_column = source_config.get("incremental_column")
    
    # Determine load mode
    load_type = "incremental" if incremental and incremental_column else "full"
    logger.info(f"Ingesting {source_type} data from {source_path} to bronze layer (mode: {load_type})")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        auditor = ETLAuditor(
            pipeline_id=f"bronze_{bronze_table}",
            source_name=source_path,
            destination_name=f"bronze.{bronze_table}",
            load_type=load_type
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=f"bronze_{bronze_table}",
                source_name=source_path,
                destination_name=f"bronze.{bronze_table}"
            )
    
    try:
        # Create Spark schema from schema_dict
        spark_schema = None
        if schema_dict:
            spark_schema = create_spark_schema(schema_dict)
        
        # Read data from source
        if source_type == "csv":
            source_df = read_csv_to_dataframe(spark, source_path, schema=spark_schema)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # For incremental loading, retrieve the last processed timestamp and filter the data
        incremental_filter_applied = False
        last_timestamp = None
        
        if incremental and incremental_column and incremental_column in schema_dict:
            last_timestamp = get_last_processed_timestamp(bronze_table, incremental_column)
            
            if last_timestamp:
                logger.info(f"Applying incremental filter on {incremental_column} > {last_timestamp}")
                source_df = source_df.filter(col(incremental_column) > lit(last_timestamp))
                incremental_filter_applied = True
                
        # Get number of records after filtering
        count = source_df.count()
        logger.info(f"Processing {count} records from {source_path} {'(after filtering)' if incremental_filter_applied else ''}")
        
        if auditor:
            incremental_info = {
                "incremental_column": str(incremental_column) if incremental_column else None,
                "last_timestamp": str(last_timestamp) if last_timestamp else None,
                "filtered_records": incremental_filter_applied
            } if incremental_filter_applied else None
            
            auditor.set_records_processed(count, metadata=incremental_info)
        
        # Skip processing if no records to process
        if count == 0:
            logger.info(f"No new records to process for {bronze_table}")
            if auditor:
                auditor.complete_successfully(0)
            return source_df, 0
        
        # Write to bronze layer - use append mode for incremental, overwrite for full load
        bronze_path = f"s3a://{BRONZE_BUCKET}/{bronze_table}"
        write_mode = "append" if incremental else "overwrite"
        
        # Log more detailed information for incremental loads
        if incremental:
            logger.info(f"Incremental load: Writing {count} new records to {bronze_table} (mode: {write_mode})")
        else:
            logger.info(f"Full load: Writing {count} records to {bronze_table} (mode: {write_mode})")
            
        write_dataframe_to_parquet(source_df, bronze_path, mode=write_mode)
        
        # Upload source file to MinIO if it's a local file
        if os.path.isfile(source_path):
            try:
                upload_file_to_minio(source_path, BRONZE_BUCKET, f"raw/{os.path.basename(source_path)}")
            except Exception as e:
                logger.warning(f"Failed to upload source file to MinIO: {e}")
        
        # Update watermark if incremental load
        if incremental and incremental_column and count > 0:
            # Find the maximum value for the incremental column
            max_value_df = source_df.agg({incremental_column: "max"}).collect()
            if max_value_df and len(max_value_df) > 0:
                max_value = max_value_df[0][0]
                if max_value:
                    # Get previous watermark for logging
                    previous_watermark = get_last_processed_timestamp(bronze_table, incremental_column)
                    update_watermark(bronze_table, incremental_column, max_value)
                    logger.info(f"Incremental load watermark updated: {bronze_table}.{incremental_column} from {previous_watermark} to {max_value}")
        
        # Update audit status
        if auditor:
            auditor.complete_successfully(count)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_success(
                    pipeline_id=f"bronze_{bronze_table}",
                    source_name=source_path,
                    destination_name=f"bronze.{bronze_table}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return source_df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error ingesting data to bronze layer: {error_msg}")
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=f"bronze_{bronze_table}",
                    source_name=source_path,
                    destination_name=f"bronze.{bronze_table}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise


def create_spark_schema(schema_dict):
    """
    Create a Spark schema from a schema dictionary
    
    Args:
        schema_dict: Dictionary mapping column names to data types
        
    Returns:
        Spark StructType schema
    """
    type_mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": IntegerType(),  # Using IntegerType for simplicity
        "double": DoubleType(),
        "float": DoubleType(),  # Using DoubleType for simplicity
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }
    
    fields = []
    for column_name, data_type in schema_dict.items():
        spark_type = type_mapping.get(data_type.lower(), StringType())
        fields.append(StructField(column_name, spark_type, True))
    
    return StructType(fields) 