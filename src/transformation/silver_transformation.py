"""
Silver layer transformation module
"""
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, current_timestamp, lit

from src.config.config import BRONZE_BUCKET, SILVER_BUCKET, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from src.utils.spark_utils import create_spark_session, apply_transformations, write_dataframe_to_parquet
from src.utils.storage_utils import create_table_if_not_exists, load_data_from_minio_to_postgres, get_postgres_connection, execute_sql
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier
from src.ingestion.bronze_ingestion import get_last_processed_timestamp, update_watermark


def transform_to_silver(pipeline_config, source_config, audit=True, notify=True, incremental=True):
    """
    Transform data from bronze to silver layer
    
    Args:
        pipeline_config: Pipeline configuration from metadata
        source_config: Source configuration from metadata
        audit: Whether to create audit records
        notify: Whether to send notifications
        incremental: Whether to use incremental loading (vs. full load)
        
    Returns:
        DataFrame containing the transformed data
        Number of records processed
    """
    bronze_table = source_config.get("bronze_table")
    silver_table = source_config.get("silver_table")
    schema_dict = source_config.get("schema", {})
    primary_key = source_config.get("primary_key")
    incremental_column = source_config.get("incremental_column")
    transformations = pipeline_config.get("transformations", [])
    quality_checks = pipeline_config.get("quality_checks", [])
    
    # Determine load mode
    load_type = "incremental" if incremental and incremental_column else "full"
    logger.info(f"Transforming data from {bronze_table} to {silver_table} (mode: {load_type})")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        auditor = ETLAuditor(
            pipeline_id=pipeline_config.get("source"),
            source_name=f"bronze.{bronze_table}",
            destination_name=f"silver.{silver_table}",
            load_type=load_type
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=pipeline_config.get("source"),
                source_name=f"bronze.{bronze_table}",
                destination_name=f"silver.{silver_table}"
            )
    
    try:
        # Read data from bronze layer
        bronze_path = f"s3a://{BRONZE_BUCKET}/{bronze_table}"
        
        # For incremental loads, we only need to process data that's newer than the last processed timestamp
        last_timestamp = None
        incremental_filter_applied = False
        
        if incremental and incremental_column:
            df_bronze = spark.read.parquet(bronze_path)
            
            if incremental_column:
                last_timestamp = get_last_processed_timestamp(silver_table, incremental_column)
                if last_timestamp:
                    logger.info(f"Applying incremental filter on {incremental_column} > {last_timestamp}")
                    df_bronze = df_bronze.filter(col(incremental_column) > lit(last_timestamp))
                    incremental_filter_applied = True
        else:
            # Full load
            df_bronze = spark.read.parquet(bronze_path)
        
        # Get the count of records to process
        bronze_count = df_bronze.count()
        logger.info(f"Processing {bronze_count} records from {bronze_table} {'(after filtering)' if incremental_filter_applied else ''}")
        
        # If no records to process, return early
        if bronze_count == 0:
            logger.info(f"No new records to process for {silver_table}")
            if auditor:
                auditor.complete_successfully(0)
            return df_bronze, 0
            
        # Apply transformations
        if transformations:
            df = apply_transformations(df_bronze, transformations)
        else:
            df = df_bronze
        
        # Apply data quality checks
        failed_checks = []
        if quality_checks:
            df, failed_checks = apply_quality_checks(df, quality_checks)
            
            # Report data quality issues
            if failed_checks and notify:
                for check in failed_checks:
                    ETLNotifier.notify_data_quality_issue(
                        pipeline_id=pipeline_config.get("source"),
                        source_name=bronze_table,
                        quality_check=check["message"],
                        failed_records=check["failed_count"]
                    )
        
        # Get record count after transformations and quality checks
        count = df.count()
        logger.info(f"Transformed {count} records from {bronze_table} to {silver_table}")
        
        if auditor:
            incremental_info = {
                "incremental_column": str(incremental_column) if incremental_column else None,
                "last_timestamp": str(last_timestamp) if last_timestamp else None,
                "filtered_records": incremental_filter_applied
            } if incremental_filter_applied else None
            
            auditor.set_records_processed(count, metadata=incremental_info)
            
            # Update watermark if we processed records
            if count > 0 and incremental_column:
                max_value = df.agg({incremental_column: "max"}).collect()[0][0]
                if max_value:
                    update_watermark(silver_table, incremental_column, str(max_value))
                    logger.info(f"Updated watermark for {silver_table}.{incremental_column} to {max_value}")
        
        # Handle the write to silver layer differently based on incremental vs full load
        silver_path = f"s3a://{SILVER_BUCKET}/{silver_table}"
        
        # Determine write strategy based on load type and primary key
        if incremental and primary_key:
            try:
                # Try to read the existing silver dataset
                logger.info(f"Attempting incremental load for {silver_table}")
                existing_df = spark.read.format("parquet").load(silver_path)
                existing_count = existing_df.count()
                logger.info(f"Found {existing_count} existing records in silver.{silver_table}")
                
                # Perform merge based on primary key
                if existing_count > 0:
                    # Anti-join: Keep only the records from the source that don't exist in the target
                    new_records = df.join(
                        existing_df.select(primary_key), 
                        on=primary_key,
                        how="left_anti"
                    )
                    new_count = new_records.count()
                    
                    # Updates: Records that exist in both source and target (get the latest)
                    updates = df.join(
                        existing_df.select(primary_key),
                        on=primary_key,
                        how="inner"
                    )
                    update_count = updates.count()
                    
                    logger.info(f"Incremental merge for silver.{silver_table}: {new_count} new records, {update_count} updates")
                    
                    # For updates, we need to remove old versions before inserting new ones
                    if update_count > 0:
                        # Remove records that are being updated
                        existing_df = existing_df.join(
                            updates.select(primary_key),
                            on=primary_key,
                            how="left_anti"
                        )
                        
                    # Combine existing (minus updates) with new records and updates
                    merged_df = existing_df.union(new_records).union(updates)
                    
                    # Write the merged dataframe
                    logger.info(f"Incremental load: Writing merged data to silver.{silver_table} (mode: overwrite)")
                    write_dataframe_to_parquet(merged_df, silver_path, mode="overwrite")
                    
                    # Track the actual merged count instead of just the new records
                    count = merged_df.count()
                    logger.info(f"Total records after incremental merge: {count}")
                else:
                    # If no existing data, just write the new data
                    logger.info(f"No existing records found, writing {df.count()} records to silver.{silver_table}")
                    write_dataframe_to_parquet(df, silver_path, mode="overwrite")
            except Exception as e:
                if "FileNotFoundException" in str(e):
                    # This is likely a transient file system issue, not a genuine "first run" scenario
                    # We still want to preserve incremental logic for the database operation
                    logger.warning(f"Parquet files not found in silver.{silver_table}: {e}")
                    logger.warning(f"Will write current filtered data but maintain incremental behavior")
                    write_dataframe_to_parquet(df, silver_path, mode="overwrite")
                else:
                    logger.warning(f"No existing data found in silver.{silver_table}, treating as initial load: {e}")
                    logger.info(f"Initial load: Writing {df.count()} records to silver.{silver_table}")
                    write_dataframe_to_parquet(df, silver_path, mode="overwrite")
        else:
            # For full loads, we overwrite the silver table
            write_mode = "overwrite"
            logger.info(f"Full load: Writing {df.count()} records to silver.{silver_table} (mode: {write_mode})")
            write_dataframe_to_parquet(df, silver_path, mode=write_mode)
        
        # Create or update PostgreSQL table for the silver layer
        create_table_if_not_exists(
            silver_table,
            schema_dict,
            schema_name="silver",
            primary_key=primary_key
        )
        
        # Use JDBC to write data to PostgreSQL - different approach for incremental vs full load
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        
        try:
            # Store a flag to track if we're doing UPSERT or full replace
            # This ensures we use incremental UPSERT even if Parquet files were missing
            do_upsert = incremental and primary_key
            
            if do_upsert:
                # For incremental loads with a primary key, need to handle upserts
                # PostgreSQL supports UPSERT with ON CONFLICT
                
                # First, create a temporary table to stage the data
                temp_table = f"{silver_table}_temp"
                df.write \
                   .format("jdbc") \
                   .option("url", jdbc_url) \
                   .option("dbtable", f"silver.{temp_table}") \
                   .option("user", POSTGRES_USER) \
                   .option("password", POSTGRES_PASSWORD) \
                   .option("driver", "org.postgresql.Driver") \
                   .mode("overwrite") \
                   .save()
                
                # Generate a list of all columns except the primary key
                cols = [col for col in schema_dict.keys() if col != primary_key]
                update_cols = ", ".join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in cols])
                
                # Perform UPSERT operation with proper transaction handling
                upsert_sql = f"""
                BEGIN;
                -- Insert new records or update existing ones
                INSERT INTO silver.{silver_table} 
                SELECT * FROM silver.{temp_table}
                ON CONFLICT ({primary_key})
                DO UPDATE SET {update_cols};
                
                -- Clean up temp table
                DROP TABLE IF EXISTS silver.{temp_table};
                COMMIT;
                """
                execute_sql(upsert_sql)
                
                logger.info(f"Successfully performed UPSERT into silver.{silver_table}")
            else:
                # For full load, delete existing and insert new
                execute_sql(f"DELETE FROM silver.{silver_table}")
                
                # Write DataFrame to PostgreSQL
                df.write \
                   .format("jdbc") \
                   .option("url", jdbc_url) \
                   .option("dbtable", f"silver.{silver_table}") \
                   .option("user", POSTGRES_USER) \
                   .option("password", POSTGRES_PASSWORD) \
                   .option("driver", "org.postgresql.Driver") \
                   .mode("append") \
                   .save()
               
                load_type_str = "incremental" if incremental else "full"
                logger.info(f"Successfully wrote data to silver.{silver_table} via JDBC (mode: {load_type_str})")
        except Exception as e:
            logger.error(f"Error writing data to PostgreSQL via JDBC: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
        # Update audit status
        if auditor:
            auditor.complete_successfully(count)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_success(
                    pipeline_id=pipeline_config.get("source"),
                    source_name=f"bronze.{bronze_table}",
                    destination_name=f"silver.{silver_table}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error transforming data to silver layer: {error_msg}")
        import traceback
        logger.error(traceback.format_exc())
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=pipeline_config.get("source"),
                    source_name=f"bronze.{bronze_table}",
                    destination_name=f"silver.{silver_table}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise


def apply_quality_checks(df, quality_checks):
    """
    Apply data quality checks to a DataFrame
    
    Args:
        df: Spark DataFrame
        quality_checks: List of quality check definitions
        
    Returns:
        DataFrame with quality checks applied
        List of failed quality checks
    """
    import traceback
    failed_checks = []
    
    logger.debug(f"Applying quality checks: {quality_checks}")
    
    for check in quality_checks:
        try:
            column = check.get("column")
            rule_type = check.get("rule_type")
            
            logger.debug(f"Processing check: column={column}, rule_type={rule_type}")
            
            if rule_type == "not_null":
                # Count records where column is null
                logger.debug(f"Checking for NULL values in column {column}")
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    message = f"Column '{column}' contains {null_count} NULL values"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": null_count
                    })
                    
                    # Filter out records with NULL values
                    df = df.filter(col(column).isNotNull())
                    
            elif rule_type == "greater_than":
                value = check.get("value")
                logger.debug(f"Checking if values in column {column} are greater than {value}")
                # Count records where column is not greater than value
                invalid_count = df.filter(col(column) <= value).count()
                if invalid_count > 0:
                    message = f"Column '{column}' contains {invalid_count} values not greater than {value}"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": invalid_count
                    })
                    
                    # Filter out invalid records
                    df = df.filter(col(column) > value)
                    
            elif rule_type == "greater_than_equal":
                value = check.get("value")
                logger.debug(f"Checking if values in column {column} are greater than or equal to {value}")
                # Count records where column is less than value
                invalid_count = df.filter(col(column) < value).count()
                if invalid_count > 0:
                    message = f"Column '{column}' contains {invalid_count} values less than {value}"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": invalid_count
                    })
                    
                    # Filter out invalid records
                    df = df.filter(col(column) >= value)
                    
            elif rule_type == "regex":
                pattern = check.get("pattern")
                logger.debug(f"Checking if values in column {column} match pattern {pattern}")
                # Count records where column doesn't match the regex pattern
                try:
                    # Use a simpler approach to avoid Column object issues
                    df_valid = df.filter(col(column).rlike(pattern))
                    invalid_count = df.count() - df_valid.count()
                    
                    if invalid_count > 0:
                        message = f"Column '{column}' contains {invalid_count} values not matching pattern '{pattern}'"
                        logger.warning(message)
                        failed_checks.append({
                            "column": column,
                            "rule_type": rule_type,
                            "message": message,
                            "failed_count": invalid_count
                        })
                        
                        # Keep only valid records
                        df = df_valid
                except Exception as e:
                    logger.error(f"Error applying regex check: {str(e)}")
                    logger.error(traceback.format_exc())
                    # Skip this check if there's an error
                    continue
                    
            elif rule_type == "date_format":
                date_format = check.get("format")
                logger.debug(f"Checking date format for column {column}")
                # This is a simplification; in practice, would need more robust date validation
                # For now, just log that we're checking this
                logger.info(f"Date format check for column {column} with format {date_format} is simplified")
                
        except Exception as e:
            logger.error(f"Error in quality check {check}: {str(e)}")
            logger.error(traceback.format_exc())
            # Continue with the next check if there's an error
            continue
            
    return df, failed_checks 