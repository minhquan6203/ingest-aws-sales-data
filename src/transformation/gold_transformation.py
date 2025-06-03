"""
Gold layer transformation module
"""
from loguru import logger
from pyspark.sql.functions import col

from src.config.config import SILVER_BUCKET, GOLD_BUCKET, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from src.utils.spark_utils import create_spark_session, write_dataframe_to_parquet
from src.utils.storage_utils import create_table_if_not_exists, execute_sql
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier


def transform_to_gold(pipeline_config, incremental=False, audit=True, notify=True):
    """
    Transform data from silver to gold layer using SQL
    
    Args:
        pipeline_config: Pipeline configuration from metadata
        incremental: Whether to use incremental loading
        audit: Whether to create audit records
        notify: Whether to send notifications
        
    Returns:
        DataFrame containing the transformed data
        Number of records processed
    """
    sources = pipeline_config.get("sources", [])
    destination = pipeline_config.get("destination")
    sql_transform = pipeline_config.get("sql_transform")
    incremental_condition = pipeline_config.get("incremental_condition", "")
    
    if not sql_transform:
        raise ValueError("SQL transform is required for gold layer transformation")
    
    # Determine load mode
    load_type = "incremental" if incremental and incremental_condition else "full"
    logger.info(f"Transforming data to gold layer: {destination} (mode: {load_type})")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        source_name = ", ".join(f"silver.{s}" for s in sources)
        
        auditor = ETLAuditor(
            pipeline_id=pipeline_config.get("id", destination),
            source_name=source_name,
            destination_name=f"gold.{destination}",
            load_type=load_type
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=pipeline_config.get("id", destination),
                source_name=source_name,
                destination_name=f"gold.{destination}"
            )
    
    try:
        # Register each silver table as a temporary view
        for silver_table in sources:
            silver_path = f"s3a://{SILVER_BUCKET}/{silver_table}"
            silver_df = spark.read.format("parquet").load(silver_path)
            silver_df.createOrReplaceTempView(silver_table)
            logger.info(f"Registered silver table as view: {silver_table}")
        
        # Execute the SQL transformation
        logger.info(f"Executing SQL transformation for {destination}")
        
        # For incremental loads, try to add a filter
        sql_with_filter = sql_transform
        if incremental and incremental_condition:
            try:
                # Try to read the table to see if it exists
                exists_result = execute_sql(
                    f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'gold' 
                        AND table_name = '{destination}'
                    );
                    """, 
                    fetch=True
                )
                table_exists = exists_result[0][0] if exists_result else False
                
                if table_exists:
                    # Get the max date for the incremental filter
                    max_date = None
                    count_result = execute_sql(f"SELECT COUNT(*) FROM gold.{destination}", fetch=True)
                    has_records = count_result[0][0] > 0 if count_result else False
                    
                    if has_records:
                        # For gold_order_summary, we're using 'last_order_date' as the incremental column
                        max_date_result = execute_sql(f"SELECT MAX(last_order_date) FROM gold.{destination}", fetch=True)
                        max_date = max_date_result[0][0] if max_date_result and max_date_result[0][0] else None
                    
                    if max_date:
                        # Add the incremental filter to the SQL by adding to JOIN condition
                        # This avoids the issue of trying to put WHERE after GROUP BY
                        if "JOIN silver_customers c ON" in sql_transform:
                            # Add to the JOIN condition before GROUP BY
                            modified_sql = sql_transform.replace(
                                "JOIN silver_customers c ON o.customer_id = c.customer_id",
                                f"JOIN silver_customers c ON o.customer_id = c.customer_id AND o.order_date > '{max_date}'"
                            )
                            sql_with_filter = modified_sql
                            logger.info(f"Added incremental filter condition with max_date: {max_date}")
                        else:
                            # Fallback to full load if we can't safely modify
                            logger.warning(f"Cannot safely add incremental filter, using full load")
                            sql_with_filter = sql_transform
                    else:
                        logger.info(f"No valid max date found in {destination}, performing full load")
                        sql_with_filter = sql_transform
            except Exception as e:
                if "FileNotFoundException" in str(e):
                    logger.warning(f"Parquet files not found when checking {destination}: {e}")
                    if incremental and incremental_condition:
                        logger.warning(f"Will continue with incremental transformation despite missing files")
                    sql_with_filter = sql_transform
                else:
                    logger.warning(f"Error checking gold table or getting max date: {e}, falling back to full load")
                    sql_with_filter = sql_transform
        else:
            # For full loads, use the original SQL
            sql_with_filter = sql_transform
        
        # Execute the final SQL query
        logger.info(f"Executing final SQL transformation")
        result_df = spark.sql(sql_with_filter)
            
        # Get record count
        count = result_df.count()
        logger.info(f"Transformed {count} records to gold.{destination}")
        
        if auditor:
            auditor.set_records_processed(count)
        
        # Write to gold layer
        gold_path = f"s3a://{GOLD_BUCKET}/{destination}"
        write_dataframe_to_parquet(result_df, gold_path)
        
        # Create or update PostgreSQL table for the gold layer
        # For simplicity, we'll infer the schema from the DataFrame
        schema_dict = {}
        for field in result_df.schema.fields:
            field_type = field.dataType.simpleString()
            # Map Spark types to our simplified type system
            if "int" in field_type:
                pg_type = "INTEGER"
            elif "double" in field_type or "float" in field_type:
                pg_type = "DOUBLE PRECISION"
            elif "boolean" in field_type:
                pg_type = "BOOLEAN"
            elif "timestamp" in field_type:
                pg_type = "TIMESTAMP"
            elif "date" in field_type:
                pg_type = "DATE"
            else:
                pg_type = "TEXT"
            
            schema_dict[field.name] = pg_type
        
        primary_key = pipeline_config.get("primary_key")
        create_table_if_not_exists(
            destination,
            schema_dict,
            schema_name="gold",
            primary_key=primary_key
        )
        
        # Write data to PostgreSQL using JDBC connection
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        
        try:
            # For incremental loads, we use a merge operation if an incremental condition is specified
            # Otherwise we use a full load with delete and insert
            if incremental and incremental_condition:
                logger.info(f"Incremental load: Using merge operation for gold.{destination}")
                
                # Save to temporary table first
                temp_table = f"temp_{destination}"
                result_df.write \
                   .format("jdbc") \
                   .option("url", jdbc_url) \
                   .option("dbtable", f"gold.{temp_table}") \
                   .option("user", POSTGRES_USER) \
                   .option("password", POSTGRES_PASSWORD) \
                   .option("driver", "org.postgresql.Driver") \
                   .mode("overwrite") \
                   .save()
                   
                # Get primary key from config or use the first column
                primary_key = pipeline_config.get("primary_key", result_df.columns[0])
                
                # Create temporary table if it doesn't exist
                create_table_if_not_exists(
                    temp_table, 
                    schema_dict,
                    schema_name="gold"
                )
                
                # Execute merge SQL using incremental condition
                merge_sql = f"""
                    BEGIN;
                    -- Insert new records or update existing ones
                    INSERT INTO gold.{destination}
                    SELECT * FROM gold.{temp_table}
                    ON CONFLICT ({primary_key})
                    DO UPDATE SET 
                        {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in result_df.columns if col != primary_key])};
                    
                    -- Clean up temp table
                    DROP TABLE IF EXISTS gold.{temp_table};
                    COMMIT;
                """
                execute_sql(merge_sql)
                logger.info(f"Successfully merged data into gold.{destination} (incremental load)")
            else:
                # Full load - delete and insert
                logger.info(f"Full load: Replacing all data in gold.{destination}")
                execute_sql(f"DELETE FROM gold.{destination}")
                
                # Write DataFrame to PostgreSQL
                result_df.write \
                   .format("jdbc") \
                   .option("url", jdbc_url) \
                   .option("dbtable", f"gold.{destination}") \
                   .option("user", POSTGRES_USER) \
                   .option("password", POSTGRES_PASSWORD) \
                   .option("driver", "org.postgresql.Driver") \
                   .mode("append") \
                   .save()
                   
                logger.info(f"Successfully wrote data to gold.{destination} via JDBC (full load)")
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
                    pipeline_id=pipeline_config.get("id", destination),
                    source_name=source_name,
                    destination_name=f"gold.{destination}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return result_df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error transforming data to gold layer: {error_msg}")
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                source_name = ", ".join(f"silver.{s}" for s in sources)
                duration = (auditor.end_time - auditor.start_time).total_seconds() if auditor.end_time else 0
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=pipeline_config.get("id", destination),
                    source_name=source_name,
                    destination_name=f"gold.{destination}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise 