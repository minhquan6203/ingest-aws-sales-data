"""
ETL Controller module for orchestrating the ETL process
"""
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config.metadata import DATA_SOURCES, PIPELINES
from src.ingestion.bronze_ingestion import ingest_to_bronze
from src.transformation.silver_transformation import transform_to_silver
from src.transformation.gold_transformation import transform_to_gold
from src.utils.storage_utils import init_minio, init_database, execute_sql
from src.notification.notification import ETLNotifier
from src.audit.audit import ETLAuditor


class ETLController:
    """
    Controller class for orchestrating the ETL process
    """
    
    def __init__(self):
        """
        Initialize the ETL controller
        """
        logger.info("Initializing ETL controller")
        
        # Initialize storage
        self._initialize_storage()
    
    def _initialize_storage(self):
        """
        Initialize storage (MinIO buckets and PostgreSQL schema)
        """
        try:
            # Initialize MinIO buckets
            init_minio()
            
            # Initialize PostgreSQL schema
            init_database()
            
            # Verify the audit table is accessible by inserting a test record
            self._verify_audit_table()
            
        except Exception as e:
            logger.error(f"Error initializing storage: {e}")
            raise
    
    def _verify_audit_table(self):
        """
        Verify the audit table is working by inserting and querying a test record
        """
        try:
            # Create a test auditor
            test_auditor = ETLAuditor(
                pipeline_id="etl_controller_test",
                source_name="controller_test",
                destination_name="test_destination"
            )
            
            # Update and verify the record exists
            test_auditor.set_records_processed(1)
            
            # Query to verify record exists
            sql = "SELECT COUNT(*) FROM public.etl_audit WHERE pipeline_id = 'etl_controller_test';"
            result = execute_sql(sql, fetch=True)
            count = result[0][0] if result and len(result) > 0 else 0
            
            if count > 0:
                logger.info(f"Audit table verification successful - found {count} test records")
            else:
                logger.error("Audit table verification failed - no test records found!")
                
        except Exception as e:
            logger.error(f"Error verifying audit table: {e}")
    
    def run_pipeline(self, pipeline_id, incremental=True):
        """
        Run a single pipeline
        
        Args:
            pipeline_id: ID of the pipeline to run
            incremental: Whether to use incremental loading (vs. full load)
            
        Returns:
            Boolean indicating success or failure
        """
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline {pipeline_id} not found")
            return False
        
        pipeline_config = PIPELINES[pipeline_id]
        load_type = "incremental" if incremental else "full"
        logger.info(f"Running pipeline: {pipeline_id} (load type: {load_type})")
        
        # Create audit record for this pipeline run
        source_name = "multiple_sources"
        destination_name = pipeline_config.get("destination", "unknown")
        
        if pipeline_config.get("type") != "gold":
            source_name = pipeline_config.get("source", "unknown")
            
        auditor = ETLAuditor(
            pipeline_id=pipeline_id,
            source_name=source_name,
            destination_name=destination_name,
            load_type=load_type
        )
        
        # Verify that the audit record was created
        if not auditor.audit_id:
            logger.error(f"Failed to create audit record for pipeline {pipeline_id}")
            # Continue anyway, as we don't want to fail the pipeline just because of audit issues
        else:
            logger.info(f"Created audit record {auditor.audit_id} for pipeline {pipeline_id}")
        
        try:
            # For a gold layer pipeline
            if pipeline_config.get("type") == "gold":
                # Transform data to gold layer
                result_df, record_count = transform_to_gold(pipeline_config, incremental=incremental)
                
                # Update audit record
                if auditor.audit_id:
                    auditor.complete_successfully(record_count)
                    
                return True
            
            # For bronze to silver pipeline
            source_name = pipeline_config.get("source")
            if source_name not in DATA_SOURCES:
                logger.error(f"Source {source_name} not found for pipeline {pipeline_id}")
                if auditor.audit_id:
                    auditor.complete_with_error(f"Source {source_name} not found")
                return False
            
            source_config = DATA_SOURCES[source_name]
            
            # Check if this source supports incremental loads
            supports_incremental = "incremental_column" in source_config and source_config["incremental_column"]
            if incremental and not supports_incremental:
                logger.warning(f"Source {source_name} does not support incremental loads, falling back to full load")
                incremental = False
            
            # Run bronze ingestion
            bronze_df, bronze_count = ingest_to_bronze(source_config, incremental=incremental)
            
            # Verify bronze ingestion worked
            if bronze_count == 0:
                logger.warning(f"Bronze ingestion for {source_name} returned 0 records")
                if auditor.audit_id:
                    if incremental:
                        logger.info(f"No new records to process for {source_name} (incremental load)")
                        auditor.complete_successfully(0)
                    else:
                        auditor.complete_with_error(f"Bronze ingestion returned 0 records")
                return True  # Still return success for incremental load with no new records
            
            # Update audit record with bronze count
            if auditor.audit_id and bronze_count > 0:
                auditor.set_records_processed(bronze_count)
                if incremental:
                    logger.info(f"Incremental load processing {bronze_count} new records for {source_name}")
            
            # Run silver transformation
            silver_df, silver_count = transform_to_silver(pipeline_config, source_config, incremental=incremental)
            
            # Update audit record with final completion
            if auditor.audit_id:
                auditor.complete_successfully(silver_count)
            
            logger.info(f"Pipeline {pipeline_id} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error running pipeline {pipeline_id}: {e}")
            
            # Mark audit record as failed
            if auditor.audit_id:
                auditor.complete_with_error(str(e))
                
            return False
    
    def run_all_pipelines(self, parallel=True, max_workers=3, incremental=True):
        """
        Run all pipelines
        
        Args:
            parallel: Whether to run pipelines in parallel
            max_workers: Maximum number of worker threads for parallel execution
            incremental: Whether to use incremental loading (vs. full load)
            
        Returns:
            Dictionary mapping pipeline IDs to success/failure status
        """
        load_type = "incremental" if incremental else "full"
        logger.info(f"Running all pipelines (parallel={parallel}, load_type={load_type})")
        
        # Get all pipeline IDs
        all_pipeline_ids = list(PIPELINES.keys())
        
        # Determine execution order - gold pipelines should run after silver
        bronze_silver_pipelines = [pid for pid in all_pipeline_ids 
                                  if PIPELINES[pid].get("type") != "gold"]
        gold_pipelines = [pid for pid in all_pipeline_ids 
                         if PIPELINES[pid].get("type") == "gold"]
        
        pipeline_results = {}
        
        # Run bronze to silver pipelines
        if parallel and len(bronze_silver_pipelines) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_pipeline = {
                    executor.submit(self.run_pipeline, pid, incremental): pid 
                    for pid in bronze_silver_pipelines
                }
                
                for future in as_completed(future_to_pipeline):
                    pid = future_to_pipeline[future]
                    try:
                        result = future.result()
                        pipeline_results[pid] = result
                    except Exception as e:
                        logger.error(f"Pipeline {pid} generated an exception: {e}")
                        pipeline_results[pid] = False
        else:
            for pid in bronze_silver_pipelines:
                pipeline_results[pid] = self.run_pipeline(pid, incremental)
        
        # Run gold pipelines (always sequential to ensure dependencies are met)
        for pid in gold_pipelines:
            # Run gold pipelines with the specified incremental mode
            pipeline_results[pid] = self.run_pipeline(pid, incremental=incremental)
        
        # Log summary
        successful = sum(1 for result in pipeline_results.values() if result)
        failed = len(pipeline_results) - successful
        
        logger.info(f"Pipeline execution summary: {successful} successful, {failed} failed")
        
        if failed > 0:
            failed_pipelines = [pid for pid, result in pipeline_results.items() if not result]
            logger.error(f"Failed pipelines: {', '.join(failed_pipelines)}")
        
        return pipeline_results
    
    def schedule_pipeline(self, pipeline_id, interval_seconds=3600, incremental=True):
        """
        Schedule a pipeline to run at regular intervals
        
        Args:
            pipeline_id: ID of the pipeline to schedule
            interval_seconds: Interval between runs in seconds
            incremental: Whether to use incremental loading (vs. full load)
            
        Note: This is a simplified implementation for demo purposes.
              In production, use a proper scheduler like Airflow.
        """
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline {pipeline_id} not found")
            return False
        
        load_type = "incremental" if incremental else "full"
        logger.info(f"Scheduling pipeline {pipeline_id} to run every {interval_seconds} seconds (load type: {load_type})")
        
        try:
            while True:
                success = self.run_pipeline(pipeline_id, incremental=incremental)
                logger.info(f"Scheduled run of pipeline {pipeline_id} {'succeeded' if success else 'failed'}")
                
                # Sleep until next run
                logger.info(f"Next run of pipeline {pipeline_id} in {interval_seconds} seconds")
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info(f"Pipeline scheduling for {pipeline_id} interrupted by user")
            return True
        except Exception as e:
            logger.error(f"Error in scheduled execution of pipeline {pipeline_id}: {e}")
            return False 