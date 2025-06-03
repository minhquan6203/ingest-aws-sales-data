"""
Main script for the metadata-driven ETL framework demo
"""
import os
import sys
import argparse
import subprocess
import traceback
from loguru import logger

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.controller.etl_controller import ETLController
from src.config.metadata import PIPELINES, DATA_SOURCES
from src.audit.audit import get_pipeline_execution_history, ETLAuditor


def setup_logging():
    """
    Set up logging configuration
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="DEBUG")
    logger.add("logs/etl_{time}.log", rotation="500 MB", level="DEBUG")


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Metadata-driven ETL Framework Demo")
    
    parser.add_argument(
        "--run-pipeline",
        help="Run a specific pipeline by ID",
    )
    
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run all pipelines"
    )
    
    parser.add_argument(
        "--list-pipelines",
        action="store_true",
        help="List all available pipelines"
    )
    
    parser.add_argument(
        "--show-history",
        action="store_true",
        help="Show pipeline execution history"
    )
    
    parser.add_argument(
        "--schedule",
        help="Schedule a pipeline to run at regular intervals (pipeline ID)"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Interval in seconds for scheduled runs (default: 3600)"
    )
    
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Run pipelines sequentially instead of in parallel"
    )
    
    # Add incremental/full load option
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Perform a full load instead of incremental load"
    )
    
    # Add test-audit argument
    parser.add_argument(
        "--test-audit",
        action="store_true",
        help="Test audit functionality by creating a test record"
    )

    # Add run-aws-sales argument
    parser.add_argument(
        "--run-aws-sales",
        action="store_true",
        help="Run the AWS sales data pipeline"
    )
    
    # Add AWS sales schema display
    parser.add_argument(
        "--show-schema",
        action="store_true",
        help="Show the AWS sales dimensional schema details"
    )
    
    # Add test ingestion option
    parser.add_argument(
        "--test-ingestion",
        action="store_true",
        help="Test data ingestion without running the full pipeline"
    )
    
    return parser.parse_args()


def list_pipelines():
    """
    List all available pipelines
    """
    print("\nAvailable Pipelines:")
    print("===================")
    
    for pid, config in PIPELINES.items():
        pipeline_type = config.get("type", "bronze_to_silver")
        
        if pipeline_type == "gold":
            sources = ", ".join(config.get("sources", []))
            destination = config.get("destination", "")
            print(f"ID: {pid} (Gold) - Sources: [{sources}], Destination: {destination}")
        else:
            source = config.get("source", "")
            destination = config.get("destination", "")
            print(f"ID: {pid} (Bronze to Silver) - Source: {source}, Destination: {destination}")
    
    print()


def show_execution_history():
    """
    Show pipeline execution history
    """
    history = get_pipeline_execution_history(limit=20)
    
    if not history:
        print("\nNo pipeline execution history found.")
        return
    
    print("\nPipeline Execution History:")
    print("==========================")
    
    for record in history:
        audit_id, pipeline_id, source, destination, start_time, end_time, records, status, error, load_type, metadata = record
        
        duration = "N/A"
        if end_time and start_time:
            duration = f"{(end_time - start_time).total_seconds():.2f}s"
        
        # Parse metadata if available
        metadata_info = ""
        if metadata:
            try:
                import json
                meta_dict = json.loads(metadata)
                if "incremental_column" in meta_dict and "last_timestamp" in meta_dict:
                    metadata_info = f"\n  Incremental on: {meta_dict['incremental_column']}"
                    if meta_dict.get("last_timestamp"):
                        metadata_info += f" (from: {meta_dict['last_timestamp']})"
            except Exception as e:
                logger.error(f"Error parsing metadata: {e}")
        
        print(f"Pipeline: {pipeline_id}")
        print(f"  Load Type: {load_type or 'full'}")
        print(f"  Status: {status}")
        print(f"  Start: {start_time}")
        print(f"  Duration: {duration}")
        print(f"  Records: {records or 0}")
        
        if metadata_info:
            print(metadata_info)
        
        if status == "FAILED" and error:
            print(f"  Error: {error}")
        
        print()


def show_aws_sales_schema():
    """
    Show the AWS sales dimensional schema details
    """
    print("\nAWS Sales Data Dimensional Model")
    print("==============================\n")
    
    print("Dimension Tables:")
    print("----------------")
    print("1. gold_dim_salesperson - Salesperson dimension")
    print("   - salesperson_id (PK)")
    print("   - salesperson_name\n")
    
    print("2. gold_dim_lead - Lead/Customer dimension")
    print("   - lead_id (PK)")
    print("   - lead_name")
    print("   - segment")
    print("   - region\n")
    
    print("3. gold_dim_date - Date dimension")
    print("   - date_id (PK)")
    print("   - date")
    print("   - year")
    print("   - month")
    print("   - day")
    print("   - quarter")
    print("   - day_of_week\n")
    
    print("4. gold_dim_opportunity_stage - Opportunity Stage dimension")
    print("   - stage_id (PK)")
    print("   - stage_name\n")
    
    print("Fact Tables:")
    print("-----------")
    print("1. gold_fact_sales - Sales Fact table")
    print("   - sales_id (PK)")
    print("   - date_id (FK)")
    print("   - lead_id (FK)")
    print("   - salesperson_id (FK)")
    print("   - stage_id (FK)")
    print("   - target_close_date")
    print("   - forecasted_monthly_revenue")
    print("   - weighted_revenue")
    print("   - closed_opportunity")
    print("   - active_opportunity")
    print("   - latest_status_entry")
    print("   - sales_cycle_days\n")
    
    print("Aggregated Views:")
    print("----------------")
    print("1. gold_aws_sales_summary - Sales summary by salesperson")
    print("2. gold_aws_sales_by_region - Sales summary by region\n")


def test_data_ingestion():
    """
    Test AWS sales data ingestion without running the full pipeline
    """
    logger.info("Testing data ingestion...")
    
    # Check if SalesData.csv exists
    if not os.path.exists("data/SalesData.csv"):
        logger.warning("SalesData.csv not found in data directory")
        
        # Create data directory if it doesn't exist
        os.makedirs("data", exist_ok=True)
        
        # Try to download the file
        logger.info("Attempting to download SalesData.csv...")
        download_cmd = [
            "python", "-c", 
            "import requests; open('data/SalesData.csv', 'wb').write(requests.get('https://raw.githubusercontent.com/aws-samples/data-engineering-on-aws/main/dataset/SalesData.csv').content)"
        ]
        subprocess.run(download_cmd, check=True)
        logger.info("Successfully downloaded SalesData.csv")
    
    # Create a test script to verify CSV reading
    test_script = """
import sys
from pyspark.sql import SparkSession
from src.utils.spark_utils import create_spark_session, read_csv_to_dataframe
from src.config.metadata import DATA_SOURCES

# Get AWS sales data config
source_config = DATA_SOURCES["aws_sales_data"]
source_path = source_config.get("source_path")
schema_dict = source_config.get("schema", {})

# Create SparkSession
spark = create_spark_session()

# Read the CSV file
print(f"\\nReading CSV file: {source_path}")
df = read_csv_to_dataframe(spark, source_path)

# Show schema
print("\\nDataFrame Schema:")
df.printSchema()

# Show sample data
print("\\nSample Data:")
df.show(5, truncate=False)

# Show record count
count = df.count()
print(f"\\nTotal records: {count}")

# Verify column names
print("\\nColumn Names:")
for column in df.columns:
    print(f"  - {column}")

spark.stop()
"""
    
    # Write the test script to a temporary file
    with open("test_ingestion.py", "w") as f:
        f.write(test_script)
    
    # Run the test script
    logger.info("Running test script...")
    test_cmd = ["python", "test_ingestion.py"]
    subprocess.run(test_cmd, check=True)
    
    # Clean up
    os.remove("test_ingestion.py")
    logger.info("Test completed successfully")


def main():
    """
    Main function
    """
    # Set up logging
    setup_logging()
    
    # Parse command line arguments
    args = parse_args()
    
    # Determine load type (incremental by default)
    incremental = not args.full_load
    load_type = "full" if args.full_load else "incremental"
    
    # Show AWS sales schema if requested
    if args.show_schema:
        show_aws_sales_schema()
        return
    
    # Test data ingestion if requested
    if args.test_ingestion:
        try:
            test_data_ingestion()
        except Exception as e:
            logger.error(f"Error testing data ingestion: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
        return
    
    # Test audit functionality if requested
    if args.test_audit:
        logger.info("Running audit test")
        
        # Create test audit record
        auditor = ETLAuditor(
            pipeline_id="test_audit_pipeline",
            source_name="test_source",
            destination_name="test_destination"
        )
        
        # Update record count and complete
        auditor.set_records_processed(1000)
        auditor.complete_successfully(1000)
        
        print(f"Created and completed audit record with ID: {auditor.audit_id}")
        return
    
    if args.list_pipelines:
        list_pipelines()
        return
    
    if args.show_history:
        show_execution_history()
        return
    
    # Create ETL controller
    controller = ETLController()
    
    if args.run_pipeline:
        pipeline_id = args.run_pipeline
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline '{pipeline_id}' not found")
            return
        
        logger.info(f"Running pipeline: {pipeline_id} (load type: {load_type})")
        controller.run_pipeline(pipeline_id, incremental=incremental)
        return
    
    if args.run_aws_sales:
        logger.info(f"Running AWS sales pipelines (load type: {load_type})")
        
        # Check if SalesData.csv exists
        if not os.path.exists("data/SalesData.csv"):
            logger.warning("SalesData.csv not found in data directory")
            
            # Create data directory if it doesn't exist
            os.makedirs("data", exist_ok=True)
            
            # Try to download the file
            logger.info("Attempting to download SalesData.csv...")
            download_cmd = [
                "python", "-c", 
                "import requests; open('data/SalesData.csv', 'wb').write(requests.get('https://raw.githubusercontent.com/aws-samples/data-engineering-on-aws/main/dataset/SalesData.csv').content)"
            ]
            subprocess.run(download_cmd, check=True)
            logger.info("Successfully downloaded SalesData.csv")
        
        # Run bronze to silver pipeline
        logger.info("Step 1/7: Running bronze to silver transformation")
        controller.run_pipeline("aws_sales_pipeline", incremental=incremental)
        
        # Run dimension tables pipelines
        logger.info("Step 2/7: Creating salesperson dimension")
        controller.run_pipeline("aws_dim_salesperson_pipeline", incremental=incremental)
        
        logger.info("Step 3/7: Creating lead dimension")
        controller.run_pipeline("aws_dim_lead_pipeline", incremental=incremental)
        
        logger.info("Step 4/7: Creating date dimension")
        controller.run_pipeline("aws_dim_date_pipeline", incremental=incremental)
        
        logger.info("Step 5/7: Creating opportunity stage dimension")
        controller.run_pipeline("aws_dim_opportunity_stage_pipeline", incremental=incremental)
        
        # Run fact table pipeline
        logger.info("Step 6/7: Creating sales fact table")
        controller.run_pipeline("aws_fact_sales_pipeline", incremental=incremental)
        
        # Run aggregated views for reporting
        logger.info("Step 7/7: Creating aggregated views for reporting")
        controller.run_pipeline("aws_sales_summary_pipeline", incremental=incremental)
        controller.run_pipeline("aws_sales_region_pipeline", incremental=incremental)
        
        logger.info("AWS sales data pipeline completed successfully")
        logger.info("The following tables have been created:")
        logger.info("- Dimension tables: gold_dim_salesperson, gold_dim_lead, gold_dim_date, gold_dim_opportunity_stage")
        logger.info("- Fact table: gold_fact_sales")
        logger.info("- Aggregated views: gold_aws_sales_summary, gold_aws_sales_by_region")
        return
    
    if args.run_all:
        logger.info(f"Running all pipelines (load type: {load_type})")
        
        # Get list of pipeline IDs
        pipeline_ids = list(PIPELINES.keys())
        
        if args.sequential:
            # Run pipelines sequentially
            for pipeline_id in pipeline_ids:
                controller.run_pipeline(pipeline_id, incremental=incremental)
        else:
            # Run pipelines in parallel
            controller.run_all_pipelines(parallel=True, max_workers=3, incremental=incremental)
        
        return
    
    if args.schedule:
        pipeline_id = args.schedule
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline '{pipeline_id}' not found")
            return
        
        interval = args.interval
        logger.info(f"Scheduling pipeline '{pipeline_id}' to run every {interval} seconds")
        controller.schedule_pipeline(pipeline_id, interval, incremental=incremental)
        return
    
    # If no arguments, show help
    list_pipelines()
    print("Use --help for more information")


if __name__ == "__main__":
    main() 