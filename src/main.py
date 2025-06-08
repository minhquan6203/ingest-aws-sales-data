"""
AWS Sales Data ETL Pipeline
==========================

A comprehensive metadata-driven ETL framework for processing AWS sales data
using a 3-layer architecture (Bronze → Silver → Gold).

Features:
- Incremental and full load processing
- Dimensional modeling with fact/dimension tables
- Advanced analytics and reporting
- Comprehensive audit trail
- Docker containerization
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
    """Configure logging for the ETL framework."""
    os.makedirs("logs", exist_ok=True)
    
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="DEBUG")
    logger.add("logs/etl_{time}.log", rotation="500 MB", level="DEBUG")


def parse_args():
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(
        description="AWS Sales Data ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --run-aws-sales              # Run standard AWS sales pipeline
  %(prog)s --run-enhanced-aws-sales     # Run enhanced pipeline with analytics
  %(prog)s --run-analytics              # Generate analytics dashboard
  %(prog)s --run-pipeline sales_pipeline --full-load  # Run specific pipeline with full load
  %(prog)s --list-pipelines             # Show all available pipelines
  %(prog)s --show-history               # Show execution history
  
Data Exploration:
  %(prog)s --show-all-tables            # Show overview of all warehouse tables
  %(prog)s --show-bronze-data           # Show Bronze layer data
  %(prog)s --show-silver-data           # Show Silver layer data
  %(prog)s --show-gold-data             # Show Gold layer data
  %(prog)s --show-table gold_fact_sales # Show specific table data
  %(prog)s --show-table gold_fact_sales --limit 20  # Show 20 rows
        """
    )
    
    # Pipeline execution options
    execution_group = parser.add_argument_group('Pipeline Execution')
    execution_group.add_argument(
        "--run-aws-sales", action="store_true",
        help="Run the complete AWS sales data pipeline (8 steps)"
    )
    execution_group.add_argument(
        "--run-enhanced-aws-sales", action="store_true", 
        help="Run enhanced AWS sales pipeline with advanced analytics (11 steps)"
    )
    execution_group.add_argument(
        "--run-pipeline",
        help="Run a specific pipeline by ID"
    )
    execution_group.add_argument(
        "--run-all", action="store_true",
        help="Run all configured pipelines"
    )
    execution_group.add_argument(
        "--run-analytics-pipeline",
        choices=["performance", "funnel", "trends", "scoring"],
        help="Run a specific analytics pipeline"
    )
    
    # Analytics and reporting
    analytics_group = parser.add_argument_group('Analytics & Reporting')
    analytics_group.add_argument(
        "--run-analytics", action="store_true",
        help="Generate and export analytics dashboard"
    )
    analytics_group.add_argument(
        "--show-schema", action="store_true",
        help="Display the AWS sales dimensional schema"
    )
    
    # Information and monitoring
    info_group = parser.add_argument_group('Information & Monitoring')
    info_group.add_argument(
        "--list-pipelines", action="store_true",
        help="List all available pipelines"
    )
    info_group.add_argument(
        "--show-history", action="store_true",
        help="Show pipeline execution history"
    )
    
    # Load options
    load_group = parser.add_argument_group('Load Options')
    load_group.add_argument(
        "--full-load", action="store_true",
        help="Perform full load instead of incremental load"
    )
    load_group.add_argument(
        "--sequential", action="store_true",
        help="Run pipelines sequentially instead of parallel (for --run-all)"
    )
    
    # Scheduling (advanced)
    schedule_group = parser.add_argument_group('Scheduling')
    schedule_group.add_argument(
        "--schedule",
        help="Schedule a pipeline to run at regular intervals (pipeline ID)"
    )
    schedule_group.add_argument(
        "--interval", type=int, default=3600,
        help="Interval in seconds for scheduled runs (default: 3600)"
    )
    
    # Testing and development
    test_group = parser.add_argument_group('Testing & Development')
    test_group.add_argument(
        "--test-ingestion", action="store_true",
        help="Test data ingestion without running full pipeline"
    )
    test_group.add_argument(
        "--test-audit", action="store_true",
        help="Test audit functionality"
    )
    
    # Data exploration
    data_group = parser.add_argument_group('Data Exploration')
    data_group.add_argument(
        "--show-bronze-data", action="store_true",
        help="Show data from Bronze layer tables"
    )
    data_group.add_argument(
        "--show-silver-data", action="store_true",
        help="Show data from Silver layer tables"
    )
    data_group.add_argument(
        "--show-gold-data", action="store_true",
        help="Show data from Gold layer tables"
    )
    data_group.add_argument(
        "--show-table",
        help="Show data from a specific table (e.g., gold_fact_sales)"
    )
    data_group.add_argument(
        "--show-all-tables", action="store_true",
        help="Show overview of all tables in the warehouse"
    )
    data_group.add_argument(
        "--limit", type=int, default=10,
        help="Number of rows to display (default: 10)"
    )

    return parser.parse_args()


def ensure_sales_data():
    """Ensure SalesData.csv exists, download if necessary."""
    if not os.path.exists("data/SalesData.csv"):
        logger.warning("SalesData.csv not found in data directory")
        
        os.makedirs("data", exist_ok=True)
        
        logger.info("Downloading SalesData.csv...")
        download_cmd = [
            "python", "-c", 
            "import requests; open('data/SalesData.csv', 'wb').write(requests.get('https://raw.githubusercontent.com/aws-samples/data-engineering-on-aws/main/dataset/SalesData.csv').content)"
        ]
        subprocess.run(download_cmd, check=True)
        logger.info("Successfully downloaded SalesData.csv")


def list_pipelines():
    """Display all available pipelines."""
    print("\n🔧 Available Pipelines")
    print("=" * 50)
    
    for pid, config in PIPELINES.items():
        pipeline_type = config.get("type", "bronze_to_silver")
        
        if pipeline_type == "gold":
            sources = ", ".join(config.get("sources", []))
            destination = config.get("destination", "")
            print(f"📊 {pid} (Gold Layer)")
            print(f"   Sources: [{sources}]")
            print(f"   Destination: {destination}")
        else:
            source = config.get("source", "")
            destination = config.get("destination", "")
            print(f"🔄 {pid} (Bronze → Silver)")
            print(f"   Source: {source}")
            print(f"   Destination: {destination}")
    print()


def show_execution_history():
    """Display pipeline execution history."""
    history = get_pipeline_execution_history(limit=20)
    
    if not history:
        print("\n📝 No pipeline execution history found.")
        return
    
    print("\n📈 Pipeline Execution History")
    print("=" * 50)
    
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
                    metadata_info = f"   Incremental: {meta_dict['incremental_column']}"
                    if meta_dict.get("last_timestamp"):
                        metadata_info += f" (from: {meta_dict['last_timestamp']})"
            except Exception as e:
                logger.error(f"Error parsing metadata: {e}")
        
        status_icon = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "🔄"
        
        print(f"{status_icon} {pipeline_id}")
        print(f"   Load Type: {load_type or 'full'}")
        print(f"   Duration: {duration}")
        print(f"   Records: {records or 0}")
        print(f"   Started: {start_time}")
        
        if metadata_info:
            print(metadata_info)
        
        if status == "FAILED" and error:
            print(f"   ❌ Error: {error}")
        
        print()


def show_aws_sales_schema():
    """Display the AWS sales dimensional schema."""
    print("\n📊 AWS Sales Data Dimensional Model")
    print("=" * 50)
    
    print("\n🏷️  Dimension Tables:")
    print("-" * 25)
    dimensions = [
        ("gold_dim_salesperson", "Salesperson dimension", ["salesperson_id (PK)", "salesperson_name"]),
        ("gold_dim_lead", "Lead/Customer dimension", ["lead_id (PK)", "lead_name", "segment", "region"]),
        ("gold_dim_date", "Date dimension", ["date_id (PK)", "date", "year", "month", "day", "quarter", "day_of_week"]),
        ("gold_dim_opportunity_stage", "Opportunity Stage dimension", ["stage_id (PK)", "stage_name"])
    ]
    
    for table, desc, fields in dimensions:
        print(f"📋 {table} - {desc}")
        for field in fields:
            print(f"   • {field}")
        print()
    
    print("📈 Fact Tables:")
    print("-" * 15)
    print("📊 gold_fact_sales - Sales Fact table")
    fact_fields = [
        "sales_id (PK)", "date_id (FK)", "lead_id (FK)", "salesperson_id (FK)",
        "stage_id (FK)", "target_close_date", "forecasted_monthly_revenue",
        "weighted_revenue", "closed_opportunity", "active_opportunity",
        "latest_status_entry", "sales_cycle_days"
    ]
    for field in fact_fields:
        print(f"   • {field}")
    print()
    
    print("📊 Aggregated Views:")
    print("-" * 20)
    print("📈 gold_aws_sales_summary - Sales summary by salesperson")
    print("🌍 gold_aws_sales_by_region - Sales summary by region")
    print()


def test_data_ingestion():
    """Test AWS sales data ingestion."""
    logger.info("🧪 Testing data ingestion...")
    
    ensure_sales_data()
    
    test_script = '''
import sys
from pyspark.sql import SparkSession
from src.utils.spark_utils import create_spark_session, read_csv_to_dataframe
from src.config.metadata import DATA_SOURCES

# Get AWS sales data config
source_config = DATA_SOURCES["aws_sales_data"]
source_path = source_config.get("source_path")

# Create SparkSession
spark = create_spark_session()

# Read the CSV file
print(f"\\n📁 Reading CSV file: {source_path}")
df = read_csv_to_dataframe(spark, source_path)

# Show schema
print("\\n📋 DataFrame Schema:")
df.printSchema()

# Show sample data
print("\\n📊 Sample Data:")
df.show(5, truncate=False)

# Show record count
count = df.count()
print(f"\\n📊 Total records: {count}")

# Verify column names
print("\\n📋 Column Names:")
for column in df.columns:
    print(f"  • {column}")

spark.stop()
'''
    
    with open("test_ingestion.py", "w") as f:
        f.write(test_script)
    
    logger.info("🚀 Running test script...")
    subprocess.run(["python", "test_ingestion.py"], check=True)
    
    os.remove("test_ingestion.py")
    logger.info("✅ Test completed successfully")


def show_warehouse_data(layer=None, table_name=None, limit=10):
    """Show data from warehouse tables."""
    try:
        from src.utils.storage_utils import get_postgres_connection
        import pandas as pd
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        if table_name:
            # Show specific table
            logger.info(f"📊 Showing data from table: {table_name}")
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            
            if not cursor.fetchone()[0]:
                print(f"❌ Table '{table_name}' not found in database")
                return
            
            # Get table info
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))
            rows = cursor.fetchall()
            
            # Get column names
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
            columns = [row[0] for row in cursor.fetchall()]
            
            print(f"\n📋 Table: {table_name}")
            print(f"📊 Total rows: {total_rows:,}")
            print(f"📝 Showing first {len(rows)} rows:\n")
            
            if rows:
                df = pd.DataFrame(rows, columns=columns)
                print(df.to_string(index=False, max_cols=10, max_colwidth=50))
            else:
                print("No data found in table")
            
        else:
            # Show tables by layer
            layer_patterns = {
                'bronze': 'bronze_%',
                'silver': 'silver_%', 
                'gold': 'gold_%'
            }
            
            pattern = layer_patterns.get(layer, '%')
            
            cursor.execute("""
                SELECT table_schema || '.' || table_name as full_table_name, table_name
                FROM information_schema.tables 
                WHERE table_schema IN ('public', 'bronze', 'silver', 'gold')
                AND (
                    (table_schema = %s) OR 
                    (table_name LIKE %s)
                )
                ORDER BY table_name
            """, (layer, pattern,))
            
            table_results = cursor.fetchall()
            tables = [(row[0], row[1]) for row in table_results]  # (full_table_name, table_name)
            
            if not tables:
                print(f"❌ No tables found for {layer} layer")
                return
            
            print(f"\n📊 {layer.upper()} Layer Tables")
            print("=" * 50)
            
            for full_table_name, table_name in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                
                # Get all column names
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))
                all_cols = [row[0] for row in cursor.fetchall()]
                col_preview = ", ".join(all_cols[:3]) + ("..." if len(all_cols) > 3 else "")
                
                print(f"📋 {table_name}")
                print(f"   📊 Rows: {count:,}")
                print(f"   📝 Columns: {col_preview}")
                
                if count > 0:
                    # Show sample data - limit to first 5 columns for display
                    cols_for_display = all_cols[:5]
                    col_list = ", ".join([f'"{col}"' for col in cols_for_display])
                    cursor.execute(f"SELECT {col_list} FROM {full_table_name} LIMIT 3")
                    sample_rows = cursor.fetchall()
                    
                    if sample_rows:
                        print(f"   📄 Sample (3 rows, showing first {len(cols_for_display)} columns):")
                        df = pd.DataFrame(sample_rows, columns=cols_for_display)
                        sample_str = df.head(3).to_string(index=False, max_cols=5, max_colwidth=30)
                        for line in sample_str.split('\n'):
                            print(f"      {line}")
                
                print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error showing warehouse data: {e}")
        import traceback
        logger.error(traceback.format_exc())


def show_all_tables():
    """Show all tables in the warehouse with summary."""
    try:
        from src.utils.storage_utils import get_postgres_connection
        
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        # Get all tables with row counts from all schemas
        cursor.execute("""
            SELECT 
                table_schema || '.' || table_name as full_table_name,
                table_name,
                CASE 
                    WHEN table_schema = 'bronze' OR table_name LIKE 'bronze_%' THEN 'Bronze'
                    WHEN table_schema = 'silver' OR table_name LIKE 'silver_%' THEN 'Silver' 
                    WHEN table_schema = 'gold' OR table_name LIKE 'gold_%' THEN 'Gold'
                    ELSE 'Other'
                END as layer
            FROM information_schema.tables 
            WHERE table_schema IN ('public', 'bronze', 'silver', 'gold')
            AND table_type = 'BASE TABLE'
            ORDER BY 
                CASE 
                    WHEN table_schema = 'bronze' OR table_name LIKE 'bronze_%' THEN 1
                    WHEN table_schema = 'silver' OR table_name LIKE 'silver_%' THEN 2 
                    WHEN table_schema = 'gold' OR table_name LIKE 'gold_%' THEN 3
                    ELSE 4
                END, table_name
        """)
        
        tables = cursor.fetchall()
        
        if not tables:
            print("❌ No tables found in database")
            return
        
        print("\n🏢 Data Warehouse Overview")
        print("=" * 60)
        
        current_layer = None
        for full_table_name, table_name, layer in tables:
            if layer != current_layer:
                current_layer = layer
                layer_icons = {'Bronze': '🥉', 'Silver': '🥈', 'Gold': '🥇', 'Other': '📊'}
                print(f"\n{layer_icons.get(layer, '📊')} {layer} Layer:")
                print("-" * 30)
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                count = cursor.fetchone()[0]
                print(f"  📋 {table_name:<35} 📊 {count:>8,} rows")
            except Exception as e:
                print(f"  📋 {table_name:<35} ❌ Error: {str(e)}")
        
        print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error showing all tables: {e}")


def run_aws_sales_pipeline(controller, incremental):
    """Run the standard AWS sales pipeline (8 steps)."""
    load_type = "incremental" if incremental else "full"
    logger.info(f"🚀 Running AWS sales pipelines (load type: {load_type})")
    
    ensure_sales_data()
    
    steps = [
        ("aws_sales_pipeline", "Bronze to silver transformation"),
        ("aws_dim_salesperson_pipeline", "Creating salesperson dimension"),
        ("aws_dim_lead_pipeline", "Creating lead dimension"),
        ("aws_dim_date_pipeline", "Creating date dimension"),
        ("aws_dim_opportunity_stage_pipeline", "Creating opportunity stage dimension"),
        ("aws_fact_sales_pipeline", "Creating sales fact table"),
        ("aws_sales_summary_pipeline", "Creating sales summary view"),
        ("aws_sales_region_pipeline", "Creating regional sales view")
    ]
    
    for i, (pipeline_id, description) in enumerate(steps, 1):
        logger.info(f"Step {i}/8: {description}")
        controller.run_pipeline(pipeline_id, incremental=incremental)
    
    logger.info("✅ AWS sales data pipeline completed successfully")
    logger.info("📊 Created tables:")
    logger.info("   • Dimensions: gold_dim_salesperson, gold_dim_lead, gold_dim_date, gold_dim_opportunity_stage")
    logger.info("   • Fact: gold_fact_sales")
    logger.info("   • Views: gold_aws_sales_summary, gold_aws_sales_by_region")


def run_enhanced_aws_sales_pipeline(controller, incremental):
    """Run the enhanced AWS sales pipeline with analytics (11 steps)."""
    load_type = "incremental" if incremental else "full"
    logger.info(f"🚀 Running enhanced AWS sales pipelines with advanced analytics (load type: {load_type})")
    
    ensure_sales_data()
    
    # Run base pipeline first (8 steps)
    base_steps = [
        ("aws_sales_pipeline", "Bronze to silver transformation"),
        ("aws_dim_salesperson_pipeline", "Creating salesperson dimension"),
        ("aws_dim_lead_pipeline", "Creating lead dimension"),
        ("aws_dim_date_pipeline", "Creating date dimension"),
        ("aws_dim_opportunity_stage_pipeline", "Creating opportunity stage dimension"),
        ("aws_fact_sales_pipeline", "Creating sales fact table"),
        ("aws_sales_summary_pipeline", "Creating sales summary"),
        ("aws_sales_region_pipeline", "Creating regional sales view")
    ]
    
    for i, (pipeline_id, description) in enumerate(base_steps, 1):
        logger.info(f"Step {i}/11: {description}")
        controller.run_pipeline(pipeline_id, incremental=incremental)
    
    # Run advanced analytics (3 additional steps)
    analytics_steps = [
        ("aws_sales_performance_pipeline", "Creating sales performance metrics"),
        ("aws_opportunity_funnel_pipeline", "Creating opportunity funnel analysis"),
        ("aws_monthly_trends_pipeline", "Creating monthly trends analysis"),
        ("aws_lead_scoring_pipeline", "Creating lead scoring analysis")
    ]
    
    for i, (pipeline_id, description) in enumerate(analytics_steps, 9):
        logger.info(f"Step {i}/11: {description}")
        controller.run_pipeline(pipeline_id, incremental=incremental)
    
    logger.info("✅ Enhanced AWS sales data pipeline completed successfully")
    logger.info("📊 Created tables:")
    logger.info("   • Dimensions: gold_dim_salesperson, gold_dim_lead, gold_dim_date, gold_dim_opportunity_stage")
    logger.info("   • Fact: gold_fact_sales")
    logger.info("   • Views: gold_aws_sales_summary, gold_aws_sales_by_region")
    logger.info("   • Analytics: gold_sales_performance_metrics, gold_opportunity_funnel")
    logger.info("   • Trends: gold_monthly_sales_trends, gold_lead_scoring")
    
    # Generate analytics dashboard
    logger.info("📊 Generating analytics dashboard...")
    try:
        from src.analytics.dashboard import SalesAnalyticsDashboard
        dashboard = SalesAnalyticsDashboard()
        dashboard.generate_executive_summary()
        dashboard.export_to_csv()
        dashboard.close()
        logger.info("✅ Analytics dashboard generated successfully")
    except Exception as e:
        logger.error(f"❌ Error generating analytics dashboard: {e}")


def main():
    """Main entry point for the ETL framework."""
    setup_logging()
    args = parse_args()
    
    # Determine load type
    incremental = not args.full_load
    load_type = "full" if args.full_load else "incremental"
    
    logger.info("🚀 Starting AWS Sales Data ETL Pipeline")
    logger.info(f"⚙️  Load type: {load_type}")
    
    try:
        # Information commands (no controller needed)
        if args.show_schema:
            show_aws_sales_schema()
            return
        
        if args.list_pipelines:
            list_pipelines()
            return
        
        if args.show_history:
            show_execution_history()
            return
        
        # Data exploration commands
        if args.show_bronze_data:
            show_warehouse_data(layer='bronze', limit=args.limit)
            return
        
        if args.show_silver_data:
            show_warehouse_data(layer='silver', limit=args.limit)
            return
        
        if args.show_gold_data:
            show_warehouse_data(layer='gold', limit=args.limit)
            return
        
        if args.show_table:
            show_warehouse_data(table_name=args.show_table, limit=args.limit)
            return
        
        if args.show_all_tables:
            show_all_tables()
            return
        
        # Test commands
        if args.test_ingestion:
            test_data_ingestion()
            return
        
        if args.test_audit:
            logger.info("🧪 Running audit test")
            auditor = ETLAuditor(
                pipeline_id="test_audit_pipeline",
                source_name="test_source", 
                destination_name="test_destination"
            )
            auditor.set_records_processed(1000)
            auditor.complete_successfully(1000)
            logger.info(f"✅ Created audit record with ID: {auditor.audit_id}")
            return
        
        # Analytics dashboard
        if args.run_analytics:
            from src.analytics.dashboard import SalesAnalyticsDashboard
            dashboard = SalesAnalyticsDashboard()
            dashboard.generate_executive_summary()
            dashboard.export_to_csv()
            dashboard.close()
            logger.info("✅ Analytics dashboard completed successfully")
            return
        
        # Create ETL controller for pipeline operations
        controller = ETLController()
        
        # Pipeline execution commands
        if args.run_aws_sales:
            run_aws_sales_pipeline(controller, incremental)
            return
        
        if args.run_enhanced_aws_sales:
            run_enhanced_aws_sales_pipeline(controller, incremental)
            return
        
        if args.run_pipeline:
            pipeline_id = args.run_pipeline
            if pipeline_id not in PIPELINES:
                logger.error(f"❌ Pipeline '{pipeline_id}' not found")
                list_pipelines()
                return
            
            logger.info(f"🚀 Running pipeline: {pipeline_id} (load type: {load_type})")
            controller.run_pipeline(pipeline_id, incremental=incremental)
            return
        
        if args.run_analytics_pipeline:
            analytics_pipelines = {
                "performance": "aws_sales_performance_pipeline",
                "funnel": "aws_opportunity_funnel_pipeline", 
                "trends": "aws_monthly_trends_pipeline",
                "scoring": "aws_lead_scoring_pipeline"
            }
            
            pipeline_name = analytics_pipelines.get(args.run_analytics_pipeline)
            logger.info(f"📊 Running analytics pipeline: {pipeline_name}")
            controller.run_pipeline(pipeline_name, incremental=incremental)
            return
        
        if args.run_all:
            logger.info(f"🚀 Running all pipelines (load type: {load_type})")
            pipeline_ids = list(PIPELINES.keys())
            
            if args.sequential:
                for pipeline_id in pipeline_ids:
                    controller.run_pipeline(pipeline_id, incremental=incremental)
            else:
                controller.run_all_pipelines(parallel=True, max_workers=3, incremental=incremental)
            return
        
        if args.schedule:
            pipeline_id = args.schedule
            if pipeline_id not in PIPELINES:
                logger.error(f"❌ Pipeline '{pipeline_id}' not found")
                list_pipelines()
                return
            
            interval = args.interval
            logger.info(f"⏰ Scheduling pipeline '{pipeline_id}' to run every {interval} seconds")
            controller.schedule_pipeline(pipeline_id, interval, incremental=incremental)
            return
        
        # Default: show help
        print("🔧 AWS Sales Data ETL Pipeline")
        print("=" * 40)
        print("No arguments provided. Use --help for usage information.")
        print("\n💡 Quick start:")
        print("   python src/main.py --run-aws-sales")
        print("   python src/main.py --run-enhanced-aws-sales")
        print("   python src/main.py --list-pipelines")
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main() 