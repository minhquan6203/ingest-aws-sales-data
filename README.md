# AWS Sales Data ETL Pipeline

This project implements a 3-layer data ingestion architecture (Bronze, Silver, Gold) for processing AWS Sales Data, including a proper dimensional model in the Gold layer.

## Architecture

1. **Bronze Layer**: Raw data ingestion from CSV files
2. **Silver Layer**: Cleaned and transformed data
3. **Gold Layer**: Dimensional model with fact and dimension tables

## Dimensional Model

The Gold layer implements a proper star schema with:

### Dimension Tables
- **gold_dim_salesperson**: Salesperson dimension
- **gold_dim_lead**: Lead/Customer dimension
- **gold_dim_date**: Date dimension
- **gold_dim_opportunity_stage**: Opportunity Stage dimension

### Fact Table
- **gold_fact_sales**: Sales fact table with foreign keys to all dimensions

### Aggregated Views
- **gold_aws_sales_summary**: Sales summary by salesperson
- **gold_aws_sales_by_region**: Sales summary by region

## Prerequisites

- Docker Desktop for Windows
- PowerShell 5.0 or higher

## Quick Start

### Using PowerShell (Recommended)

1. Right-click on `run_docker.ps1` and select "Run with PowerShell"
   - This script will check if Docker is running
   - Create necessary directories
   - Build and start the containers

### Testing Data Ingestion

To test if the data ingestion works correctly without running the full pipeline:

```
python run_aws_sales_pipeline.py --test
```

This will:
1. Download the SalesData.csv file if not present
2. Test reading the CSV file with the configured schema
3. Display sample data and schema information
4. Verify column names match the configuration

### Viewing the Dimensional Schema

To view the details of the dimensional model:

```
python run_aws_sales_pipeline.py --show-schema
```

### Using Batch Script

1. Double-click the `run_docker.bat` script:
   ```
   run_docker.bat
   ```

### Manual Setup

1. Ensure Docker Desktop is running
2. Open a command prompt or PowerShell window
3. Build and start the Docker containers:
   ```
   docker-compose up --build
   ```

## Data Flow

1. **Bronze Layer**: Raw data is ingested from the CSV file and stored in MinIO
2. **Silver Layer**: Data is cleaned, validated, and transformed
3. **Gold Layer**: 
   - Dimension tables are created from the silver layer
   - Fact table is created with foreign keys to the dimension tables
   - Aggregated views are created for reporting

## Accessing Results

- **MinIO**: Access the MinIO console at http://localhost:9001 (login: minioadmin/minioadmin)
  - Bronze data: `bronze` bucket
  - Silver data: `silver` bucket
  - Gold data: `gold` bucket (contains dimension and fact tables)

- **PostgreSQL**: Connect to PostgreSQL at localhost:5432 (login: postgres/postgres)
  - Database: datawarehouse
  - Tables: Check the ETL audit records in `etl_audit` table
  - Gold schema: Contains all dimension and fact tables

## Customization

- Edit `src/config/metadata.py` to modify the data schema and transformations
- Edit `docker-compose.yml` to change container configurations

## Troubleshooting

- Check logs in the `logs` directory
- If containers fail to start:
  1. Run `docker-compose down` to stop all containers
  2. Run `docker-compose up --build` to rebuild and restart
- Ensure Docker Desktop has at least 4GB of memory allocated
- If MinIO or PostgreSQL services are unavailable, check if those ports (9000, 9001, 5432) are already in use by other applications
- If you encounter date parsing issues, verify the date format in the CSV matches the expected format (M/d/yyyy) 