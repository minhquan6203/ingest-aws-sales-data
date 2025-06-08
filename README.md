# 🚀 AWS Sales Data ETL Pipeline

A comprehensive **Extract, Load, Transform (ELT)** system built with modern 3-layer architecture for processing AWS sales data efficiently using Apache Spark and PostgreSQL.

## 📋 Table of Contents

- [🏗️ Architecture Overview](#️-architecture-overview)
- [✨ Key Features](#-key-features)
- [🚀 Quick Start](#-quick-start)
- [💻 Usage Examples](#-usage-examples)
- [📊 Data Assets](#-data-assets)
- [🛠️ Installation](#️-installation)
- [🔧 Configuration](#-configuration)
- [📁 Project Structure](#-project-structure)
- [🔍 Monitoring & Analytics](#-monitoring--analytics)
- [🐳 Docker Deployment](#-docker-deployment)
- [🎯 Business Intelligence](#-business-intelligence)

## 🏗️ Architecture Overview

```
Data Sources → Bronze Layer → Silver Layer → Gold Layer → Analytics
   (Raw)      (Validated)    (Cleaned)     (Business)    (Reports)
```

### Layer Breakdown

- **🥉 Bronze Layer**: Raw data ingestion with validation and quality checks
- **🥈 Silver Layer**: Cleaned, standardized, and validated data
- **🥇 Gold Layer**: Business-ready dimensional model with fact/dimension tables
- **📊 Analytics Layer**: Advanced analytics, dashboards, and business intelligence

## ✨ Key Features

### 🎯 Advanced Data Processing
- **Incremental & Full Load**: Support for both incremental and full data loads
- **Distributed Processing**: Apache Spark-based for handling large datasets
- **Data Quality**: Comprehensive validation rules and quality checks
- **Error Handling**: Robust error handling with detailed logging

### 📈 Business Intelligence
- **Dimensional Modeling**: Star schema with fact and dimension tables
- **Advanced Analytics**: Sales performance metrics, funnel analysis, and trends
- **Executive Dashboard**: Real-time KPIs and business insights
- **Lead Scoring**: Intelligent lead qualification and scoring

### 🔧 Production Ready
- **Audit Trail**: Complete tracking of all pipeline executions
- **Monitoring**: Real-time monitoring and alerting capabilities
- **Containerization**: Docker and Docker Compose support
- **Scheduling**: Built-in pipeline scheduling capabilities

## 🚀 Quick Start with Docker

### Cách 1: Sử dụng Docker Compose (Khuyên dùng)
```bash
# Clone repository
git clone <repository-url>
cd ingest-aws-sales-data

# Khởi động tất với Docker Compose
docker-compose up --build
```

### Cách 2: Sử dụng PowerShell Script (Windows)
```bash
# Chạy script PowerShell (tự động build và start)
.\run_docker.ps1
```

### Cách 3: Sử dụng Batch Script (Windows)
```bash
# Chạy script batch
.\run_docker.bat
```

### Chạy trong Background
```bash
# Chạy dịch vụ trong background
docker-compose up -d --build

# Xem trạng thái dịch vụ
docker-compose ps

# Xem logs
docker-compose logs -f aws-sales-etl

# Dừng dịch vụ
docker-compose down
```

## 💻 Sử dụng Docker Container

### Chạy Pipeline trong Docker
```bash
# Chạy pipeline AWS sales chuẩn (8 bước)
docker-compose exec aws-sales-etl python src/main.py --run-aws-sales

# Chạy pipeline nâng cao với analytics (11 bước)
docker-compose exec aws-sales-etl python src/main.py --run-enhanced-aws-sales

# Chạy với full load thay vì incremental
docker-compose exec aws-sales-etl python src/main.py --run-enhanced-aws-sales --full-load

# Chạy pipeline cụ thể
docker-compose exec aws-sales-etl python src/main.py --run-pipeline aws_sales_pipeline

# Liệt kê tất cả pipeline có sẵn
docker-compose exec aws-sales-etl python src/main.py --list-pipelines
```

### Analytics Operations trong Docker
```bash
# Chạy analytics pipeline cụ thể
docker-compose exec aws-sales-etl python src/main.py --run-analytics-pipeline performance
docker-compose exec aws-sales-etl python src/main.py --run-analytics-pipeline funnel
docker-compose exec aws-sales-etl python src/main.py --run-analytics-pipeline trends
docker-compose exec aws-sales-etl python src/main.py --run-analytics-pipeline scoring

# Tạo analytics dashboard
docker-compose exec aws-sales-etl python src/main.py --run-analytics

# Hiển thị schema dimensional
docker-compose exec aws-sales-etl python src/main.py --show-schema
```

### Data Exploration trong Docker
```bash
# Xem tổng quan tất cả tables trong warehouse
docker-compose exec aws-sales-etl python src/main.py --show-all-tables

# Xem dữ liệu Bronze layer
docker-compose exec aws-sales-etl python src/main.py --show-bronze-data

# Xem dữ liệu Silver layer
docker-compose exec aws-sales-etl python src/main.py --show-silver-data

# Xem dữ liệu Gold layer  
docker-compose exec aws-sales-etl python src/main.py --show-gold-data

# Xem dữ liệu table cụ thể
docker-compose exec aws-sales-etl python src/main.py --show-table gold_fact_sales

# Xem nhiều hơn 10 rows (default)
docker-compose exec aws-sales-etl python src/main.py --show-table gold_fact_sales --limit 20
```

### Monitoring & Debugging trong Docker
```bash
# Xem lịch sử thực thi pipeline
docker-compose exec aws-sales-etl python src/main.py --show-history

# Test data ingestion
docker-compose exec aws-sales-etl python src/main.py --test-ingestion

# Test audit functionality
docker-compose exec aws-sales-etl python src/main.py --test-audit

# Xem logs của container
docker-compose logs -f aws-sales-etl

# Vào shell của container
docker-compose exec aws-sales-etl bash
```

### Quản lý Docker Services
```bash
# Kiểm tra trạng thái các dịch vụ
docker-compose ps

# Restart một dịch vụ cụ thể
docker-compose restart aws-sales-etl

# Xem resource usage
docker stats

# Dọn dẹp
docker-compose down --volumes --rmi all
```

## 📊 Data Assets

### Bronze Layer
- `bronze_aws_sales` - Raw sales data with validation and data quality checks

### Silver Layer  
- `silver_aws_sales` - Cleaned, standardized, and validated sales data

### Gold Layer - Dimensional Model

#### Dimension Tables
- `gold_dim_salesperson` - Salesperson master data
- `gold_dim_lead` - Lead/Customer dimension with segmentation
- `gold_dim_date` - Date dimension with calendar hierarchies
- `gold_dim_opportunity_stage` - Sales stage definitions

#### Fact Tables
- `gold_fact_sales` - Central sales fact table with all metrics

#### Business Views
- `gold_aws_sales_summary` - Executive sales summaries by salesperson
- `gold_aws_sales_by_region` - Regional performance analysis

### Gold Layer - Advanced Analytics
- `gold_sales_performance_metrics` - Sales performance KPIs and metrics
- `gold_opportunity_funnel` - Sales funnel analysis and conversion rates
- `gold_monthly_sales_trends` - Time-series analysis and forecasting
- `gold_lead_scoring` - Lead qualification and scoring insights

## 🛠️ Yêu cầu hệ thống

### Prerequisites
- **Docker** (phiên bản 20.10+)
- **Docker Compose** (phiên bản 2.0+)
- **Git** để clone repository
- **8GB RAM** tối thiểu (16GB khuyến nghị)
- **10GB dung lượng ổ cứng** trống

### Kiểm tra Docker
```bash
# Kiểm tra Docker version
docker --version

# Kiểm tra Docker Compose version
docker-compose --version

# Test Docker hoạt động
docker run hello-world
```

### Setup ban đầu
```bash
# Clone repository
git clone <repository-url>
cd ingest-aws-sales-data

# Tạo thư mục cần thiết (tự động tạo khi chạy Docker)
mkdir -p data logs

# File SalesData.csv sẽ được tự động tải về khi chạy pipeline
```

## 🔧 Cấu hình Docker

### Cấu hình trong docker-compose.yml
Tất cả cấu hình đã được thiết lập sẵn trong file `docker-compose.yml`:

```yaml
# Các dịch vụ được cấu hình:
services:
  postgres:         # PostgreSQL Database
    ports: "5432:5432"
    database: etl_demo
    
  minio:           # Object Storage (tương thích S3)
    ports: "9000:9000, 9001:9001"
    
  aws-sales-etl:   # Main ETL Application
    depends_on: postgres, minio
```

### Thay đổi cấu hình (nếu cần)
```bash
# Chỉnh sửa file docker-compose.yml
# Thay đổi ports, passwords, hoặc volumes theo nhu cầu

# Restart sau khi thay đổi
docker-compose down
docker-compose up --build
```

### Truy cập các dịch vụ
- **PostgreSQL**: `localhost:5432` (user: postgres, pass: postgres)
- **MinIO Console**: `http://localhost:9001` (admin/password123)
- **MinIO API**: `http://localhost:9000`

## 📁 Project Structure

```
ingest-aws-sales-data/
├── src/
│   ├── main.py                 # Main entry point
│   ├── analytics/              # Analytics dashboard and reporting
│   │   └── dashboard.py        # Executive dashboard generator
│   ├── audit/                  # Audit trail system
│   │   └── audit.py           # Pipeline execution tracking
│   ├── config/                 # Configuration and metadata
│   │   ├── metadata.py        # Pipeline and data source definitions
│   │   └── config.py          # System configuration
│   ├── controller/             # ETL orchestration
│   │   └── etl_controller.py  # Main controller for pipeline execution
│   ├── ingestion/              # Bronze layer data ingestion
│   │   └── csv_ingestion.py   # CSV file ingestion logic
│   ├── transformation/         # Silver & Gold layer transformations
│   │   ├── bronze_to_silver.py # Data cleaning and standardization
│   │   └── silver_to_gold.py   # Dimensional modeling
│   ├── notification/           # Alerting and notifications
│   └── utils/                  # Utilities and helpers
│       ├── spark_utils.py     # Spark session management
│       └── db_utils.py        # Database utilities
├── data/                       # Data files (SalesData.csv)
├── logs/                       # Execution logs
├── docker-compose.yml          # Container orchestration
├── Dockerfile                  # Application container
├── requirements.txt            # Python dependencies
├── run_docker.ps1             # PowerShell deployment script
├── run_docker.bat             # Batch deployment script
└── README.md                  # This file
```

## 🔍 Monitoring & Analytics

### Execution Monitoring
- **Audit Trail**: Every pipeline execution is tracked with timestamps, record counts, and status
- **Error Logging**: Detailed error logging with stack traces
- **Performance Metrics**: Execution duration and resource usage tracking

### Analytics Dashboard Features
- **Executive Summary**: High-level KPIs and performance metrics
- **Sales Performance**: Individual and team performance analysis
- **Opportunity Funnel**: Conversion rates and pipeline health
- **Regional Analysis**: Geographic performance breakdown
- **Trend Analysis**: Time-series analysis and forecasting
- **Lead Scoring**: Lead qualification and prioritization

### Sample Analytics Queries
```sql
-- Top performing salespeople
SELECT salesperson, region, actual_revenue, win_rate_percent
FROM gold_sales_performance_metrics
ORDER BY actual_revenue DESC;

-- Sales funnel analysis
SELECT stage, opportunity_count, stage_pipeline_value, conversion_rate
FROM gold_opportunity_funnel
ORDER BY stage;

-- Monthly revenue trends
SELECT year, month, region, revenue_won, revenue_pipeline
FROM gold_monthly_sales_trends
ORDER BY year DESC, month DESC;

-- Lead scoring insights
SELECT segment, avg_score, high_value_leads, conversion_probability
FROM gold_lead_scoring
ORDER BY avg_score DESC;
```

## 🐳 Docker Deployment

### Services Architecture
```yaml
services:
  postgres:      # Data warehouse
  minio:         # Object storage (S3-compatible)
  aws-sales-etl: # Main ETL application
```

### Quick Deployment
```bash
# Start all services
docker-compose up --build

# Run in background
docker-compose up -d --build

# Check service status
docker-compose ps

# View logs
docker-compose logs aws-sales-etl

# Stop services
docker-compose down
```

### Service Access
- **PostgreSQL**: `localhost:5432`
- **MinIO Console**: `http://localhost:9001`
- **Application Logs**: Available in `logs/` directory

## 🎯 Business Intelligence

### Key Performance Indicators (KPIs)
- **Revenue Metrics**: Actual vs forecasted revenue, win rates
- **Sales Performance**: Individual and team performance rankings
- **Pipeline Health**: Opportunity counts, stage distribution
- **Conversion Rates**: Stage-to-stage conversion analysis
- **Lead Quality**: Lead scoring and qualification metrics

### Executive Dashboard Export
The system generates comprehensive business reports in CSV format:
- Executive summary with key metrics
- Sales performance by salesperson
- Regional performance analysis
- Opportunity funnel metrics
- Monthly trend analysis

### Advanced Analytics Features
- **Predictive Analytics**: Lead scoring and revenue forecasting
- **Trend Analysis**: Historical trend analysis and seasonality detection
- **Performance Benchmarking**: Comparative analysis across regions and teams
- **Pipeline Forecasting**: Future revenue and opportunity projections

## 🔧 Troubleshooting

### Common Issues

1. **SalesData.csv not found**
   - The pipeline automatically downloads the file from AWS samples
   - Ensure internet connectivity for automatic download

2. **Spark connection issues**
   - Check if Spark is properly installed
   - Verify JAVA_HOME environment variable

3. **Database connection errors**
   - Ensure PostgreSQL is running
   - Check connection parameters in docker-compose.yml

4. **Memory issues with large datasets**
   - Adjust Spark configuration in `src/utils/spark_utils.py`
   - Increase Docker memory allocation

### Logging
- All execution logs are stored in `logs/` directory
- Use `--show-history` to view pipeline execution history
- Check Docker logs with `docker-compose logs`

## 📧 Support

For issues, questions, or contributions:
1. Check the troubleshooting section above
2. Review execution logs in the `logs/` directory
3. Use `python src/main.py --help` for usage information
4. Run `python src/main.py --test-ingestion` to verify data access

## 📄 License

This project is provided as-is for educational and demonstration purposes.

---

**Built with ❤️ using Apache Spark, PostgreSQL, and modern data engineering practices.** 