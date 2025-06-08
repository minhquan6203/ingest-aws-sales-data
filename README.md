# Enhanced 3-Layer ELT System for AWS Sales Data

A comprehensive **Extract, Load, Transform (ELT)** system built with modern 3-layer architecture for processing AWS sales data efficiently.

## 🏗️ Architecture Overview

```
Data Sources → Bronze Layer → Silver Layer → Gold Layer → Analytics
   (Raw)      (Validated)    (Cleaned)     (Business)    (Reports)
```

### Layer Breakdown

- **🥉 Bronze Layer**: Raw data ingestion with validation
- **🥈 Silver Layer**: Cleaned and standardized data
- **🥇 Gold Layer**: Business-ready dimensional model and analytics

## 🚀 Quick Start

### Option 1: Interactive Demo
```bash
python quick_start.py
```

### Option 2: Direct Pipeline Execution
```bash
# Run enhanced pipeline with advanced analytics
python src/main.py --run-enhanced-aws-sales

# Generate analytics dashboard
python src/main.py --run-analytics
```

### Option 3: Docker Deployment
```bash
docker-compose up --build
```

## 📊 Data Assets Created

### Bronze Layer
- `bronze_aws_sales` - Raw sales data with validation

### Silver Layer  
- `silver_aws_sales` - Cleaned and standardized data

### Gold Layer - Dimensional Model
- `gold_dim_salesperson` - Salesperson dimension
- `gold_dim_lead` - Lead/Customer dimension
- `gold_dim_date` - Date dimension
- `gold_dim_opportunity_stage` - Stage dimension
- `gold_fact_sales` - Central fact table

### Gold Layer - Advanced Analytics
- `gold_sales_performance_metrics` - Sales performance analysis
- `gold_opportunity_funnel` - Sales funnel metrics
- `gold_monthly_sales_trends` - Time-series analysis
- `gold_lead_scoring` - Lead qualification insights
- `gold_aws_sales_summary` - Executive summaries
- `gold_aws_sales_by_region` - Regional performance

## 🎯 Key Features

### ✅ Enhanced Data Quality
- Comprehensive validation rules
- Data type checking
- Business rule validation
- Audit trail for all operations

### ⚡ Performance Optimized
- Incremental loading support
- Spark-based distributed processing
- Intelligent caching and partitioning
- Resource optimization

### 📈 Advanced Analytics
- Executive dashboard with KPIs
- Sales performance metrics
- Opportunity funnel analysis
- Lead scoring and qualification
- Time-series trend analysis

### 🔧 Production Ready
- Complete audit trail
- Error handling and recovery
- Monitoring and alerting
- Docker containerization

## 💻 Usage Examples

### Pipeline Operations
```bash
# Run standard pipeline
python src/main.py --run-aws-sales

# Run enhanced pipeline
python src/main.py --run-enhanced-aws-sales

# Run with full load
python src/main.py --run-enhanced-aws-sales --full-load

# Run specific analytics
python src/main.py --run-analytics-pipeline performance
```

### System Management
```bash
# List all pipelines
python src/main.py --list-pipelines

# Show execution history
python src/main.py --show-history

# Show dimensional schema
python src/main.py --show-schema

# Test data ingestion
python src/main.py --test-ingestion
```

## 📋 Requirements

### Dependencies
- Python 3.8+
- Apache Spark 3.x
- PostgreSQL 13+
- MinIO (S3-compatible storage)
- Docker & Docker Compose

### Installation
```bash
pip install -r requirements.txt
```

## 🏗️ Infrastructure

### Services
- **PostgreSQL**: Data warehouse for analytics tables
- **MinIO**: Object storage for data files
- **Apache Spark**: Distributed processing engine

### Docker Services
```yaml
services:
  postgres:    # Data warehouse
  minio:       # Object storage  
  aws-sales-etl: # Main ETL application
```

## 📊 Business Intelligence

### Executive Dashboard Features
- Overall performance metrics
- Top performer rankings
- Regional analysis
- Opportunity funnel
- Monthly trends

### Sample Analytics Queries
```sql
-- Top performing salespeople
SELECT salesperson, region, actual_revenue, win_rate_percent
FROM gold_sales_performance_metrics
ORDER BY actual_revenue DESC;

-- Sales funnel analysis
SELECT stage, opportunity_count, stage_pipeline_value
FROM gold_opportunity_funnel
ORDER BY stage;

-- Monthly revenue trends
SELECT year, month, region, revenue_won
FROM gold_monthly_sales_trends
ORDER BY year DESC, month DESC;
```

## 📁 Project Structure

```
├── src/
│   ├── analytics/          # Analytics dashboard
│   ├── audit/             # Audit trail system
│   ├── config/            # Pipeline configuration
│   ├── controller/        # ETL orchestration
│   ├── ingestion/         # Bronze layer ingestion
│   ├── transformation/    # Silver & Gold transformations
│   └── utils/            # Utilities and helpers
├── data/                 # Data files
├── docs/                 # Documentation
├── logs/                 # Execution logs
├── docker-compose.yml    # Container orchestration
├── quick_start.py        # Interactive demo
└── README.md
```

## 🔍 Monitoring & Auditing

### Audit Trail Features
- Pipeline execution tracking
- Error logging and recovery
- Performance metrics
- Data lineage tracking

### Log Management
- Structured logging with Loguru
- Automatic log rotation
- Debug and production modes
- Centralized error tracking

## 🚀 Advanced Features

### Incremental Loading
- Timestamp-based processing
- Delta detection
- Efficient resource usage
- Near real-time capability

### Data Quality Framework
- Pre-ingestion validation
- Business rule checking
- Data profiling
- Quality metrics tracking

### Analytics Export
- CSV data exports
- Automated reporting
- Custom dashboard generation
- Business intelligence integration

## 📖 Documentation

- **[Complete System Guide](docs/ELT_SYSTEM_GUIDE.md)** - Comprehensive documentation
- **[Architecture Diagram](#)** - Visual system overview
- **[API Reference](#)** - Technical specifications

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙋‍♂️ Support

For questions, issues, or feature requests:
- Check the execution logs in `logs/`
- Review the audit trail with `--show-history`
- Consult the comprehensive documentation
- Open an issue on GitHub

---

**Ready to get started?** Run `python quick_start.py` for an interactive demo! 🚀 