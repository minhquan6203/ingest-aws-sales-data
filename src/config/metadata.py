"""
Metadata-driven ETL pipeline configuration.
This file defines the pipelines, sources, and transformations for the ETL process.
"""

# Sample Data Sources
DATA_SOURCES = {
    "salespeople_data": {
        "source_type": "csv",
        "source_path": "data/salespeople.csv",
        "schema": {
            "salesperson": "string",
            "salesperson_id": "string"
        },
        "primary_key": "salesperson_id",
        "bronze_table": "bronze_salespeople",
        "silver_table": "silver_salespeople",
    },
    "leads_data": {
        "source_type": "csv",
        "source_path": "data/leads.csv",
        "schema": {
            "lead_name": "string",
            "segment": "string",
            "region": "string",
            "lead_id": "string"
        },
        "primary_key": "lead_id",
        "bronze_table": "bronze_leads",
        "silver_table": "silver_leads",
    },
    "dates_data": {
        "source_type": "csv",
        "source_path": "data/dates.csv",
        "schema": {
            "full_date": "date",
            "date_id": "string",
            "day": "integer",
            "month": "integer",
            "year": "integer",
            "quarter": "integer"
        },
        "primary_key": "date_id",
        "incremental_column": "full_date",
        "bronze_table": "bronze_dates",
        "silver_table": "silver_dates",
    },
    "sales_opportunities_data": {
        "source_type": "csv",
        "source_path": "data/sales_opportunities.csv",
        "schema": {
            "date_id": "string",
            "target_close_id": "string",
            "salesperson_id": "string",
            "lead_id": "string",
            "forecasted_monthly_revenue": "double",
            "opportunity_stage": "string",
            "weighted_revenue": "double",
            "closed_opportunity": "boolean",
            "active_opportunity": "boolean",
            "latest_status_entry": "boolean",
            "opportunity_id": "string"
        },
        "primary_key": "opportunity_id",
        "bronze_table": "bronze_sales_opportunities",
        "silver_table": "silver_sales_opportunities",
    },
    "aws_sales_data": {
        "source_type": "csv",
        "source_path": "data/SalesData.csv",
        "schema": {
            "Date": "date",
            "Salesperson": "string",
            "Lead Name": "string",
            "Segment": "string",
            "Region": "string",
            "Target Close": "date",
            "Forecasted Monthly Revenue": "double",
            "Opportunity Stage": "string",
            "Weighted Revenue": "double",
            "Closed Opportunity": "boolean",
            "Active Opportunity": "boolean",
            "Latest Status Entry": "boolean"
        },
        "primary_key": "Lead Name",
        "incremental_column": "Date",
        "bronze_table": "bronze_aws_sales",
        "silver_table": "silver_aws_sales",
    }
}

# Data Quality Rules
DATA_QUALITY_RULES = {
    "salespeople_data": [
        {"column": "salesperson_id", "rule_type": "not_null"},
        {"column": "salesperson", "rule_type": "not_null"}
    ],
    "leads_data": [
        {"column": "lead_id", "rule_type": "not_null"},
        {"column": "lead_name", "rule_type": "not_null"}
    ],
    "dates_data": [
        {"column": "date_id", "rule_type": "not_null"},
        {"column": "full_date", "rule_type": "not_null"}
    ],
    "sales_opportunities_data": [
        {"column": "opportunity_id", "rule_type": "not_null"},
        {"column": "forecasted_monthly_revenue", "rule_type": "greater_than_equal", "value": 0},
        {"column": "weighted_revenue", "rule_type": "greater_than_equal", "value": 0}
    ],
    "aws_sales_data": [
        {"column": "Lead Name", "rule_type": "not_null"},
        {"column": "Forecasted Monthly Revenue", "rule_type": "greater_than_equal", "value": 0},
        {"column": "Weighted Revenue", "rule_type": "greater_than_equal", "value": 0}
    ]
}

# Pipeline Definitions
PIPELINES = {
    "salespeople_pipeline": {
        "source": "salespeople_data",
        "destination": "silver_salespeople",
        "schedule": "daily",
        "transformations": [
            {"type": "clean_column", "column": "salesperson"}
        ],
        "quality_checks": DATA_QUALITY_RULES["salespeople_data"]
    },
    "leads_pipeline": {
        "source": "leads_data",
        "destination": "silver_leads",
        "schedule": "daily",
        "transformations": [
            {"type": "clean_column", "column": "lead_name"},
            {"type": "clean_column", "column": "segment"},
            {"type": "clean_column", "column": "region"}
        ],
        "quality_checks": DATA_QUALITY_RULES["leads_data"]
    },
    "dates_pipeline": {
        "source": "dates_data",
        "destination": "silver_dates",
        "schedule": "daily",
        "transformations": [],
        "quality_checks": DATA_QUALITY_RULES["dates_data"]
    },
    "sales_opportunities_pipeline": {
        "source": "sales_opportunities_data",
        "destination": "silver_sales_opportunities",
        "schedule": "hourly",
        "transformations": [
            {"type": "type_conversion", "column": "forecasted_monthly_revenue", "to_type": "double"},
            {"type": "type_conversion", "column": "weighted_revenue", "to_type": "double"}
        ],
        "quality_checks": DATA_QUALITY_RULES["sales_opportunities_data"]
    },
    "aws_sales_pipeline": {
        "source": "aws_sales_data",
        "destination": "silver_aws_sales",
        "schedule": "daily",
        "transformations": [
            {"type": "clean_column", "column": "Salesperson"},
            {"type": "clean_column", "column": "Lead Name"},
            {"type": "clean_column", "column": "Segment"},
            {"type": "clean_column", "column": "Region"},
            {"type": "clean_column", "column": "Opportunity Stage"},
            {"type": "type_conversion", "column": "Forecasted Monthly Revenue", "to_type": "double"},
            {"type": "type_conversion", "column": "Weighted Revenue", "to_type": "double"}
        ],
        "quality_checks": DATA_QUALITY_RULES["aws_sales_data"]
    },
    "sales_summary_pipeline": {
        "type": "gold",
        "sources": ["silver_sales_opportunities", "silver_salespeople", "silver_leads", "silver_dates"],
        "destination": "gold_sales_summary",
        "schedule": "daily",
        "primary_key": "salesperson_id",
        "incremental_condition": "o.full_date > ?",
        "sql_transform": """
            SELECT 
                s.salesperson_id,
                s.salesperson,
                l.region,
                l.segment,
                COUNT(o.opportunity_id) as total_opportunities,
                SUM(o.forecasted_monthly_revenue) as total_forecasted_revenue,
                SUM(o.weighted_revenue) as total_weighted_revenue,
                SUM(CASE WHEN o.closed_opportunity = true THEN 1 ELSE 0 END) as closed_opportunities,
                SUM(CASE WHEN o.active_opportunity = true THEN 1 ELSE 0 END) as active_opportunities
            FROM silver_sales_opportunities o
            JOIN silver_salespeople s ON o.salesperson_id = s.salesperson_id
            JOIN silver_leads l ON o.lead_id = l.lead_id
            JOIN silver_dates d ON o.date_id = d.date_id
            GROUP BY s.salesperson_id, s.salesperson, l.region, l.segment
        """
    },
    # Dimension tables for AWS sales data
    "aws_dim_salesperson_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_salesperson",
        "schedule": "daily",
        "primary_key": "salesperson_id",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT DISTINCT
                "Salesperson" as salesperson_id,
                "Salesperson" as salesperson_name
            FROM silver_aws_sales
        """
    },
    "aws_dim_lead_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_lead",
        "schedule": "daily",
        "primary_key": "lead_id",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT DISTINCT
                "Lead Name" as lead_id,
                "Lead Name" as lead_name,
                "Segment" as segment,
                "Region" as region
            FROM silver_aws_sales
        """
    },
    "aws_dim_date_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_date",
        "schedule": "daily",
        "primary_key": "date_id",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT DISTINCT
                "Date" as date_id,
                "Date" as date,
                EXTRACT(YEAR FROM "Date") as year,
                EXTRACT(MONTH FROM "Date") as month,
                EXTRACT(DAY FROM "Date") as day,
                EXTRACT(QUARTER FROM "Date") as quarter,
                EXTRACT(DOW FROM "Date") as day_of_week
            FROM silver_aws_sales
        """
    },
    "aws_dim_opportunity_stage_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_opportunity_stage",
        "schedule": "daily",
        "primary_key": "stage_id",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT DISTINCT
                "Opportunity Stage" as stage_id,
                "Opportunity Stage" as stage_name
            FROM silver_aws_sales
        """
    },
    # Fact table for AWS sales data
    "aws_fact_sales_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_fact_sales",
        "schedule": "daily",
        "primary_key": "sales_id",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT
                CONCAT("Lead Name", '_', CAST("Date" AS VARCHAR), '_', "Salesperson") as sales_id,
                "Date" as date_id,
                "Lead Name" as lead_id,
                "Salesperson" as salesperson_id,
                "Opportunity Stage" as stage_id,
                "Target Close" as target_close_date,
                "Forecasted Monthly Revenue" as forecasted_monthly_revenue,
                "Weighted Revenue" as weighted_revenue,
                "Closed Opportunity" as closed_opportunity,
                "Active Opportunity" as active_opportunity,
                "Latest Status Entry" as latest_status_entry,
                DATEDIFF('day', "Date", "Target Close") as sales_cycle_days
            FROM silver_aws_sales
        """
    },
    # Aggregated views for reporting
    "aws_sales_summary_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_aws_sales_summary",
        "schedule": "daily",
        "primary_key": "salesperson",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT 
                "Salesperson" as salesperson,
                "Region" as region,
                "Segment" as segment,
                COUNT(*) as total_opportunities,
                SUM("Forecasted Monthly Revenue") as total_forecasted_revenue,
                SUM("Weighted Revenue") as total_weighted_revenue,
                SUM(CASE WHEN "Closed Opportunity" = true THEN 1 ELSE 0 END) as closed_opportunities,
                SUM(CASE WHEN "Active Opportunity" = true THEN 1 ELSE 0 END) as active_opportunities,
                AVG(DATEDIFF('day', "Date", "Target Close")) as avg_sales_cycle_days
            FROM silver_aws_sales
            GROUP BY "Salesperson", "Region", "Segment"
        """
    },
    "aws_sales_region_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_aws_sales_by_region",
        "schedule": "daily",
        "primary_key": "region",
        "incremental_condition": "\"Date\" > ?",
        "sql_transform": """
            SELECT 
                "Region" as region,
                "Segment" as segment,
                EXTRACT(YEAR FROM "Date") as year,
                EXTRACT(MONTH FROM "Date") as month,
                COUNT(*) as total_opportunities,
                SUM("Forecasted Monthly Revenue") as total_forecasted_revenue,
                SUM("Weighted Revenue") as total_weighted_revenue,
                SUM(CASE WHEN "Closed Opportunity" = true THEN 1 ELSE 0 END) as closed_opportunities
            FROM silver_aws_sales
            GROUP BY "Region", "Segment", EXTRACT(YEAR FROM "Date"), EXTRACT(MONTH FROM "Date")
        """
    }
} 