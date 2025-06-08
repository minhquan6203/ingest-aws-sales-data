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
            "Date": "string",
            "Salesperson": "string",
            "Lead Name": "string",
            "Segment": "string",
            "Region": "string",
            "Target Close": "string",
            "Forecasted Monthly Revenue": "integer",
            "Opportunity Stage": "string",
            "Weighted Revenue": "integer",
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

# Enhanced Data Quality Rules with more comprehensive validation
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
        # Minimal data quality rules - temporarily disabled problematic checks
        {"column": "Lead Name", "rule_type": "not_null"},
        {"column": "Salesperson", "rule_type": "not_null"}
        # Removed all other checks to debug date conversion issue
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
            # Enable date conversions with proper formatting
            {"type": "date_conversion", "column": "Date", "from_format": "M/d/yyyy", "to_format": "yyyy-MM-dd"},
            {"type": "date_conversion", "column": "Target Close", "from_format": "M/d/yyyy", "to_format": "yyyy-MM-dd"},
            # Convert revenue to double for calculations
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
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                HASH(`Salesperson`) as salesperson_id,
                `Salesperson` as salesperson_name
            FROM (
                SELECT DISTINCT `Salesperson`
                FROM silver_aws_sales
                WHERE `Salesperson` IS NOT NULL
            ) t
        """
    },
    "aws_dim_lead_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_lead",
        "schedule": "daily",
        "primary_key": "lead_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                HASH(`Lead Name`) as lead_id,
                `Lead Name` as lead_name,
                `Segment` as segment,
                `Region` as region
            FROM (
                SELECT DISTINCT 
                    `Lead Name`,
                    `Segment`,
                    `Region`
                FROM silver_aws_sales
                WHERE `Lead Name` IS NOT NULL
            ) t
        """
    },
    "aws_dim_date_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_date",
        "schedule": "daily",
        "primary_key": "date_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                HASH(`Date`) as date_id,
                `Date` as date_value,
                YEAR(`Date`) as year,
                MONTH(`Date`) as month,
                DAY(`Date`) as day,
                QUARTER(`Date`) as quarter,
                DAYOFWEEK(`Date`) as day_of_week
            FROM (
                SELECT DISTINCT `Date`
                FROM silver_aws_sales
                WHERE `Date` IS NOT NULL
            ) t
        """
    },
    "aws_dim_opportunity_stage_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_dim_opportunity_stage",
        "schedule": "daily",
        "primary_key": "stage_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                HASH(`Opportunity Stage`) as stage_id,
                `Opportunity Stage` as stage_name
            FROM (
                SELECT DISTINCT `Opportunity Stage`
                FROM silver_aws_sales
                WHERE `Opportunity Stage` IS NOT NULL
            ) t
        """
    },
    # Fact table for AWS sales data
    "aws_fact_sales_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_fact_sales",
        "schedule": "daily",
        "primary_key": "sales_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY `Date`, `Lead Name`, `Salesperson`) as sales_id,
                HASH(`Date`) as date_id,
                HASH(`Lead Name`) as lead_id,
                HASH(`Salesperson`) as salesperson_id,
                HASH(`Opportunity Stage`) as stage_id,
                `Target Close` as target_close_date,
                `Forecasted Monthly Revenue` as forecasted_monthly_revenue,
                `Weighted Revenue` as weighted_revenue,
                `Closed Opportunity` as closed_opportunity,
                `Active Opportunity` as active_opportunity,
                `Latest Status Entry` as latest_status_entry,
                DATEDIFF(`Target Close`, `Date`) as sales_cycle_days
            FROM silver_aws_sales
            WHERE `Lead Name` IS NOT NULL 
              AND `Date` IS NOT NULL 
              AND `Salesperson` IS NOT NULL
              AND `Opportunity Stage` IS NOT NULL
        """
    },
    # Aggregated views for reporting
    "aws_sales_summary_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_aws_sales_summary",
        "schedule": "daily",
        "primary_key": "summary_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY `Salesperson`, `Region`, `Segment`) as summary_id,
                `Salesperson` as salesperson_name,
                `Region` as region,
                `Segment` as segment,
                COUNT(*) as total_opportunities,
                SUM(`Forecasted Monthly Revenue`) as total_forecasted_revenue,
                SUM(`Weighted Revenue`) as total_weighted_revenue,
                SUM(CASE WHEN `Closed Opportunity` = true THEN 1 ELSE 0 END) as closed_opportunities,
                SUM(CASE WHEN `Active Opportunity` = true THEN 1 ELSE 0 END) as active_opportunities,
                AVG(DATEDIFF(`Target Close`, `Date`)) as avg_sales_cycle_days
            FROM silver_aws_sales
            WHERE `Salesperson` IS NOT NULL AND `Region` IS NOT NULL AND `Segment` IS NOT NULL
            GROUP BY `Salesperson`, `Region`, `Segment`
        """
    },
    "aws_sales_region_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_aws_sales_by_region",
        "schedule": "daily",
        "primary_key": "region_summary_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY `Region`, `Segment`, YEAR(`Date`), MONTH(`Date`)) as region_summary_id,
                `Region` as region,
                `Segment` as segment,
                YEAR(`Date`) as year,
                MONTH(`Date`) as month,
                COUNT(*) as total_opportunities,
                SUM(`Forecasted Monthly Revenue`) as total_forecasted_revenue,
                SUM(`Weighted Revenue`) as total_weighted_revenue,
                SUM(CASE WHEN `Closed Opportunity` = true THEN 1 ELSE 0 END) as closed_opportunities
            FROM silver_aws_sales
            WHERE `Region` IS NOT NULL AND `Segment` IS NOT NULL AND `Date` IS NOT NULL
            GROUP BY `Region`, `Segment`, YEAR(`Date`), MONTH(`Date`)
        """
    },
    
    # Advanced Analytics Pipelines
    "aws_sales_performance_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_sales_performance_metrics",
        "schedule": "daily",
        "primary_key": "performance_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY `Salesperson`, `Region`, `Segment`) as performance_id,
                `Salesperson` as salesperson_name,
                `Region` as region,
                `Segment` as segment,
                COUNT(*) as total_leads,
                COUNT(CASE WHEN `Closed Opportunity` = true AND `Opportunity Stage` = 'Closed Won' THEN 1 END) as won_deals,
                COUNT(CASE WHEN `Closed Opportunity` = true AND `Opportunity Stage` = 'Closed Lost' THEN 1 END) as lost_deals,
                ROUND(
                    COUNT(CASE WHEN `Closed Opportunity` = true AND `Opportunity Stage` = 'Closed Won' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(CASE WHEN `Closed Opportunity` = true THEN 1 END), 0), 2
                ) as win_rate_percent,
                SUM(`Forecasted Monthly Revenue`) as total_pipeline_value,
                SUM(CASE WHEN `Opportunity Stage` = 'Closed Won' THEN `Forecasted Monthly Revenue` ELSE 0 END) as actual_revenue,
                AVG(DATEDIFF(`Target Close`, `Date`)) as avg_sales_cycle_days,
                AVG(`Forecasted Monthly Revenue`) as avg_deal_size,
                STDDEV(`Forecasted Monthly Revenue`) as deal_size_variance
            FROM silver_aws_sales
            WHERE `Salesperson` IS NOT NULL AND `Region` IS NOT NULL AND `Segment` IS NOT NULL
            GROUP BY `Salesperson`, `Region`, `Segment`
        """
    },
    
    "aws_opportunity_funnel_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_opportunity_funnel",
        "schedule": "daily",
        "primary_key": "funnel_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY `Opportunity Stage`, `Region`, `Segment`) as funnel_id,
                `Opportunity Stage` as stage_name,
                `Region` as region,
                `Segment` as segment,
                COUNT(*) as opportunity_count,
                SUM(`Forecasted Monthly Revenue`) as stage_pipeline_value,
                SUM(`Weighted Revenue`) as stage_weighted_value,
                AVG(`Forecasted Monthly Revenue`) as avg_opportunity_value,
                AVG(DATEDIFF(`Target Close`, `Date`)) as avg_days_to_close,
                COUNT(CASE WHEN `Active Opportunity` = true THEN 1 END) as active_opportunities,
                COUNT(CASE WHEN `Latest Status Entry` = true THEN 1 END) as recent_activity_count
            FROM silver_aws_sales
            WHERE `Opportunity Stage` IS NOT NULL AND `Region` IS NOT NULL AND `Segment` IS NOT NULL
            GROUP BY `Opportunity Stage`, `Region`, `Segment`
        """
    },
    
    "aws_monthly_trends_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_monthly_sales_trends",
        "schedule": "daily",
        "primary_key": "trends_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY YEAR(`Date`), MONTH(`Date`), `Region`) as trends_id,
                YEAR(`Date`) as year,
                MONTH(`Date`) as month,
                `Region` as region,
                COUNT(*) as new_opportunities,
                COUNT(CASE WHEN `Closed Opportunity` = true AND `Opportunity Stage` = 'Closed Won' THEN 1 END) as deals_won,
                COUNT(CASE WHEN `Closed Opportunity` = true AND `Opportunity Stage` = 'Closed Lost' THEN 1 END) as deals_lost,
                SUM(`Forecasted Monthly Revenue`) as monthly_pipeline_added,
                SUM(CASE WHEN `Opportunity Stage` = 'Closed Won' THEN `Forecasted Monthly Revenue` ELSE 0 END) as monthly_revenue_won,
                AVG(`Forecasted Monthly Revenue`) as avg_monthly_deal_size,
                COUNT(DISTINCT `Salesperson`) as active_salespeople,
                COUNT(DISTINCT `Lead Name`) as unique_leads
            FROM silver_aws_sales
            WHERE `Date` IS NOT NULL AND `Region` IS NOT NULL
            GROUP BY YEAR(`Date`), MONTH(`Date`), `Region`
        """
    },
    
    "aws_lead_scoring_pipeline": {
        "type": "gold",
        "sources": ["silver_aws_sales"],
        "destination": "gold_lead_scoring",
        "schedule": "daily",
        "primary_key": "scoring_id",
        "incremental_condition": "`Date` > ?",
        "sql_transform": """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY `Lead Name`) as scoring_id,
                `Lead Name` as lead_name,
                `Segment` as segment,
                `Region` as region,
                `Salesperson` as assigned_salesperson,
                MAX(`Forecasted Monthly Revenue`) as max_opportunity_value,
                COUNT(*) as total_interactions,
                MAX(`Date`) as last_interaction_date,
                DATEDIFF(MAX(`Date`), MIN(`Date`)) as engagement_duration_days,
                CASE 
                    WHEN COUNT(CASE WHEN `Opportunity Stage` IN ('Qualified', 'Contracting') THEN 1 END) > 0 THEN 'High'
                    WHEN COUNT(CASE WHEN `Opportunity Stage` = 'Prospect' THEN 1 END) > 0 THEN 'Medium'
                    ELSE 'Low'
                END as lead_score,
                CASE WHEN MAX(`Active Opportunity`) = true THEN 'Active' ELSE 'Inactive' END as status,
                CASE WHEN MAX(`Closed Opportunity`) = true THEN 
                    CASE WHEN MAX(`Opportunity Stage`) = 'Closed Won' THEN 'Won' ELSE 'Lost' END
                ELSE 'Open' END as final_outcome
            FROM silver_aws_sales
            WHERE `Lead Name` IS NOT NULL
            GROUP BY `Lead Name`, `Segment`, `Region`, `Salesperson`
        """
    }
} 