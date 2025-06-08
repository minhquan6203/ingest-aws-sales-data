"""
Sales Analytics Dashboard Module
Provides comprehensive business intelligence views for AWS sales data
"""

import os
import sys
from loguru import logger
import pandas as pd
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.utils.storage_utils import execute_sql
from src.utils.spark_utils import create_spark_session


class SalesAnalyticsDashboard:
    """
    Sales Analytics Dashboard for comprehensive business intelligence
    """
    
    def __init__(self):
        """Initialize the dashboard"""
        logger.info("Initializing Sales Analytics Dashboard")
        self.spark = create_spark_session()
    
    def get_sales_performance_summary(self, start_date=None, end_date=None):
        """
        Get overall sales performance summary
        """
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE date_id BETWEEN '{start_date}' AND '{end_date}'"
        
        sql = f"""
        SELECT 
            COUNT(*) as total_opportunities,
            COUNT(CASE WHEN closed_opportunity = true AND stage_id = 'Closed Won' THEN 1 END) as won_deals,
            COUNT(CASE WHEN closed_opportunity = true AND stage_id = 'Closed Lost' THEN 1 END) as lost_deals,
            ROUND(
                COUNT(CASE WHEN closed_opportunity = true AND stage_id = 'Closed Won' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN closed_opportunity = true THEN 1 END), 0), 2
            ) as overall_win_rate,
            SUM(forecasted_monthly_revenue) as total_pipeline_value,
            SUM(CASE WHEN stage_id = 'Closed Won' THEN forecasted_monthly_revenue ELSE 0 END) as total_revenue_won,
            AVG(sales_cycle_days) as avg_sales_cycle
        FROM gold_fact_sales
        {date_filter}
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['total_opportunities', 'won_deals', 'lost_deals', 'overall_win_rate', 
                      'total_pipeline_value', 'total_revenue_won', 'avg_sales_cycle']
            return dict(zip(columns, result[0]))
        return {}
    
    def get_top_performers(self, metric='total_revenue', limit=10):
        """
        Get top performing salespeople
        """
        metric_column = {
            'total_revenue': 'actual_revenue',
            'win_rate': 'win_rate_percent',
            'deal_count': 'won_deals',
            'pipeline_value': 'total_pipeline_value'
        }.get(metric, 'actual_revenue')
        
        sql = f"""
        SELECT 
            salesperson,
            region,
            segment,
            total_leads,
            won_deals,
            lost_deals,
            win_rate_percent,
            total_pipeline_value,
            actual_revenue,
            avg_sales_cycle_days,
            avg_deal_size
        FROM gold_sales_performance_metrics
        ORDER BY {metric_column} DESC
        LIMIT {limit}
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['salesperson', 'region', 'segment', 'total_leads', 'won_deals', 
                      'lost_deals', 'win_rate_percent', 'total_pipeline_value', 
                      'actual_revenue', 'avg_sales_cycle_days', 'avg_deal_size']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def get_regional_performance(self):
        """
        Get performance by region
        """
        sql = """
        SELECT 
            region,
            COUNT(*) as total_opportunities,
            SUM(total_forecasted_revenue) as total_pipeline,
            SUM(closed_opportunities) as deals_closed,
            ROUND(SUM(closed_opportunities) * 100.0 / COUNT(*), 2) as close_rate
        FROM gold_aws_sales_by_region
        GROUP BY region
        ORDER BY total_pipeline DESC
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['region', 'total_opportunities', 'total_pipeline', 'deals_closed', 'close_rate']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def get_opportunity_funnel(self):
        """
        Get opportunity funnel analysis
        """
        sql = """
        SELECT 
            stage,
            SUM(opportunity_count) as total_count,
            SUM(stage_pipeline_value) as total_value,
            SUM(stage_weighted_value) as weighted_value,
            AVG(avg_opportunity_value) as avg_opportunity_size,
            AVG(avg_days_to_close) as avg_days_to_close
        FROM gold_opportunity_funnel
        GROUP BY stage
        ORDER BY 
            CASE stage
                WHEN 'Lead' THEN 1
                WHEN 'Prospect' THEN 2
                WHEN 'Qualified' THEN 3
                WHEN 'Contracting' THEN 4
                WHEN 'Closed Won' THEN 5
                WHEN 'Closed Lost' THEN 6
                ELSE 7
            END
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['stage', 'total_count', 'total_value', 'weighted_value', 
                      'avg_opportunity_size', 'avg_days_to_close']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def get_monthly_trends(self, last_months=12):
        """
        Get monthly sales trends
        """
        sql = f"""
        SELECT 
            year,
            month,
            region,
            SUM(new_opportunities) as new_opportunities,
            SUM(deals_won) as deals_won,
            SUM(deals_lost) as deals_lost,
            SUM(monthly_revenue_won) as revenue_won,
            AVG(avg_monthly_deal_size) as avg_deal_size
        FROM gold_monthly_sales_trends
        WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '{last_months}' MONTH)
        GROUP BY year, month, region
        ORDER BY year DESC, month DESC, region
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['year', 'month', 'region', 'new_opportunities', 'deals_won', 
                      'deals_lost', 'revenue_won', 'avg_deal_size']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def get_lead_insights(self, score_filter=None):
        """
        Get lead scoring insights
        """
        where_clause = ""
        if score_filter:
            where_clause = f"WHERE lead_score = '{score_filter}'"
        
        sql = f"""
        SELECT 
            lead_name,
            segment,
            region,
            assigned_salesperson,
            max_opportunity_value,
            total_interactions,
            engagement_duration_days,
            lead_score,
            status,
            final_outcome
        FROM gold_lead_scoring
        {where_clause}
        ORDER BY max_opportunity_value DESC
        LIMIT 100
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['lead_name', 'segment', 'region', 'assigned_salesperson', 
                      'max_opportunity_value', 'total_interactions', 'engagement_duration_days',
                      'lead_score', 'status', 'final_outcome']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def get_segment_analysis(self):
        """
        Get analysis by business segment
        """
        sql = """
        SELECT 
            segment,
            region,
            COUNT(*) as total_opportunities,
            SUM(total_forecasted_revenue) as total_pipeline,
            AVG(total_forecasted_revenue) as avg_deal_size,
            SUM(closed_opportunities) as closed_deals,
            ROUND(SUM(closed_opportunities) * 100.0 / COUNT(*), 2) as close_rate
        FROM gold_aws_sales_by_region
        GROUP BY segment, region
        ORDER BY total_pipeline DESC
        """
        
        result = execute_sql(sql, fetch=True)
        if result:
            columns = ['segment', 'region', 'total_opportunities', 'total_pipeline', 
                      'avg_deal_size', 'closed_deals', 'close_rate']
            return [dict(zip(columns, row)) for row in result]
        return []
    
    def generate_executive_summary(self):
        """
        Generate an executive summary report
        """
        logger.info("Generating executive summary report")
        
        print("\n" + "="*80)
        print("                         SALES ANALYTICS EXECUTIVE SUMMARY")
        print("="*80)
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Overall Performance
        performance = self.get_sales_performance_summary()
        if performance:
            print("📊 OVERALL PERFORMANCE")
            print("-" * 40)
            print(f"Total Opportunities: {performance.get('total_opportunities', 0):,}")
            print(f"Won Deals: {performance.get('won_deals', 0):,}")
            print(f"Lost Deals: {performance.get('lost_deals', 0):,}")
            print(f"Overall Win Rate: {performance.get('overall_win_rate', 0)}%")
            print(f"Total Pipeline Value: ${performance.get('total_pipeline_value', 0):,.2f}")
            print(f"Total Revenue Won: ${performance.get('total_revenue_won', 0):,.2f}")
            print(f"Average Sales Cycle: {performance.get('avg_sales_cycle', 0):.1f} days")
            print()
        
        # Top Performers
        top_performers = self.get_top_performers(limit=5)
        if top_performers:
            print("🏆 TOP PERFORMERS (by Revenue)")
            print("-" * 40)
            for i, performer in enumerate(top_performers, 1):
                print(f"{i}. {performer['salesperson']} ({performer['region']}) - ${performer['actual_revenue']:,.2f}")
            print()
        
        # Regional Performance
        regional = self.get_regional_performance()
        if regional:
            print("🌍 REGIONAL PERFORMANCE")
            print("-" * 40)
            for region in regional:
                print(f"{region['region']}: ${region['total_pipeline']:,.2f} pipeline, {region['close_rate']}% close rate")
            print()
        
        # Opportunity Funnel
        funnel = self.get_opportunity_funnel()
        if funnel:
            print("🔄 OPPORTUNITY FUNNEL")
            print("-" * 40)
            for stage in funnel:
                print(f"{stage['stage']}: {stage['total_count']:,} opportunities, ${stage['total_value']:,.2f} value")
            print()
        
        print("="*80)
        print("For detailed analytics, run individual dashboard methods")
        print("="*80)
    
    def export_to_csv(self, output_dir="analytics_exports"):
        """
        Export all analytics data to CSV files
        """
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"Exporting analytics data to {output_dir}")
        
        exports = [
            ("sales_performance_summary", self.get_sales_performance_summary()),
            ("top_performers", self.get_top_performers(limit=50)),
            ("regional_performance", self.get_regional_performance()),
            ("opportunity_funnel", self.get_opportunity_funnel()),
            ("monthly_trends", self.get_monthly_trends()),
            ("lead_insights", self.get_lead_insights()),
            ("segment_analysis", self.get_segment_analysis())
        ]
        
        for name, data in exports:
            if data:
                filename = f"{output_dir}/{name}_{timestamp}.csv"
                if isinstance(data, list):
                    pd.DataFrame(data).to_csv(filename, index=False)
                else:
                    pd.DataFrame([data]).to_csv(filename, index=False)
                logger.info(f"Exported {name} to {filename}")
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """
    Main function for running analytics dashboard
    """
    dashboard = SalesAnalyticsDashboard()
    
    try:
        # Generate executive summary
        dashboard.generate_executive_summary()
        
        # Export data
        dashboard.export_to_csv()
        
        logger.info("Analytics dashboard completed successfully")
        
    except Exception as e:
        logger.error(f"Error running analytics dashboard: {e}")
        
    finally:
        dashboard.close()


if __name__ == "__main__":
    main() 