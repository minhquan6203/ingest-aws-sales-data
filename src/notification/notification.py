"""
Notification module for ETL pipeline alerts
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger

from src.config.config import (
    ENABLE_EMAIL_NOTIFICATIONS, EMAIL_SENDER, EMAIL_RECIPIENTS,
    SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
)


class ETLNotifier:
    """
    Class for sending notifications about ETL pipeline events
    """
    
    @staticmethod
    def send_email_notification(subject, message):
        """
        Send an email notification
        
        Args:
            subject: Email subject
            message: Email message
        
        Returns:
            Boolean indicating success or failure
        """
        if not ENABLE_EMAIL_NOTIFICATIONS:
            logger.info("Email notifications are disabled")
            return False
        
        if not EMAIL_SENDER or not EMAIL_RECIPIENTS or not SMTP_SERVER:
            logger.warning("Email configuration is incomplete, skipping notification")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_SENDER
            msg['To'] = ', '.join(EMAIL_RECIPIENTS)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()
            
            if SMTP_USERNAME and SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    @staticmethod
    def notify_pipeline_start(pipeline_id, source_name, destination_name):
        """
        Send a notification when a pipeline starts
        
        Args:
            pipeline_id: Identifier for the pipeline
            source_name: Name of the source
            destination_name: Name of the destination
        """
        subject = f"ETL Pipeline Started: {pipeline_id}"
        message = f"""
        ETL Pipeline started:
        
        Pipeline ID: {pipeline_id}
        Source: {source_name}
        Destination: {destination_name}
        
        This is an automated message.
        """
        
        return ETLNotifier.send_email_notification(subject, message)
    
    @staticmethod
    def notify_pipeline_success(pipeline_id, source_name, destination_name, records_processed, duration_seconds):
        """
        Send a notification when a pipeline completes successfully
        
        Args:
            pipeline_id: Identifier for the pipeline
            source_name: Name of the source
            destination_name: Name of the destination
            records_processed: Number of records processed
            duration_seconds: Duration in seconds
        """
        subject = f"ETL Pipeline Completed: {pipeline_id}"
        message = f"""
        ETL Pipeline completed successfully:
        
        Pipeline ID: {pipeline_id}
        Source: {source_name}
        Destination: {destination_name}
        Records processed: {records_processed}
        Duration: {duration_seconds:.2f} seconds
        
        This is an automated message.
        """
        
        return ETLNotifier.send_email_notification(subject, message)
    
    @staticmethod
    def notify_pipeline_failure(pipeline_id, source_name, destination_name, error_message, duration_seconds):
        """
        Send a notification when a pipeline fails
        
        Args:
            pipeline_id: Identifier for the pipeline
            source_name: Name of the source
            destination_name: Name of the destination
            error_message: Error message
            duration_seconds: Duration in seconds
        """
        subject = f"ALERT: ETL Pipeline Failed: {pipeline_id}"
        message = f"""
        ETL Pipeline failed:
        
        Pipeline ID: {pipeline_id}
        Source: {source_name}
        Destination: {destination_name}
        Duration: {duration_seconds:.2f} seconds
        
        Error:
        {error_message}
        
        This is an automated message.
        """
        
        return ETLNotifier.send_email_notification(subject, message)
    
    @staticmethod
    def notify_data_quality_issue(pipeline_id, source_name, quality_check, failed_records):
        """
        Send a notification when a data quality check fails
        
        Args:
            pipeline_id: Identifier for the pipeline
            source_name: Name of the source
            quality_check: Description of the quality check that failed
            failed_records: Number of records that failed the check
        """
        subject = f"ALERT: Data Quality Issue: {pipeline_id}"
        message = f"""
        Data quality check failed:
        
        Pipeline ID: {pipeline_id}
        Source: {source_name}
        Quality check: {quality_check}
        Failed records: {failed_records}
        
        This is an automated message.
        """
        
        return ETLNotifier.send_email_notification(subject, message) 