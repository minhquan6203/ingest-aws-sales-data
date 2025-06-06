"""
Utility functions for Spark
"""
from pyspark.sql import SparkSession
from loguru import logger
import os
import sys
from pyspark.sql.functions import col, trim

from src.config.config import SPARK_MASTER, SPARK_APP_NAME, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SPARK_ENDPOINT_INTERNAL


def create_spark_session():
    """
    Create a SparkSession
    """
    logger.info(f"Creating SparkSession with master={SPARK_MASTER} and appName={SPARK_APP_NAME}")
    
    # Determine the correct endpoint for MinIO/S3
    # When running in Docker, use the service name instead of localhost
    endpoint = SPARK_ENDPOINT_INTERNAL
    logger.info(f"Using S3 endpoint: {endpoint}")
    
    # Create SparkSession with standard configuration
    spark = (SparkSession.builder
             .master(SPARK_MASTER)
             .appName(SPARK_APP_NAME)
             .config("spark.driver.extraClassPath", "/usr/share/java/*")
             .config("spark.executor.extraClassPath", "/usr/share/java/*")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5s")
             .config("spark.hadoop.fs.s3a.connection.timeout", "10s")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "20")
             .getOrCreate())
    
    return spark


def read_csv_to_dataframe(spark, file_path, schema=None, header=True):
    """
    Read a CSV file into a Spark DataFrame

    Args:
        spark: SparkSession
        file_path: Path to the CSV file
        schema: Optional schema for the DataFrame
        header: Whether the CSV file has a header row

    Returns:
        Spark DataFrame
    """
    logger.info(f"Reading CSV file: {file_path}")
    
    # Set up options for CSV reading
    options = {
        "header": str(header).lower(),
        "dateFormat": "M/d/yyyy,yyyy-MM-dd,MM/dd/yyyy,d/M/yyyy",  # Support multiple date formats
        "timestampFormat": "M/d/yyyy HH:mm:ss,yyyy-MM-dd HH:mm:ss,MM/dd/yyyy HH:mm:ss",  # Support multiple timestamp formats
        "mode": "PERMISSIVE",  # Be permissive with bad records
        "nullValue": "",  # Treat empty strings as null
        "emptyValue": None,  # Treat empty fields as null
        "maxCharsPerColumn": "4096",  # Handle larger field values
        "multiLine": "true",  # Handle multi-line fields
        "maxColumns": "300"  # Allow many columns
    }
    
    # Create reader with options
    reader = spark.read.format("csv")
    for key, value in options.items():
        reader = reader.option(key, value)
    
    # Apply schema if provided
    if schema:
        logger.info("Using provided schema for CSV reading")
        return reader.schema(schema).load(file_path)
    else:
        logger.info("Inferring schema for CSV reading")
        return reader.option("inferSchema", "true").load(file_path)


def write_dataframe_to_parquet(df, destination_path, mode="overwrite", partition_by=None):
    """
    Write a DataFrame to Parquet format

    Args:
        df: Spark DataFrame
        destination_path: Path to write the Parquet files
        mode: Write mode (overwrite, append, etc.)
        partition_by: Column(s) to partition by
    """
    logger.info(f"Writing DataFrame to Parquet: {destination_path}")
    
    writer = df.write.format("parquet").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.save(destination_path)


def apply_transformations(df, transformations):
    """
    Apply transformations to a DataFrame based on transformation definitions

    Args:
        df: Spark DataFrame
        transformations: List of transformation definitions

    Returns:
        Transformed DataFrame
    """
    result_df = df
    
    for transform in transformations:
        transform_type = transform.get("type")
        
        if transform_type == "clean_column":
            column = transform.get("column")
            result_df = result_df.withColumn(column, trim(col(column)))
            
        elif transform_type == "type_conversion":
            column = transform.get("column")
            to_type = transform.get("to_type")
            result_df = result_df.withColumn(column, col(column).cast(to_type))
            
        elif transform_type == "drop_column":
            column = transform.get("column")
            result_df = result_df.drop(column)
            
        elif transform_type == "rename_column":
            old_name = transform.get("from")
            new_name = transform.get("to")
            result_df = result_df.withColumnRenamed(old_name, new_name)
    
    return result_df 